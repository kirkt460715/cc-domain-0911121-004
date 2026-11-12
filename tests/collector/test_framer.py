from hangar_ingest.collector.framer import CsvFramer, is_header, parse_frame
from hangar_ingest.types import PipelineError
import unittest


class FramerTests(unittest.TestCase):
    def test_half_record_at_eof_stays_pending(self):
        framer = CsvFramer()
        frames = framer.feed(b'event_id,device,observed_at,value,unit\ne1,sensor1,t,"2')
        self.assertEqual(len(frames), 1)  # 只有完整的表头成帧
        self.assertTrue(framer.in_quotes)
        pending = framer.pending_bytes()
        self.assertTrue(pending.endswith(b'"2'))

    def test_half_record_completes_on_next_feed(self):
        framer = CsvFramer()
        framer.feed(b'event_id,device,observed_at,value,unit\ne1,sensor1,t,"2')
        frames = framer.feed(b'1",C\n')
        self.assertEqual(len(frames), 1)  # 本轮只补全那一条记录
        self.assertEqual(framer.pending_bytes(), b'')
        self.assertEqual(parse_frame(frames[0]['bytes']), ['e1', 'sensor1', 't', '21', 'C'])

    def test_quoted_newline_spans_feeds(self):
        framer = CsvFramer()
        first = framer.feed(b'e4,sensor1,t,"2\n')
        self.assertEqual(first, [])
        self.assertTrue(framer.in_quotes)
        frames = framer.feed(b'3",C\n')
        self.assertEqual(parse_frame(frames[0]['bytes']), ['e4', 'sensor1', 't', '2\n3', 'C'])

    def test_split_multibyte_utf8_is_held_then_joined(self):
        text = 'event_id,device,observed_at,value,unit\ne1,sensor1,t,20,摄\n'
        full = text.encode('utf-8')
        cut = full.index('摄'.encode('utf-8')) + 1  # 切在多字节字符内部
        framer = CsvFramer()
        first_frames = framer.feed(full[:cut])
        self.assertEqual(len(first_frames), 1)  # 表头成帧，含半字符的记录挂起
        self.assertNotEqual(framer.pending_bytes(), b'')
        frames = framer.feed(full[cut:])
        self.assertEqual(parse_frame(frames[0]['bytes'])[4], '摄')

    def test_bare_cr_at_buffer_end_waits_for_lf(self):
        framer = CsvFramer()
        framer.feed(b'event_id,device,observed_at,value,unit')
        frames = framer.feed(b'\r')
        self.assertEqual(frames, [])
        frames = framer.feed(b'\ne1,sensor1,t,20,C\r\n')
        self.assertTrue(is_header(frames[0]['bytes']))
        self.assertEqual(parse_frame(frames[1]['bytes']), ['e1', 'sensor1', 't', '20', 'C'])

    def test_oversized_unquoted_record_is_marked(self):
        framer = CsvFramer(max_record_bytes=8)
        frames = framer.feed(b'aaaaaaaaaaaa\nbbb\n')
        self.assertEqual(len(frames), 2)
        self.assertTrue(frames[0]['oversized'])
        self.assertFalse(frames[1]['oversized'])

    def test_quote_toggle_resets_across_feeds(self):
        # 跨行引号记录分多次喂入，状态不能与缓冲内的开引号重复切换。
        framer = CsvFramer()
        framer.feed(b'a,b,"x\n')
        framer.feed(b'y",c\n')
        frames = framer.feed(b'd,e,f\n')
        self.assertEqual(parse_frame(frames[0]['bytes']), ['d', 'e', 'f'])

    def test_invalid_utf8_frame_raises(self):
        with self.assertRaises(PipelineError) as failure:
            parse_frame(b'\xff\xfe,C\n')
        self.assertEqual(failure.exception.code, 'invalid_utf8')


if __name__ == '__main__':
    unittest.main()
