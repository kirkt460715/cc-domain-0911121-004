'''字节级 CSV 分帧。

现场日志按字节追加，且可能在一条记录写完前被读取：
- 带引号字段可以跨行（CR / LF / CRLF 都可能出现在引号内）；
- 多字节 UTF-8（例如中文）可能分两次写入，EOF 处只落下半个字符；
- 只允许在「完整 CSV 记录」之后确认进度，EOF 处的半条内容原样留给下次扫描。

分帧器纯按字节工作：CR/LF 只有在引号外才结束记录，因此半条或未闭合的
引号字段永远停在 pending 区，不会产生假记录。
'''

import codecs

from hangar_ingest.types import PipelineError, fail

HEADER = b'event_id,device,observed_at,value,unit\n'
DEFAULT_MAX_RECORD = 1048576


class CsvFramer:
    '''维护单个文件世代的分帧状态。

    每次扫描把新读到的字节交给 :meth:`feed`，拿到本轮完整帧；
    :meth:`pending_bytes` 返回必须留在 EOF、下次再喂入的残余字节。
    帧为 ``{'bytes': ..., 'oversized': bool}``。
    '''

    def __init__(self, *, max_record_bytes=DEFAULT_MAX_RECORD):
        self.max_record_bytes = max_record_bytes
        self._buffer = bytearray()
        self._in_quotes = False

    def feed(self, data):
        if not isinstance(data, (bytes, bytearray)):
            fail('framer_input', 'Framer input must be bytes')
        self._buffer.extend(data)
        # 每次都从完整缓冲（含上次遗留的 pending）起点重扫，引号状态从 False
        # 开始，避免把上次的结尾状态与缓冲内的开引号重复切换。
        self._in_quotes = False
        frames = []
        start = 0
        index = 0
        length = len(self._buffer)
        while index < length:
            byte = self._buffer[index]
            if byte == 0x22:  # 双引号
                if self._in_quotes and index + 1 < length and self._buffer[index + 1] == 0x22:
                    # 引号字段内的转义双引号 ""，整体跳过，不改变引号开闭状态。
                    index += 2
                    continue
                self._in_quotes = not self._in_quotes
                index += 1
                continue
            if byte in (0x0A, 0x0D) and not self._in_quotes:
                if byte == 0x0D:
                    if index + 1 >= length:
                        # 缓冲区末尾的裸 CR 可能是 CRLF 的前半，留给下次。
                        break
                    end = index + 2 if self._buffer[index + 1] == 0x0A else index + 1
                else:
                    end = index + 1
                frames.append({'bytes': bytes(self._buffer[start:end]), 'oversized': False})
                start = end
                index = end
                continue
            if not self._in_quotes and index - start + 1 > self.max_record_bytes:
                # 未闭合且超过单条上限的内容不能无限积压，截出到下一个行尾并标记。
                next_lf = self._buffer.find(b'\n', index)
                if next_lf == -1:
                    break
                end = next_lf + 1
                frames.append({'bytes': bytes(self._buffer[start:end]), 'oversized': True})
                start = end
                index = end
                continue
            index += 1
        if start:
            del self._buffer[:start]
        return frames

    def pending_bytes(self):
        return bytes(self._buffer)

    @property
    def in_quotes(self):
        return self._in_quotes


def decode_frame(frame):
    '''把一条完整字节记录解码为文本；半字符不会进入这里。'''
    decoder = codecs.getincrementaldecoder('utf-8')()
    try:
        text = decoder.decode(frame, final=True)
    except UnicodeDecodeError as exc:
        raise PipelineError('invalid_utf8', 'CSV record is not valid UTF-8') from exc
    return text


def parse_frame(frame):
    '''解析单条完整 CSV 记录，返回字段列表。'''
    import csv
    import io
    text = decode_frame(frame)
    try:
        rows = list(csv.reader(io.StringIO(text, newline=''), strict=True))
    except csv.Error as exc:
        raise PipelineError('csv_malformed', 'Malformed quoted CSV') from exc
    if len(rows) != 1:
        raise PipelineError('csv_malformed', 'CSV frame must contain exactly one record')
    return rows[0]


def is_header(frame):
    return frame == HEADER or frame == b'event_id,device,observed_at,value,unit\r\n'
