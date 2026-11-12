#!/bin/sh
set -eu
python -m compileall -q hangar_ingest demo_support tests
python -m unittest discover -s tests -v
python -m hangar_ingest --db :memory: demo --fixture fixtures/demo.json
python demo_support/check.py
python demo_support/collector_drill.py
