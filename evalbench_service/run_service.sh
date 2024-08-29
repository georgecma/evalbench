#!/bin/bash
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
export PYTHONPATH=../evalbench/evalproto:../evalbench
cd ../evalbench
python3 ./eval_server.py --localhost
