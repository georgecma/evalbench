#!/bin/bash
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
export PYTHONPATH=../evalbench/evalproto:../evalbench
python3 evalbench/nl2code_eval_server.py 

