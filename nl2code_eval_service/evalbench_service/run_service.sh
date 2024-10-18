#!/bin/bash
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
export PYTHONPATH=./evalproto:.
/gcompute-tools/git-cookie-authdaemon
cd evalbench
python3 ./nl2code_eval_server.py 

