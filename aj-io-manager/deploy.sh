#!/bin/bash

source activate hackathon
export PYTHONPATH=.
python runner/run.py --dag TEST_PIPELINE
