#!/usr/bin/bash

python $CODE/bar.py $@
if [ $? != 0 ]; then exit; fi

python $CODE/agg.py $@
if [ $? != 0 ]; then exit; fi
