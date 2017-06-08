#!/usr/bin/env python
import pandas as pd
import subprocess
from datetime import datetime
import os
# import csv
import shlex
import json
import bson
import gzip
from pytz import UTC


def ensure_path(*p):
    f = os.path.join(*p)
    f = os.path.expandvars(f)
    if not os.path.exists(f):
        try:
            os.makedirs(f)
        except Exception:
            pass
    return(f)


def main():

    from multiprocessing import Pool
    from lib.bb.Symbol import Symbol
    pool = Pool(processes=5)

    exch = 'bbl1_equities_all'
    path = os.path.expandvars('$HOME/data/bson/%s' % exch)
    newpath = ensure_path('$HOME/Dropbox/intraday/bson/agg')

    for sym in sorted(os.listdir(path)):
        subpath = os.path.join(path, sym)
        new_filepath = os.path.join(newpath, '%s.bson.gz' % sym)
        print 'aggregating %s ...' % sym
        with gzip.open(new_filepath, 'wb') as new_bson:
            for file in os.listdir(subpath):
                filepath = os.path.join(subpath, file)
                with gzip.open(filepath, 'rb') as bson_file:
                    for line in bson_file.readlines():
                        new_bson.write(line)

                    # for doc in bson.decode_file_iter(bson_file):
                    #     import pdb
                    #     pdb.set_trace()
        # raise SystemExit


if __name__ == '__main__':
    main()
