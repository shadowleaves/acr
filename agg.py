#!/usr/bin/env python
# import pandas as pd
# import subprocess
# from datetime import datetime
import os
# import csv
# import shlex
# import json
# import bson
import gzip
# from pytz import UTC


def ensure_path(*p):
    f = os.path.join(*p)
    f = os.path.expandvars(f)
    if not os.path.exists(f):
        try:
            os.makedirs(f)
        except Exception:
            pass
    return(f)


def aggregate(sym, source_path, dest_path):
    subpath = os.path.join(source_path, sym)
    new_filepath = os.path.join(dest_path, '%s.bson.gz' % sym)
    print 'aggregating %s ...' % sym
    try:
        with gzip.open(new_filepath, 'wb') as new_bson:
            for file in os.listdir(subpath):
                filepath = os.path.join(subpath, file)
                with gzip.open(filepath, 'rb') as bson_file:
                    for line in bson_file.readlines():
                        new_bson.write(line)
    except KeyboardInterrupt:
        print '^C detected...'


def main():

    from multiprocessing import Pool
    pool = Pool(processes=8)

    # from multiprocessing.pool import ThreadPool
    # pool = ThreadPool(processes=5)

    exch = 'bbl1_equities_all'
    path = os.path.expandvars('$HOME/data/bson/%s' % exch)
    newpath = ensure_path('$HOME/Dropbox/intraday/bson/agg')
    # newpath = ensure_path('/tmp/agg')

    res = []
    for sym in sorted(os.listdir(path)):
        args = (sym, path, newpath)
        # download_bars(*args)
        result = pool.apply_async(aggregate, args=args)
        res.append(result)

    # retriving results (process won't start to run until here)
    [x.get() for x in res]

    # print 'total tasks %d' % len(tmp)


if __name__ == '__main__':
    main()
