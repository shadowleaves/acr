#!/usr/bin/env python
import pandas as pd
import subprocess
from datetime import datetime
import os
# import csv
import json
import bson
import gzip
from pytz import UTC


def ensure_path(*p):
    f = os.path.join(*p)
    f = os.path.expandvars(f)
    if not os.path.exists(f):
        os.makedirs(f)
    return(f)


def run_iter(args):
    args = args.split(' ')
    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    while(True):
        retcode = p.poll()  # returns None while subprocess is running
        line = p.stdout.readline()
        yield line
        if(retcode is not None):
            break


def download_bars(path, file, exch, sym, reset=False):
    # tmp_file = '/tmp/bson.tmp'

    filepath = os.path.join(path, file)

    date = file.split('.')[2]
    bson_path = ensure_path('$HOME/Dropbox/intraday/bson/%s/%s' %
                            (exch, sym))
    bson_file = os.path.join(bson_path, '%s.bson.gz' % date)

    if os.path.exists(bson_file) and not reset:
        return

    print 'producing %s ...' % bson_file
    with gzip.open(bson_file, 'wb') as f_out:

        args = 'dfview --json --threaded --symbol %s %s' % (sym, filepath)
        for line in run_iter(args):
            if not len(line):
                continue

            if 'error' in line or line[0] != '{':
                break

            try:
                dt = json.loads(line)
            except ValueError as e:
                print e
                import pdb
                pdb.set_trace()

            dt = {x: dt[x] for x in dt if dt[x] != 'NaN'}
            for key in dt:
                if 'time' in key:
                    date = datetime.utcfromtimestamp(dt[key])
                    date = date.replace(tzinfo=UTC)
                    dt[key] = date
            f_out.write(bson.BSON.encode(dt))


def main():

    from multiprocessing import Pool
    pool = Pool(processes=8)

    file = '~/Dropbox/intraday/DOW_univ.csv'
    df = pd.read_csv(file, index_col=0)
    df = df[~df.sym.isnull()]

    root_path = '/nfs/data/hotel.inp/candlesticks/'
    # exch = 'shfe'
    exch = 'bbl1_equities_all'
    for year in 2017, 2016:
        for month in xrange(12):
            path = os.path.join(root_path, exch, str(year), '%02d' % month)
            if not os.path.exists(path):
                continue
            for file in sorted(os.listdir(path)):
                if '.bb.gz' in file:
                    # filepath = os.path.join(path, file)
                    res = []
                    for sym in df.sym:
                        args = (path, file, exch, sym)
                        result = pool.apply_async(download_bars, args=args)
                        res.append(result)

                    # retriving results (process won't start to run until here)
                    tmp = [x.get(timeout=1e6) for x in res]

        # raise SystemExit


if __name__ == '__main__':
    main()
