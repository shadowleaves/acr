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
        os.makedirs(f)
    return(f)


def run_iter(args):
    args = shlex.split(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    while(True):
        retcode = p.poll()  # returns None while subprocess is running
        line = p.stdout.readline()
        yield line
        if(retcode is not None):
            break


def download_bars(path, file, exch, univ, reset=False):
    # tmp_file = '/tmp/bson.tmp'

    filepath = os.path.join(path, file)

    date_str = file.split('.')[2]
    picked = []
    for sym in univ:
        bson_path = ensure_path('$HOME/Dropbox/intraday/bson/%s/%s' %
                                (exch, sym))
        bson_file = os.path.join(bson_path, '%s.bson.gz' % date_str)
        if not os.path.exists(bson_file) or reset:
            picked.append(sym)

    print 'parsing %s ...' % date_str

    univ_str = ' '.join(['-S %s' % x for x in univ])
    args = 'dfview --json --threaded %s %s' % (
        univ_str, filepath)

    files = {}

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

        sym = dt['instr']
        if sym not in files:
            bson_path = ensure_path('$HOME/Dropbox/intraday/bson/%s/%s' %
                                    (exch, sym))
            bson_file = os.path.join(bson_path, '%s.bson.gz' % date_str)
            files[sym] = gzip.open(bson_file, 'wb')
        files[sym].write(bson.BSON.encode(dt))
        # f_out.write(bson.BSON.encode(dt))

    for sym in files:
        files[sym].close()

    import pdb
    pdb.set_trace()


def main():

    from multiprocessing import Pool
    pool = Pool(processes=5)

    file = '~/Dropbox/intraday/DOW_univ.csv'
    df = pd.read_csv(file, index_col=0)
    df = df[~df.sym.isnull()]
    univ = list(df.sym)

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
                    args = (path, file, exch, univ)
                    download_bars(*args)
                    # result = pool.apply_async(download_bars, args=args)
                    # res.append(result)

            # retriving results (process won't start to run until here)
            # tmp = [x.get(timeout=1e6) for x in res]

        # raise SystemExit


if __name__ == '__main__':
    main()
