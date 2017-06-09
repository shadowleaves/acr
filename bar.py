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


def sym2uid(sym):
    return sym.replace('-', '/').replace('/', '.')


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
        uid = sym2uid(sym)
        bson_path = ensure_path('$HOME/data/bson/%s/%s' %
                                (exch, uid))
        bson_file = os.path.join(bson_path, '%s.bson.gz' % date_str)
        if not os.path.exists(bson_file) or reset:
            picked.append(sym)

    if len(picked) == 0:
        return

    print 'parsing %s for %d symbols ...' % (date_str, len(picked))
    univ_str = ' '.join(['-S %s' % x for x in picked])
    args = 'dfview --json --ignore-file-errors --threaded %s %s' % (
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

        uid = sym2uid(dt['instr'])
        if uid not in files:
            bson_path = os.path.expandvars(
                '$HOME/data/bson/%s/%s' % (exch, uid))
            bson_file = os.path.join(bson_path, '%s.bson.gz' % date_str)
            files[uid] = gzip.open(bson_file, 'wb')
        files[uid].write(bson.BSON.encode(dt))
        # f_out.write(bson.BSON.encode(dt))

    for uid in files:
        files[uid].close()


def main():

    from multiprocessing import Pool
    from lib.bb.Symbol import Symbol

    pool = Pool(processes=6)

    file = '~/Dropbox/intraday/R1K_univ.csv'
    df = pd.read_csv(file, index_col=0)
    df = df[~df.sym.isnull()]
    univ = [x for x in list(df.sym) if Symbol(x).is_valid()]

    root_path = '/nfs/data/hotel.inp/candlesticks/'
    # exch = 'shfe'
    exch = 'bbl1_equities_all'
    for year in 2014, :
        for month in xrange(13):
            path = os.path.join(root_path, exch, str(year), '%02d' % month)
            if not os.path.exists(path):
                continue
            res = []
            for file in sorted(os.listdir(path)):
                if '.bb.gz' in file:
                    # filepath = os.path.join(path, file)
                    args = (path, file, exch, univ)
                    # download_bars(*args)
                    result = pool.apply_async(download_bars, args=args)
                    res.append(result)

            # retriving results (process won't start to run until here)
            [x.get(timeout=1e6) for x in res]

        # raise SystemExit


if __name__ == '__main__':
    main()
