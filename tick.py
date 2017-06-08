#!/usr/bin/env python
import pandas as pd
import subprocess
from datetime import datetime
import os
import csv


def download_ticks(uid, sym, date, ex, folder, verbose=False):
    tmp_file = '/tmp/tmp_tick.txt'

    date_str = date.strftime('%Y%m%d')
    new_file = os.path.join(folder, '%s_%s_ticks.csv' % (
        uid, date_str))

    cmd = 'tick_printer'
    args = '-i%s -d%s --source SRC_%s.OSECAUCUS.DSECAUCUS' % (
        sym, date_str, ex)

    print 'retrieving ticks for %s on %s ...' % (uid, date_str)
    # print cmd, args

    with open(tmp_file, "w+") as output:
        args = [cmd] + args.split(' ')
        subprocess.call(args, stdout=output)

    cnt = 0
    with open(new_file, 'w') as csvfile:
        fields = ['ts', 'lag', 'px', 'vol']
        writer = csv.DictWriter(csvfile, fieldnames=fields)

        writer.writeheader()
        with open(tmp_file) as f_in:
            for line in f_in:
                if 'tick update' in line:
                    line = line.replace('\n', '')
                    dt = {}
                    for seg in line.split(' ')[3:]:
                        tag, val = seg.split(':')
                        if tag == 'ex_time':
                            dt['ts'] = val
                        if tag == 'msg_time':
                            lag = float(val) - float(dt['ts'])
                            dt['lag'] = int(lag * 1e6)
                        # elif tag == 'instr':
                        #     dt['sym'] = val
                        elif tag == 'sz':
                            dt['vol'] = val
                        elif tag == 'px':
                            dt['px'] = val

                    writer.writerow(dt)
                    if verbose and cnt % 10000 == 0:
                        print '%s line %d, last ts = %s' % (
                            sym, cnt, datetime.fromtimestamp(float(dt['ts'])))
                    cnt += 1


def download_quotes(uid, sym, date, ex, folder, verbose=False):
    tmp_file = '/tmp/tmp_book.txt'

    date_str = date.strftime('%Y%m%d')
    new_file = os.path.join(folder, '%s_%s_quotes.csv' % (
        uid, date_str))

    cmd = 'book_printer'
    args = '-i%s -d%s --source SRC_%s.OSECAUCUS.DSECAUCUS' % (
        sym, date_str, ex)

    print 'retrieving quotes for %s on %s ...' % (uid, date_str)
    # print cmd, args

    with open(tmp_file, "w+") as output:
        args = [cmd] + args.split(' ')
        subprocess.call(args, stdout=output)

    with open(new_file, 'w') as csvfile:
        fields = ['ts', 'bid', 'ask', 'mid', 'bid_sz', 'ask_sz']
        writer = csv.DictWriter(csvfile, fieldnames=fields)

        writer.writeheader()
        cnt = 0
        with open(tmp_file) as f_in:
            for line in f_in:
                if 'L1 update' in line:
                    line = line.replace('\n', '')
                    dt = {}
                    for seg in line.split(' ')[3:]:
                        tag, val = seg.split(':')
                        if tag == 'time':
                            dt['ts'] = val
                        # elif tag == 'instr':
                        #     dt['sym'] = val
                        elif tag == 'bid_px':
                            dt['bid'] = val
                        elif tag == 'bid_sz':
                            dt['bid_sz'] = val
                        elif tag == 'ask_px':
                            dt['ask'] = val
                        elif tag == 'ask_sz':
                            dt['ask_sz'] = val
                        elif tag == 'mid_px':
                            dt['mid'] = val
                    writer.writerow(dt)

                    if verbose and cnt % 10000 == 0:
                        print '%s line %d, last ts = %s' % (
                            sym, cnt, datetime.fromtimestamp(float(dt['ts'])))
                    cnt += 1


def main():

    file = '~/Dropbox/intraday/DOW_dates.csv'
    dates = pd.read_csv(file, index_col=1, header=None)
    dates = pd.DatetimeIndex(dates.index)

    file = '~/Dropbox/intraday/DOW_univ.csv'
    df = pd.read_csv(file, index_col=0)

    mapping = {
        'ticks': {
            'XNGS': 'UTDF',
            'XNYS': 'CTS'},
        'quotes': {
            'XNGS': 'UQDF',
            'XNYS': 'CQS',
        }
    }
    mode = 'quotes'

    path = '$HOME/Dropbox/intraday/%s/' % mode
    for uid, vec in df.iterrows():
        ex = mapping[mode][vec['prim_exch']]
        sym = vec['sym']

        folder = os.path.expandvars(os.path.join(path, uid))
        if not os.path.exists(folder):
            os.makedirs(folder)

        for date in dates:
            if mode == 'ticks':
                download_ticks(uid, sym, date, ex, folder)
            elif mode == 'quotes':
                download_quotes(uid, sym, date, ex, folder)
        #     break
        # break


if __name__ == '__main__':
    main()
