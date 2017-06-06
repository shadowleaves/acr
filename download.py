
#!/usr/bin/env python
import pandas as pd
import subprocess
from datetime import datetime
import os

if __name__ == '__main__':

    file = '~/Dropbox/intraday/DOW_dates.csv'
    dates = pd.read_csv(file, index_col=1, header=None)
    dates = pd.DatetimeIndex(dates.index)

    file = '~/Dropbox/intraday/DOW_univ.csv'
    df = pd.read_csv(file, index_col=0)

    mapping = {'XNGS': 'UTDF',
               'XNYS': 'CTS'}

    tmp_file = '/tmp/output.txt'
    path = '~/Dropbox/intraday/tick/'
    for uid, vec in df.iterrows():
        ex = mapping[vec['prim_exch']]
        sym = vec['sym']

        folder = os.path.join(path, uid)
        if not os.path.exists(folder):
            os.makedirs(folder)

        import pdb
        pdb.set_trace()

        for date in dates:

            date_str = date.strftime('%Y%m%d')
            cmd = 'tick_printer'
            args = '-i%s -d%s --source SRC_%s.OSECAUCUS.DSECAUCUS' % (
                sym, date_str, ex)

            print cmd, args

            with open(tmp_file, "w+") as output:
                args = [cmd] + args.split(' ')
                subprocess.call(args, stdout=output)

            with open(tmp_file) as f:
                for line in f:
                    if 'tick update' in line:
                        line = line.replace('\n', '')
                        for seg in line.split(' ')[3:]:
                            tag, val = seg.split(':')
                            if tag == 'ex_time':
                                ts = datetime.utcfromtimestamp(float(val))
                            elif tag == 'instr':
                                sym = val
                            elif tag == 'sz':
                                vol = int(val)
                            elif tag == 'px':
                                px = float(val)

                        import pdb
                        pdb.set_trace()

