#!/usr/bin/env python

import os
# import csv
import gzip
import bson
import pandas as pd
from collections import OrderedDict


def main():

    # file = '~/Dropbox/intraday/DOW_dates.csv'
    # dates = pd.read_csv(file, index_col=1, header=None)
    # dates = pd.DatetimeIndex(dates.index)

    path = '$HOME/Dropbox/intraday/bson/bbl1_equities_all/AAPL/'
    path = os.path.expandvars(path)

    mapping = {'open_px': 'open',
               'high_px': 'high',
               'low_px': 'low',
               'close_px': 'close',
               'value': 'value',
               'volume': 'volume',
               'spd': 'spd',
               }

    res = pd.DataFrame()
    for file in sorted(os.listdir(path)):
        filepath = os.path.join(path, file)
        df = []
        with gzip.open(filepath, 'rb') as bson_file:
            for doc in bson.decode_file_iter(bson_file):
                df.append(doc)

        df = pd.DataFrame(df).set_index('bucket_start_time')
        df.index.name = None
        df['spd'] = df['best_ask_px'] - df['best_bid_px']
        df['value'] = df['vwap'] * df['volume']
        df = df[mapping.keys()]
        df.columns = mapping.values()

        how = OrderedDict([('open', 'first'),
                           ('high', 'max'),
                           ('low', 'min'),
                           ('close', 'last'),
                           ('spd', 'mean'),
                           ('volume', 'sum'),
                           ('value', 'sum'),
                           ])

        df = df.resample('15Min', how=how)
        df['vwap'] = df['value'] / df['volume']
        df = df.drop('value', axis=1)
        res = pd.concat((res, df), axis=0)

    import pdb
    pdb.set_trace()


if __name__ == '__main__':
    main()
