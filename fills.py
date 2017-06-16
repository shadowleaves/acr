#!/usr/bin/env python
from tailf import tailf
import os
from datetime import datetime


def main():
    path = '/local/oakthistle/trader/'
    date_str = datetime.today().strftime('%Y%m%d')
    fn = 'oak_thistle_twap_trader_OAK_LIME1.ordertracker.OAK_LIME1-%s.1.bson'
    fn = fn % date_str
    filepath = os.path.join(path, fn)
    listen(filepath, start_count=5)


def listen(filepath, **kwargs):
    # filepath = '/var/log/distributed-local.log'
    for line in tailf(filepath, **kwargs):
        print line
    pass


if __name__ == '__main__':
    main()
