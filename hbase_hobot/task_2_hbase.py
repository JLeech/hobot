import argparse
import calendar
import getpass
import happybase
import logging
import random
import sys


USAGE = """

To set from date:
  $ {0} --from 2014-01-01

To set till date:
  $ {0} --to 2015-01-01

""".format(sys.argv[0])

HOSTS = ["bds%02d.vdi.mipt.ru" % i for i in xrange(7, 10)]
TABLE = "table_19433"

def get_table():
    host = random.choice(HOSTS)
    conn = happybase.Connection(host)

    logging.debug("Connecting to HBase Thrift Server on %s", host)
    conn.open()

    return happybase.Table(TABLE, conn)

def main():
    parser = argparse.ArgumentParser(description="task 2", usage=USAGE)
    parser.add_argument("--from", type=str, required=True)
    parser.add_argument("--to", type=str, default=None)

    args = parser.parse_args()
    
    table = get_table()

    row = table.row('2014-10-14:youtube.com')
    print row.keys()

if __name__ == "__main__":
    main()