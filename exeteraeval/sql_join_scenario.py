import sys
import time
import psycopg2
from psycopg2.sql import SQL, Identifier
import pandas as pd


def go(l_length, r_length, conn):
  cursor = conn.cursor()

  sql = ('create table l_to_r as select l.pk, l.l0, l.l1, r.fk, r.r0, r.r1 from {} as r '
         'left join {} as l on l.pk = r.fk')
  print(sql)
  sql = SQL(sql).format(Identifier('r_{}'.format(r_length)),
                        Identifier('l_{}'.format(l_length)))

  t0 = time.time()
  cursor.execute(sql)
  cursor.execute('commit')
  print("joined in", time.time() - t0)

  cursor.execute('drop table l_to_r')
  cursor.execute('commit')


if __name__ == '__main__':
  if len(sys.argv) != 3:
    print("Usage: sql_join_scenario.py <left_length> <right_length>")
    exit(-1)

  try:
    conn = psycopg2.connect(host='127.0.0.1', database='exetera',
                            user='postgres', password='postgres')

    go(int(sys.argv[1]), int(sys.argv[2]), conn)
  except Exception as e:
    print(e)
