import sys
import pandas as pd
import psycopg2
from psycopg2.sql import SQL, Identifier
import pandas as pd


def go(l_length, r_length, conn):
  cursor = conn.cursor()

  # sql = "CREATE TABLE l_100000 (pk bigint, l0 bigint, l1 bigint);"
  # cursor.execute(sql)

  # left table

  print("create left table")
  sql = "CREATE TABLE {} (pk bigint, l0 bigint, l1 double precision);"
  sql = SQL(sql).format(Identifier('l_{}'.format(l_length)))

  cursor.execute(sql)
  cursor.execute('commit')

  print("load left hdf5 and save as csv")
  df = pd.read_hdf('l_df_{}.hdf5'.format(l_length))
  df.to_csv('l_df_{}.csv'.format(l_length), index=False)
  del df

  print("copy left csv")
  with open('l_df_{}.csv'.format(l_length)) as f:
    i_f = iter(f)
    next(i_f)
    cursor.copy_from(i_f, 'l_{}'.format(l_length), sep=',', columns=('pk', 'l0', 'l1'))

  print("add left pk")
  sql = "ALTER TABLE {} ADD PRIMARY KEY (pk);"
  sql = SQL(sql).format(Identifier('l_{}'.format(l_length)))
  cursor.execute(sql)
  cursor.execute('commit')

  # right table

  print("create right table")
  sql = "CREATE TABLE {} (pk bigint, fk bigint, r0 bigint, r1 double precision);"
  sql = SQL(sql).format(Identifier('r_{}'.format(r_length)))
  cursor.execute(sql)
  cursor.execute('commit')

  print("load right hdf5 and save as csv")
  df = pd.read_hdf('r_df_{}.hdf5'.format(r_length))
  df.to_csv('r_df_{}.csv'.format(r_length), index=False)
  del df

  print("copy right csv")
  with open('r_df_{}.csv'.format(r_length)) as f:
    i_f = iter(f)
    next(i_f)
    cursor.copy_from(i_f, 'r_{}'.format(r_length), sep=',', columns=('pk', 'fk', 'r0', 'r1'))

  print("add right pk")
  sql = "ALTER TABLE {} ADD PRIMARY KEY (pk);"
  sql = SQL(sql).format(Identifier('r_{}'.format(r_length)))
  cursor.execute(sql)
  cursor.execute('commit')

  print("add right fk")
  sql = "ALTER TABLE {} ADD FOREIGN KEY (fk) references {}(pk);"
  sql = SQL(sql).format(Identifier('r_{}'.format(r_length)),
                        Identifier('l_{}'.format(l_length)))
  cursor.execute(sql)
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
