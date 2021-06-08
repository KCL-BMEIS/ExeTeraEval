import resource
import sys
import time
import numpy as np
import pandas as pd


def go(l_length, r_length):

  t0 = time.time()
  l_df = pd.read_hdf('l_df_{}.hdf5'.format(l_length))
  left_read = time.time() - t0
  print("left dataframe read in {}".format(left_read))

  t0 = time.time()
  r_df = pd.read_hdf('r_df_{}.hdf5'.format(r_length))
  right_read = time.time() - t0
  print("left dataframe read in {}".format(right_read))

  t0 = time.time()
  m_df = pd.merge(l_df, r_df, left_on='pk', right_on='fk', how='left')
  # m_df = pd.merge(l_df, r_df, left_index=True, right_on='fk')
  m_df.to_hdf('m_df.hdf5', '/data')
  merged = time.time() - t0
  print("pandas: merged in {}".format(merged))
  print(len(m_df))

  # print(l_df[:100])
  # print(r_df[:100])
  # print(m_df[:100])
  print("total:", left_read + right_read + merged)




if __name__ == '__main__':
  c_soft, c_hard = resource.getrlimit(resource.RLIMIT_AS)
  resource.setrlimit(resource.RLIMIT_AS, (32 * 1024 * 1024 * 1024, c_hard))
  if len(sys.argv) != 3:
    print("Usage: pandas_join_scenario.py <left_length> <right_length>")
    exit(-1)

  go(int(sys.argv[1]), int(sys.argv[2]))
