import resource
import sys
import time
import numpy as np
import pandas as pd


def go(l_filename, r_filename):

  t0 = time.time()
  l_df = pd.read_hdf(l_filename)
  left_read = time.time() - t0
  print("left dataframe read in {}".format(left_read))

  t0 = time.time()
  r_df = pd.read_hdf(r_filename)
  right_read = time.time() - t0
  print("left dataframe read in {}".format(right_read))

  t0 = time.time()
  l_df = l_df.sort_values(['patient_id', 'created_at'])
  r_df = r_df.sort_values('id')
  print("sorted in {}".format(time.time() - t0))

  t0 = time.time()
  m_df = pd.merge(l_df, r_df, left_on='patient_id', right_on='id', how='left')
  # m_df = pd.merge(l_df, r_df, left_index=True, right_on='fk')
  merged = time.time() - t0
  print("pandas: merged in {}".format(merged))
  print(len(m_df))

  print(l_df[:100])
  print(r_df[:100])
  print(m_df[:100])
  print("total:", left_read + right_read + merged)

if __name__ == '__main__':
  # c_soft, c_hard = resource.getrlimit(resource.RLIMIT_AS)
  # resource.setrlimit(resource.RLIMIT_AS, (32 * 1024 * 1024 * 1024, c_hard))
  if len(sys.argv) != 3:
    print("Usage: pandas_join_scenario.py <left_csv> <right_csv>")
    exit(-1)

  go(sys.argv[1], sys.argv[2])
