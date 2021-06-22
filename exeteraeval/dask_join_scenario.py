import sys
import time
import dask
import dask.dataframe as ddf
import numpy as np
import pandas as pd

def go(l_length, r_length):

  t0 = time.time()
  slock = dask.utils.SerializableLock()
  p_l_df = ddf.read_hdf('d_l_df_{}.hdf'.format(l_length), key='/data', lock=slock)
  p_r_df = ddf.read_hdf('d_r_df_{}.hdf'.format(r_length), key='/data', lock=slock)

  # print(p_l_df)
  # print(p_r_df)

  m_df = ddf.merge(p_l_df, p_r_df, left_on='lpk', right_on='fk', how='left')
  # print(m_df)
  m_df.to_hdf('d_m_df_{}_{}.hdf'.format(l_length, r_length), key='/data')
  # result = m_df.compute()
  print("merged in:", time.time() - t0)
  d_m_df = ddf.read_hdf('d_m_df_{}_{}.hdf'.format(l_length, r_length), key='/data')
  print(d_m_df.compute())


if __name__ == '__main__':
  if len(sys.argv) != 3:
    print("Usage: dask_join_scenario.py <left_length> <right_length>")
    exit(-1)

  go(int(sys.argv[1]), int(sys.argv[2]))
