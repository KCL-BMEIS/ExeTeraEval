import sys
import dask
import dask.dataframe as ddf
import numpy as np
import pandas as pd


def go(l_length, r_length, p_length):
  rng = np.random.RandomState(12345678)
  l_key_ = np.arange(1, l_length + 1) * 10
  l_df = pd.DataFrame({'lpk': l_key_,
                       'l0': rng.randint(1, 1000, l_length) * 1000,
                       'l1': rng.uniform(0.0, 1.0, l_length)})

  rng = np.random.RandomState(12345679)
  lf_key_ = rng.randint(0, l_length, r_length)
  lf_key_ = l_key_[lf_key_]
  r_df = pd.DataFrame({'rpk': np.arange(1, r_length + 1) * 10,
                       'fk': lf_key_,
                       'r0': rng.randint(1000, 10000, r_length) * 1000,
                       'r1': rng.uniform(0.0, 1.0, r_length)})
  r_df.sort_values('fk', inplace=True)
  del l_key_
  del lf_key_

  print(l_df)
  print(r_df)

  p_l_df = ddf.from_pandas(l_df, npartitions=max(1, l_length // p_length))
  p_r_df = ddf.from_pandas(r_df, npartitions=max(1, r_length // p_length))

  p_l_df.to_hdf('d_l_df_{}.hdf'.format(l_length), key='/data')
  p_r_df.to_hdf('d_r_df_{}.hdf'.format(r_length), key='/data')

  # slock = dask.utils.SerializableLock()
  # p_l_df = ddf.read_hdf('p_l_df', key='/data', lock=slock)
  # p_r_df = ddf.read_hdf('p_r_df', key='/data', lock=slock)

  # print(p_l_df)
  # print(p_r_df)

  # m_df = ddf.merge(p_l_df, p_r_df, left_on='pk', right_on='fk', how='left')
  # print(m_df)
  # result = m_df.compute()
  # print(result)

if __name__ == '__main__':
  if len(sys.argv) != 4:
    print("Usage: create_dask_join_scenario.py <left_length> <right_length> <partition_length>")
    exit(-1)

  go(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
