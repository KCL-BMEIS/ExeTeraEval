import sys
import time
import numpy as np
import pandas as pd


def go(l_length, r_length, p_length):
  rng = np.random.RandomState(12345678)

  t0 = time.time()
  l_df = pd.DataFrame({'pk': np.arange(1, l_length + 1) * 10})
  l_df['l0'] = rng.randint(1, 1000, l_length) * 1000
  l_df['l1'] = rng.uniform(0.0, 1.0, l_length)

  t0 = time.time()
  l_df.set_index('pk', verify_integrity=True)
  print("setting index:", time.time() - t0)

  t0 = time.time()
  l_df.to_hdf('l_df_{}.hdf5'.format(l_length), 'data', 'w', format='table')
  print("saving to hdf5:", time.time() - t0)


  rng = np.random.RandomState(12345679)

  t0 = time.time()
  r_df = pd.DataFrame({'pk': np.arange(1, r_length + 1) * 10})
  l_key_ = np.arange(1, l_length + 1) * 10
  lf_key_ = rng.randint(0, l_length, r_length)
  lf_key_ = l_key_[lf_key_]
  r_df['fk'] = lf_key_
  del l_key_
  del lf_key_
  r_df['r0'] = rng.randint(1000, 10000, r_length) * 1000
  r_df['r1'] = rng.uniform(0.0, 1.0, r_length)
  print("creating and adding columns:", time.time() - t0)

  t0 = time.time()
  r_df.set_index('pk', verify_integrity=True)
  print("setting index:", time.time() - t0)

  t0 = time.time()
  r_df.sort_values('fk', inplace=True)
  print("sorted on fk:", time.time() - t0)

  t0 = time.time()
  # r_df.to_hdf('r_df_{}.hdf5'.format(r_length), 'data', 'w')

  partitions = r_length // p_length
  if r_length % p_length != 0:
    partitions += 1
  for i in range(partitions):
    pr_df = r_df[p_length * i: min(r_length, p_length * (i + 1))]
    print(len(pr_df))
    pr_df.to_hdf('r_df_{}_{}.hdf5'.format(r_length, i), 'data', 'w', format='table')
  print("saving to hdf5:", time.time() - t0)

if __name__ == '__main__':
  if len(sys.argv) != 4:
    print("Usage: create_pandas_join_scenario.py <left_length> <right_length>")
    exit(-1)

  go(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))

