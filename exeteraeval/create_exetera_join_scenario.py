import sys
import time
import numpy as np
import pandas as pd
from exetera.core.session import Session

def go(l_length, r_length):
  with Session() as s:
    ds = s.open_dataset('ds_{}_{}.hdf5'.format(l_length, r_length), 'w', 'ds')

    t0 = time.time()
    rng = np.random.RandomState(12345678)
    l_df = ds.create_dataframe('left')
    l_key_ = np.arange(1, l_length + 1) * 10
    l_df.create_numeric('pk', 'int64').data.write(l_key_)
    l_df.create_numeric('l0', 'int64').data.write(rng.randint(1, 1000, l_length) * 1000)
    l_df.create_numeric('l1', 'float64').data.write(rng.uniform(0.0, 1.0, l_length))
    print("left_table_creation:", time.time() - t0)

    t0 = time.time()
    rng = np.random.RandomState(12345679)
    r_df = ds.create_dataframe('right')
    r_df.create_numeric('pk', 'int64').data.write(np.arange(1, r_length + 1) * 10)
    lf_key_ = rng.randint(0, l_length, r_length)
    lf_key_ = l_key_[lf_key_]
    r_df.create_numeric('fk', 'int64').data.write(lf_key_)
    s.sort_on(r_df, r_df, ('fk',))
    del l_key_
    del lf_key_
    r_df.create_numeric('r0', 'int64').data.write(rng.randint(1000, 10000, r_length) * 1000)
    r_df.create_numeric('r1', 'float64').data.write(rng.uniform(0.0, 1.0, r_length))
    print("right_table_creation:", time.time() - t0)


if __name__ == '__main__':
  if len(sys.argv) != 3:
    print("Usage: create_exetera_join_scenario.py <left_length> <right_length>")
    exit(-1)

  go(int(sys.argv[1]), int(sys.argv[2]))
