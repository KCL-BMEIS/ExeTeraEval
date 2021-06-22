import sys
import time
from exetera.core.session import Session
from exetera.core.dataframe import merge

def go(l_length, r_length):
  with Session() as s:
    ds = s.open_dataset('ds_{}_{}.hdf5'.format(l_length, r_length), 'r', 'ds')
    mds = s.open_dataset('mds_{}_{}.hdf5'.format(l_length, r_length), 'w', 'mds')
    l_df = ds['left']
    r_df = ds['right']

    m_df = mds.create_dataframe('merged')
    t0 = time.time()
    merge(l_df, r_df, m_df, 'pk', 'fk',
          left_fields=('pk', 'l0', 'l1'), right_fields=('fk', 'r0', 'r1'),
          hint_left_keys_ordered=True, hint_right_keys_ordered=True)
    print("exetera: merged in {}".format(time.time() - t0))
    print(len(m_df['fk']))


if __name__ == '__main__':
  if len(sys.argv) != 3:
    print("Usage: exetera_join_scenario.py <left_length> <right_length>")
    exit(-1)

  go(int(sys.argv[1]), int(sys.argv[2]))
