import numpy as np
from exetera.core.session import Session


with Session() as s:
  ds = s.open_dataset('./ds_100000_1000000.hdf5', 'r', 'ds')
  mds = s.open_dataset('./mds_100000_1000000.hdf5', 'r', 'mds')

  print(ds.keys())

  for k in ds.keys():
    df = ds[k]
    print([k for k in df])
    for i in range(20):
      line = list()
      for k in df:
        line.append(df[k].data[i])
      print(line)

  mdf = mds['merged']
  print([k for k in mdf])
  len_mdf = len(mdf['fk'])
  print(len_mdf)
  for i in range(len_mdf-20, len_mdf):
    line = list()
    for k in mdf:
      line.append(mdf[k].data[i])
    print(line)

  filt = mdf['fk'] == 0
  print(np.unique(filt.data[:], return_counts=True))
