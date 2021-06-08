import h5py


with h5py.File('/home/bm18/git/exeteraeval/exeteraeval/pandas_patients.hdf5', 'r') as ds:
  print(ds.keys())
  data = ds['data']
  table = data['table']
  print(table.shape)
  print(table.dtype)
