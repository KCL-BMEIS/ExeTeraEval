import sys
from collections import OrderedDict
import json
import time
import pandas as pd
import dask
import dask.dataframe as ddf


def dtype_from_schema(entry):

  ft = entry['field_type']

  if ft == 'categorical':
    if 'out_of_range' in entry['categorical']:
      return 'object'
    else:
      # return 'category'
      return 'object'
  elif ft == 'numeric':
    if entry['value_type'] in ('uint8', 'int8', 'uint16', 'int16', 'uint32', 'int32', 'bool'):
      return 'float32'
    else:
      return 'float64'
  else:
    return 'object'


def go(schema_filename, table, src_filename, dest_filename):

  with open(schema_filename, 'r') as f:
    schema = json.load(f)
  schema = schema['schema'][table]['fields']
  column_dtypes = OrderedDict()
  for fk, fv in schema.items():
    column_dtypes[fk] = dtype_from_schema(fv)

  for fk, fv in column_dtypes.items():
    print(fk, fv)

  # t0 = time.time()
  # df = ddf.read_csv(src_filename) #, dtype=column_dtypes)
  # df.compute()
  # print(df)
  # print('to_hdf')
  # df.to_hdf(dest_filename, '/data', lock=dask.utils.SerializableLock(), format='table')
  # print(time.time() - t0)

  t0 = time.time()
  print('read csv')
  df = pd.read_csv(src_filename) #, dtype=column_dtypes)
  print('from pandas')
  df = ddf.from_pandas(df, npartitions=1)
  print('to hdf')
  df.to_hdf(dest_filename, '/data', format='table')
  print(time.time() - t0)

  t0 = time.time()
  df2 = ddf.read_hdf(dest_filename, '/data')
  print(df2.compute())
  print(time.time() - t0)

if __name__ == '__main__':
  if len(sys.argv) != 5:
    print("Usage: import_patients_pandas.py "
          "<schema_filename> <table> <source filename> <destination filename>")
    exit(-1)

  t0 = time.time()
  try:
    go(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
  except Exception as e:
    print("failed after", time.time() - t0)
    raise
