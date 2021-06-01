import sys
from collections import OrderedDict
import json
import time
import pandas as pd


def dtype_from_schema(entry):

  ft = entry['field_type']

  if ft == 'categorical':
    if 'out_of_range' in entry['categorical']:
      return 'object'
    else:
      return 'category'
  elif ft == 'numeric':
    if entry['value_type'] in ('uint8', 'int8', 'uint16', 'int16', 'uint32', 'int32', 'bool'):
      return 'float32'
    else:
      return 'float64'
  elif ft == 'fixed_string':
    return 'S{}'.format(entry['length'])
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

  t0 = time.time()
  df = pd.read_csv(src_filename, dtype=column_dtypes)
  print(df.dtypes)
  df.to_hdf(dest_filename, 'data', 'w')
  print(time.time() - t0)


if __name__ == '__main__':
  if len(sys.argv) != 5:
    print("Usage: import_patients_pandas.py "
          "<schema_filename> <table> <source filename> <destination filename>")
    exit(-1)

  t0 = time.time()
  try:
    go(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
  except:
    print("failed after", time.time() - t0)
