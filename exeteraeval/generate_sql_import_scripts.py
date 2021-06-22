import sys
import csv
import json
from io import StringIO

# schema_filename = '/home/ben/covid/covid_schema_20210505.json'
# import_filename = '/home/ben/covid/patients_export_geocodes_20210331040019.csv'

def go(schema_filename, table_name, import_filename):
  with open(schema_filename, 'r') as f:
    src = json.load(f)
  schema = src['schema']

  for k in schema.keys():
    enums = []
    columns = []
    unrecognised = []
    force_null = []

    if k == table_name:

      fields = schema[k]['fields']
      for f in fields.items():
        print('f:', f[0], f[1]['field_type'])
        if f[1]['field_type'] == 'categorical':
          if 'out_of_range' in f[1]['categorical']:
            columns.append("{} TEXT".format(f[0]))
          else:
            ctype = '{}_{}_e'.format(k, f[0])
            enumvals = [e[0] for e in f[1]['categorical']['strings_to_values'].items()]
            enums.append(
              'CREATE TYPE {} AS ENUM ({});'.format(
                ctype, ', '.join(["'{}'".format(e) for e in enumvals])))
            columns.append("{} {}".format(f[0], ctype))
        elif f[1]['field_type'] == 'numeric':
          if f[1]['value_type'] in ('uint8', 'int8', 'int16'):
            columns.append("{} int2".format(f[0]))
            force_null.append(f[0])
          elif f[1]['value_type'] in ('uint16', 'int32'):
            columns.append("{} int4".format(f[0]))
            force_null.append(f[0])
          elif f[1]['value_type'] in ('uint32', 'int64'):
            columns.append("{} int8".format(f[0]))
            force_null.append(f[0])
          elif f[1]['value_type'] == 'float32':
            columns.append("{} float8".format(f[0]))
            force_null.append(f[0])
          elif f[1]['value_type'] == 'float64':
            columns.append("{} float8".format(f[0]))
            force_null.append(f[0])
          elif f[1]['value_type'] == 'bool':
            columns.append("{} bool".format(f[0]))
            force_null.append(f[0])
          else:
            columns.append("{} {}".format(f[0], 'TEXT'))
        elif f[1]['field_type'] == 'fixed_string':
          columns.append("{} VARCHAR({})".format(f[0], f[1]['length']))
        elif f[1]['field_type'] == 'string':
          columns.append("{} {}".format(f[0], 'TEXT'))
        elif f[1]['field_type'] == 'datetime':
          columns.append("{} {}".format(f[0], 'TIMESTAMP'))
        elif f[1]['field_type'] == 'date':
          columns.append("{} date".format(f[0], 'date'))
        else:
          unrecognized.append("{} {}".format(f[0], f[1]['field_type']))

      strio = StringIO()
      for e in enums:
        strio.write(e+'\n')

      strio.write('CREATE TABLE {} (\n'.format(k))
      strio.write(',\n'.join(['  '+e for e in columns]))
      strio.write('\n')
      strio.write(');\n')

      strio.write('COPY {}('.format(k))
      with open(import_filename, 'r') as csvf:
        csvr = csv.reader(csvf)
        strio.write(','.join(h for h in next(csvr, None)))
      strio.write(
        ") FROM '{}' WITH (FORMAT CSV, DELIMITER ',', FORCE_NULL({}), HEADER);\n".format(
          import_filename, ','.join(force_null)))

      print('unrecognised fields:')
      for u in unrecognised:
        print(u)

      print(strio.getvalue())
      print(len(strio.getvalue()))
      with open('/home/ben/covid/{}_table.sql'.format(k), 'w') as of:
        of.write(strio.getvalue())

      exit()

  print("failed to find table {}".format(table_name))

if __name__ == '__main__':
  print(sys.argv)

  _, schema_filename, table_name, import_filename = sys.argv
  go(schema_filename, table_name, import_filename)
