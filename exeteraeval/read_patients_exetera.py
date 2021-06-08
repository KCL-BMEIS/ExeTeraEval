import sys
import time
from exetera.core.session import Session


def go(dataset_name, count):
  columns = ['id', 'created_at', 'country_code', 'year_of_birth',
             'height_cm', 'weight_kg', 'ever_had_covid_test', 'smoker_status']
  t0 = time.time()
  with Session() as s:
    ds = s.open_dataset(dataset_name, 'r', 'ds')
    df = ds['patients']
    for k in columns[:count]:
      print(df[k])
      vals = df[k].data[:]
      print(len(vals))
  print(time.time() - t0)

if __name__ == '__main__':
  if len(sys.argv) != 3:
    print("Usage: read_patients_pandas.py <dataset> <count>")
    exit(-1)

  t0 = time.time()
  try:
    go(sys.argv[1], int(sys.argv[2]))
  except:
    print("Failed after", time.time() - t0, "seconds")
    raise
