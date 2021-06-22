from exetera.core.session import Session
from exetera.core.dataframe import merge
from exetera.core.utils import Timer

with Session() as s:
  src = s.open_dataset('/home/bm18/covid/ds_20210523_sorted.hdf5', 'r', 'src')
  dst = s.open_dataset('/home/bm18/covid/ds_20210523_merged.hdf5', 'w', 'dst')

  ptnts = src['patients']
  asmts = src['assessments']
  merged = dst.create_dataframe('merged')
  with Timer("merging", new_line=True):
    merge(ptnts, asmts, merged, left_on='id', right_on='patient_id', how='left',
          left_fields=('id',),
          right_fields=('health_status', 'tested_covid_positive', 'temperature'),
          hint_left_keys_ordered=True, hint_right_keys_ordered=True)
