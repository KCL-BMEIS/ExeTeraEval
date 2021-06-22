from exetera.core.session import Session
from exetera.core.utils import Timer


with Session() as s:
  src = s.open_dataset('/home/bm18/covid/ds_20210523_base.hdf5', 'r', 'src')
  dst = s.open_dataset('/home/bm18/covid/ds_20210523_sorted.hdf5', 'w', 'dst')

  print('type(src):', type(src))
  print(src.keys())
  print(src._dataframes)
  for k in src.keys():
    print(type(src[k]))

  # sort patients
  with Timer("sorting patients", new_line=True):
    dst.create_dataframe('patients')
    for k in src['patients'].keys():
      print(k, type(src['patients'][k]), len(src['patients'][k]))
    print('a:', src['patients']['zipcode'].indices[:20])
    print('b:', src['patients']['zipcode'].indices[-20:])
    print('c:', src['patients']['zipcode'].data[-20:])
    s.sort_on(src['patients'], dst['patients'], ('id',))

  # sort assessments
  with Timer("sorting assessments", new_line=True):
    dst.create_dataframe('assessments')
    s.sort_on(src['assessments'], dst['assessments'], ('patient_id', 'created_at'))

  # sort tests
  with Timer("sorting tests", new_line=True):
    dst.create_dataframe('tests')
    s.sort_on(src['tests'], dst['tests'], ('patient_id', 'created_at'))
