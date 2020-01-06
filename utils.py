import os
import pandas as pd
import settings
# awesome-slugify
from slugify import slugify, Slugify, slugify_filename

def rename_slugify_file(filepath):
  basepath, filename = os.path.split(filepath)
  src = os.path.join(basepath, filename)
  dest = os.path.join(basepath, slugify_filename(filename))
  os.rename(src, dest)
  print('src: {}'.format(src))
  print('dest: {}'.format(dest))
  return dest


def file_to_df(file):

  file_name, file_extension = os.path.splitext(file)
  print('{file_name}\n{file_extension}'.format(file_name=file_name, file_extension=file_extension ))
  #df = pd.read_csv(csvfile, encoding='latin1', sep=';')
  if (file_extension == ".xlsx" or file_extension == ".xls"):
    df = pd.read_excel(file)
    print('{file} - file to df sucess'.format(file=file))
    return df
  
  if file_extension == ".csv":
    df = pd.read_csv(file, encoding='latin1', sep=';')
    print('{file} - file to df sucess'.format(file=file))
    return df

  else:
    print ('{file} er ikke xls, xlsx eller csv'.format(file=file))

def query_result(q, conn):
  try:
    cursor = conn.cursor(as_dict=True)
    cursor.execute(q)
    result = cursor.fetchall()
    return result
  except Exception as e:
    result = 'error failure: query_result (failed to connect to db. probably.)'
    raise e
  return result

def query_commit(q, conn):
  try:
    cursor = conn.cursor()
    cursor.execute(q)
    conn.commit()
    print( 'commit' )
  except Exception as e:
    print( 'error failure: query_commit' )
    raise e



