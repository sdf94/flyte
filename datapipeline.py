import pandas as pd
from flytekit import Resources, kwtypes, task, workflow
from flytekit.types.file import FlyteFile
import urllib
import typing 

DATASET_LOCAL = "salary.csv"
DATASET_REMOTE = "https://raw.githubusercontent.com/sdf94/flyte/master/salary.csv"

@task
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    df = df.drop_duplicates()
    df = df[(df != '**').all(1)]
    df = df[(df != '*').all(1)]
    df = df[(df != '#').all(1)]
    return df

@task
def filter_states(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['area_title'].str.contains("WA|OR|CA|PA|TX|GA|FL|MI|")]
    df = df[df['area_title'].str.contains("-")]
    return df

@task
def apply_types(df: pd.DataFrame) -> pd.DataFrame:
    return df.astype({"area_title": 'object',
                              "occ_title": 'object', 
                              "tot_emp":'float',
                              "jobs_1000":float,
                              "a_mean":float, 
                              "a_pct10":float,
                              "a_pct25":float,
                              "a_median":float,
                              "a_pct75":float,
                              "a_pct90":float,
                              "year":'object',
                              'o_group':str})

@task
def download_file(
       dataset: str
) -> pd.DataFrame:
    urllib.request.urlretrieve(dataset, DATASET_LOCAL)
    df = pd.read_csv(DATASET_LOCAL)
    return df

@task
def filter_columns(
      df: pd.DataFrame) -> pd.DataFrame: 
    return df[['area_title','occ_title', 'tot_emp',  'jobs_1000', 'a_mean', 'a_pct10', 'a_pct25',
       'a_median', 'a_pct75', 'a_pct90', 'year','o_group']]


@workflow
def file_wf(
   dataset: str
    = DATASET_REMOTE
) -> pd.DataFrame:
   df = download_file(dataset=dataset)
   df = filter_columns(df=df)
   df = clean_data(df=df)
   df = filter_states(df=df)
   df = apply_types(df=df)
   return df