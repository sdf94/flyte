import pandas as pd
from flytekit import Resources, kwtypes, task, workflow
from flytekit.types.file import FlyteFile
import typing 

DATASET_LOCAL = "salary.csv"

@task(cache=True, cache_version="1.0")
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    df = df.drop_duplicates()
    df = df[(df != '**').all(1)]
    df = df[(df != '*').all(1)]
    df = df[(df != '#').all(1)]
    return df

@task(cache=True, cache_version="1.0")
def filter_states(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['area_title'].str.contains("WA|OR|CA|PA|TX|GA|FL|MI|")]
    df = df[df['area_title'].str.contains("-")]
    return df

@task(cache=True, cache_version="1.0")
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

@task(cache=True, cache_version="1.0")
def my_task(
       dataset: FlyteFile[typing.TypeVar("csv")]
) -> pd.DataFrame:
   return pd.read_csv(dataset)

@task(cache=True, cache_version="1.0")
def filter_columns(
      df: pd.DataFrame) -> pd.DataFrame: 
    return df[['area_title','occ_title', 'tot_emp',  'jobs_1000', 'a_mean', 'a_pct10', 'a_pct25',
       'a_median', 'a_pct75', 'a_pct90', 'year','o_group']]


@workflow
def file_wf(
   dataset: FlyteFile[
       typing.TypeVar("csv")
   ] = DATASET_LOCAL,
) -> pd.DataFrame:
   df = my_task(dataset=dataset)
   df = filter_columns(df=df)
   df = clean_data(df=df)
   df = filter_states(df=df)
   df = apply_types(df=df)
   return df