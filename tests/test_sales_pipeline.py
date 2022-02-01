import glob
import os

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas._testing import assert_frame_equal


def insert_initial_data(tablename, hook):
    """This script will populate database with initial data to run job"""
    conn_engine = hook.get_sqlalchemy_engine()
    all_files = glob.glob(os.path.join("/opt/airflow/data/", "Base_*.csv"))
    df_from_each_file = (pd.read_csv(f, sep=",") for f in all_files)
    df_merged = pd.concat(df_from_each_file, ignore_index=True)
    df_merged.to_sql(name=tablename, con=conn_engine, if_exists="replace", index=False)


def create_table(tablename, hook):
    sql_query = open(f"/opt/airflow/sql/init/create_{tablename}.sql").read()
    hook.run(sql_query.format(tablename=tablename))


def output_df(filename):
    return pd.read_csv(f"/opt/airflow/data/{filename}.csv")


def output_parquet(filename):
    return pd.read_parquet(f"/opt/airflow/tests/fixtures/{filename}.parquet")


base_file_path = "tests/output/"

# test_sales_pipeline.py
class TestSalesPipeline:
    def test_validate_sales_pipeline(self):

        legacy_hook = PostgresHook("legacy")

        create_table("sales", legacy_hook)
        insert_initial_data("sales", legacy_hook)

        legacy_sales_size = legacy_hook.get_records("select * from sales")
        assert len(legacy_sales_size) == 3000

        years = ["2017", "2018", "2019"]

        for year in years:

            query = f'select * from "sales" WHERE SUBSTR("sales"."DATA_VENDA", 7, 4) = \'{year}\';'

            expected_sales_data = output_df(f"Base_{year}")

            legacy_sales_data = legacy_hook.get_pandas_df(query)
            assert_frame_equal(legacy_sales_data, expected_sales_data)
