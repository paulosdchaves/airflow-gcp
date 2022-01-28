import pandas as pd
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas._testing import assert_frame_equal


def insert_initial_data(filename, tablename, hook):
    """This script will populate database with initial data to run job"""
    conn_engine = hook.get_sqlalchemy_engine()
    sample_data = pd.read_csv(f"/opt/airflow/data/{filename}.csv")
    sample_data.to_sql(
        name=tablename, con=conn_engine, if_exists="replace", index=False
    )


def create_table(tablename, hook):
    sql_query = open(f"/opt/airflow/sql/init/create_{tablename}.sql").read()
    hook.run(sql_query.format(tablename=tablename))


def output_df(filename):
    return pd.read_csv(f"/opt/airflow/data/{filename}.csv")


base_file_path = "tests/output/"

# test_sales_pipeline.py
class TestSalesPipeline:
    @pytest.mark.dependency(name="test_a")
    def test_validate_sales_pipeline(self):

        legacy_hook = PostgresHook("legacy")

        create_table("sales", legacy_hook)
        insert_initial_data("Base_2017", "sales", legacy_hook)

        legacy_sales_2017_size = legacy_hook.get_records("select * from sales")
        assert len(legacy_sales_2017_size) == 1000

        expected_sales_data = output_df("Base_2017")

        legacy_sales_data = legacy_hook.get_pandas_df("select * from sales")
        assert_frame_equal(legacy_sales_data, expected_sales_data)
