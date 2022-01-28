import pandas as pd
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


def output_parquet(filename):
    return pd.read_parquet(f"/opt/airflow/tests/fixtures/{filename}.parquet")


base_file_path = "tests/output/"

# test_sales_pipeline.py
class TestSalesPipeline:
    def test_validate_sales_pipeline(self):

        legacy_hook = PostgresHook("legacy")

        create_table("sales", legacy_hook)
        insert_initial_data("Base_2017", "sales", legacy_hook)

        legacy_sales_2017_size = legacy_hook.get_records("select * from sales")
        assert len(legacy_sales_2017_size) == 1000

        expected_sales_data = output_df("Base_2017")

        legacy_sales_data = legacy_hook.get_pandas_df("select * from sales")
        assert_frame_equal(legacy_sales_data, expected_sales_data)

        list_tables = [
            "sales_year_month",
            "sales_brand_line",
            "sales_brand_year_month",
            "sales_line_year_month",
        ]

        for table in list_tables:

            query = open(f"/opt/airflow/sql/analytics/tb_vis_{table}.sql").read()
            expected_data = output_parquet(table)

            sales_analytics_data = legacy_hook.get_pandas_df(query)
            assert_frame_equal(sales_analytics_data, expected_data)
