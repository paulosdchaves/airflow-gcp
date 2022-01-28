import os

import pandas as pd
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas._testing import assert_frame_equal


def output_df(filename):
    return pd.read_parquet(f"/opt/airflow/tests/output/{filename}.parquet")


# test_sales analytics_pipeline.py
class TestSalesAnalyticsPipeline:
    @pytest.mark.dependency(depends=["TestSalesPipeline::test_validate_sales_pipeline"])
    @pytest.mark.run(order=1)
    def test_validate_dataframe_pipeline(self):

        legacy_hook = PostgresHook("legacy")

        list_tables = ["sales_year_month", "sales_brand_line"]

        for table in list_tables:

            query = open(f"/opt/airflow/sql/analytics/tb_vis_{table}.sql").read()

            df = legacy_hook.get_pandas_df(query)

            filename = f"{table}.parquet"
            path = os.path.join("/opt/airflow/tests/output/", filename)
            df.to_parquet(path)

            expected_data = output_df(table)

            sales_analytics_data = legacy_hook.get_pandas_df(query)
            assert_frame_equal(sales_analytics_data, expected_data)