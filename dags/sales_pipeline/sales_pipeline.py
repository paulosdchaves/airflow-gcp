import datetime
import json
import logging
import os
from pathlib import Path

import airflow
import yaml
from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

HERE = Path(__file__).parent.absolute()
DAG_NAME = Path(__file__).parent.name
DAG_CONFIG_FILE = HERE.joinpath(f"{DAG_NAME}.yml")
JSON_CONFIG_FILE = HERE.joinpath(f"{DAG_NAME}.json")
DAG_CONFIG_PAGARME_KEY = "env_sinistro_pipeline"

AIRFLOW_ENV = os.getenv("AIRFLOW_ENV", "dev")
GOOGLE_CONN_ID = "google_cloud_default"

# Load the DAG configuration, setting a default if none is present
with open(DAG_CONFIG_FILE) as f:
    config = yaml.safe_load(f)
    config = config[AIRFLOW_ENV]
config = json.loads(json.dumps(config, default=str))

# Load the entity configuation
with open(JSON_CONFIG_FILE) as json_file:
    entities_config = json.load(json_file)


def parquet_to_gcs(entity_snake_case, query_sql, database, bucket_name):
    gcs_hook = GoogleCloudStorageHook(GOOGLE_CONN_ID)
    pg_hook = PostgresHook(f"{database}")

    # Execute the query
    logging.info("Exporting sql to parquet '%s'", entity_snake_case)
    df = pg_hook.get_pandas_df(sql=query_sql)

    prefix = "sales_{}.ftr".format(datetime.timedelta(days=1)).strftime("%Y%m%d")
    filename = os.path.join("airflow/tests/output/", prefix)
    df.to_parquet(filename, index=False)

    with open(filename, "rb") as source_file:

        logging.info("Uploading to %s/%s", bucket_name, source_file)
        gcs_hook.upload(bucket_name, source_file, prefix)


# def create_fact_table(entity_snake_case, fact_sql, destination_fact, dag):
#     return BigQueryOperator(
#         task_id=f"tk_fat_{entity_snake_case}",
#         use_legacy_sql=False,
#         create_disposition="CREATE_IF_NEEDED",
#         write_disposition="WRITE_TRUNCATE",
#         params={
#             "idProject": config["environment"]["project_id"],
#             "idDataSet": config["environment"]["refined_dataset_id"],
#         },
#         allow_large_results=True,
#         sql=fact_sql,
#         destination_dataset_table=f"{config['environment']['project_id']}.{config['environment']['refined_dataset_id']}.{destination_fact}",
#         bigquery_conn_id="gcp_vida_previdencia",
#         dag=dag,
#     )


# def create_vis_table(entity_snake_case, vis_sql, destination_vis, dag):
#     return BigQueryOperator(
#         task_id=f"tk_vis_{entity_snake_case}",
#         use_legacy_sql=False,
#         create_disposition="CREATE_IF_NEEDED",
#         write_disposition="WRITE_TRUNCATE",
#         params={
#             "idProject": config["environment"]["project_id"],
#             "idDataSet": config["environment"]["refined_dataset_id"],
#         },
#         allow_large_results=True,
#         sql=vis_sql,
#         destination_dataset_table=f"{config['environment']['project_id']}.{config['environment']['dmt_dataset_id']}.{destination_vis}",
#         bigquery_conn_id="gcp_vida_previdencia",
#         dag=dag,
#         trigger_rule=TriggerRule.ALL_DONE,
#     )


def create_dag(
    dag_id,
    schedule_interval,
    description,
    is_paused_upon_creation,
    entity_snake_case,
    query_sql,
    database,
    bucket_name,
    default_args,
):

    dag = DAG(
        dag_id=dag_id,
        start_date=airflow.utils.dates.days_ago(1),
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        is_paused_upon_creation=is_paused_upon_creation,
    )

    with dag:

        start_pipeline = DummyOperator(task_id="start_pipeline")

        end_pipeline = DummyOperator(
            task_id="end_pipeline", trigger_rule=TriggerRule.ALL_DONE, dag=dag
        )

        postgresql_to_parquet = parquet_to_gcs(
            entity_snake_case, query_sql, database, bucket_name, dag
        )

        postgresql_to_parquet.set_upstream(start_pipeline)
        end_pipeline.set_upstream(postgresql_to_parquet)

    return dag


for entity, entity_config in entities_config.items():

    dag_id = f"pipeline_{entity}"

    globals()[dag_id] = create_dag(
        dag_id=dag_id,
        schedule_interval=config["dag"]["schedule_interval"],
        description=config["dag"]["description"],
        is_paused_upon_creation=config["dag"]["is_paused_upon_creation"],
        entity_snake_case=entity,
        query_sql=entity_config["query_sql"],
        database=entity_config["database"],
        bucket_name=config["environment"]["refined_bucket"],
        default_args=config["dag"],
    )
