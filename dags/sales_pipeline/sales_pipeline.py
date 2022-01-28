import json
import logging
import os
import tempfile
from pathlib import Path

import airflow
import yaml
from airflow import DAG
from airflow.contrib.operators import gcs_to_bq
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
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


def parquet_to_gcs(entity_snake_case, query_sql, database, bucket_name, **context):
    year = context["execution_date"].year
    month = context["execution_date"].month

    pg_hook = PostgresHook(f"{database}")

    # Execute the query
    logging.info("Exporting sql to parquet '%s'", entity_snake_case)
    df = pg_hook.get_pandas_df(sql=open(query_sql).read())

    prefix = f"{entity_snake_case}.parquet"

    # Write sales to temp file.
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = os.path.join(tmp_dir, prefix)
        df.to_parquet(tmp_path, index=False)

        # Upload file to GCS.
        logging.info(f"Writing results to sales/{year}/{month:02d}.csv")
        gcs_hook = GCSHook(GOOGLE_CONN_ID)
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=f"sales/{prefix}",
            filename=tmp_path,
        )


def generate_export_gcs_to_bq(
    entity_snake_case,
    bucket_name,
    destination_dataset_table,
    dag,
):

    return gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id=f"tk_gcs_to_bq_{entity_snake_case}",
        bucket=bucket_name,
        source_objects=[f"sales/{entity_snake_case}.parquet"],
        destination_project_dataset_table=destination_dataset_table,
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        bigquery_conn_id=GOOGLE_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CONN_ID,
        dag=dag,
        autodetect=True,
    )


def create_dag(
    dag_id,
    schedule_interval,
    description,
    is_paused_upon_creation,
    entity_snake_case,
    query_sql,
    database,
    destination_dataset_table,
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

        postgresql_to_parquet = PythonOperator(
            task_id=f"tk_copy_to_gcs_{entity_snake_case}",
            python_callable=parquet_to_gcs,
            op_kwargs={
                "entity_snake_case": entity_snake_case,
                "query_sql": query_sql,
                "database": database,
                "bucket_name": bucket_name,
            },
            dag=dag,
        )

        export_gcs_to_bq = generate_export_gcs_to_bq(
            entity_snake_case,
            bucket_name,
            destination_dataset_table,
            dag,
        )

        postgresql_to_parquet.set_upstream(start_pipeline)
        export_gcs_to_bq.set_upstream(postgresql_to_parquet)
        end_pipeline.set_upstream(export_gcs_to_bq)

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
        destination_dataset_table=entity_config["destination_dataset_table"],
        bucket_name=config["environment"]["refined_bucket"],
        default_args=config["dag"],
    )
