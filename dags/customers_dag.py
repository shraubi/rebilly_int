from datetime import datetime, timedelta
import requests
import json
import re

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.api_core.exceptions import BadRequest

from utils import chunks

DATASET_NAME = "Rebilly"
TABLE_NAME = 'customers'
headers = {"REB-APIKEY": "sk_live_LO-uyzhEfsz9NY5jR7dqNb10ko-PpPSAhf1D2dj"}
organization_id = "b5672fbd-8023-4d9b-a31b-1d7eaf9edb8b"
schema = [
    {"name": "id", "type": "STRING"},
    {"name": "email", "type": "STRING"},
    {"name": "firstName", "type": "STRING"},
    {"name": "lastName", "type": "STRING"},
    {"name": "createdTime", "type": "TIMESTAMP"},
    {"name": "updatedTime", "type": "TIMESTAMP"},
    {
        "name": "customFields",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
    {
        "name": "defaultPaymentInstrument",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "method", "type": "STRING", "mode": "NULLABLE"},
            {"name": "paymentInstrumentId", "type": "STRING", "mode": "NULLABLE"},
            {"name": "paymentCardId", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
    {
        "name": "primaryAddress",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "firstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "organization", "type": "STRING", "mode": "NULLABLE"},
            {"name": "address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "address2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "postalCode", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "phoneNumbers",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "label", "type": "STRING"},
                    {"name": "value", "type": "STRING"},
                ],
            },
            {
                "name": "emails",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "label", "type": "STRING"},
                    {"name": "primary", "type": "BOOLEAN"},
                    {"name": "value", "type": "STRING"},
                ],
            },
            {"name": "dob", "type": "STRING"},
            {"name": "jobTitle", "type": "STRING"},
            {"name": "hash", "type": "STRING"},
        ],
    },
    {"name": "company", "type": "STRING"},
    {
        "name": "lifetimeRevenue",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "currency", "type": "STRING"},
            {"name": "amount", "type": "FLOAT"},
            {"name": "amountUsd", "type": "FLOAT"},
        ],
    },
    {"name": "invoiceCount", "type": "INTEGER"},
    {
        "name": "averageValue",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "currency", "type": "STRING"},
            {"name": "amount", "type": "FLOAT"},
            {"name": "amountUsd", "type": "FLOAT"},
        ],
    },
    {"name": "paymentCount", "type": "INTEGER"},
    {"name": "lastPaymentTime", "type": "TIMESTAMP"},
    {"name": "revision", "type": "INTEGER"},
    {"name": "tags", "type": "RECORD", "mode": "NULLABLE",
     "fields": [{"name": "id", "type": "STRING"},
      {"name": "name", "type": "STRING"},
      {"name": "type", "type": "STRING"},
      {"name": "createdTime", "type": "STRING"},
      {"name": "updatedTime", "type": "STRING"}],
    },
    {"name": "websiteId", "type": "STRING"},
    {"name": "organizationId", "type": "STRING"},
    {"name": "locale", "type": "STRING"},
    {"name": "isEddRequired", "type": "BOOLEAN"},
    {"name": "hasFulfilledKyc", "type": "BOOLEAN"},
    {
        "name": "taxNumbers",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [{"name": "taxNumber", "type": "STRING"}],
    },
    {
        "name": "_links",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "rel", "type": "STRING"},
            {"name": "href", "type": "STRING"},
        ],
    },
]


def get_data(start_time: str, end_time: str = None) -> list:
    start_dt = datetime.strptime(start_time, "%Y%m%dT%H%M%S")
    if not end_time:
        end_dt = start_dt + timedelta(days=1)
    else:
        end_dt = datetime.strptime(end_time, "%Y%m%dT%H%M%S")
    url = "https://api.rebilly.com/organizations/" + organization_id + "/customers"
    query = {
        "limit": "1000",
        "offset": "0",
        "filter": f"createdTime:{start_dt.strftime('%Y-%m-%dT%H:%M:%S')}Z..{end_dt.strftime('%Y-%m-%dT%H:%M:%S')}Z",
        "sort": "createdTime",
    }
    response = requests.get(url, headers=headers, params=query)
    if int(response.headers["Pagination-total"]) <= 1000:
        return json.loads(response.text)
    delta = end_dt - start_dt
    return get_data(
        start_dt.strftime("%Y%m%dT%H%M%S"), (start_dt + delta / 2).strftime("%Y%m%dT%H%M%S")
    ) + get_data(
        (start_dt + delta / 2).strftime("%Y%m%dT%H%M%S"), end_dt.strftime("%Y%m%dT%H%M%S")
    )


def transform(items):
    def schema_to_struct(schema):
        fields = ', '.join([f"NULL AS `{field['name']}`" for field in schema['fields']])
        return f"STRUCT({fields})"
    def is_float(s):
        try:
            float(s)
            return True
        except ValueError:
            return False
    def is_int(s):
        try:
            int(s)
            return True
        except ValueError:
            return False
    def sql_repr(value, inside_dict=False, key=None):
        if inside_dict and value is None:
            if key:
                field_schema = next((field for field in schema if field['name'] == key), None)
                if field_schema:
                    return schema_to_struct(field_schema)
            return 'NULL'
        elif value is None:
            return 'NULL'
        elif key == '_links':
            link = [f"'{value}' AS `{key}`" for key, value in value[0].items()]
            return f"STRUCT({', '.join(link)})"
        elif isinstance(value, dict):
            if value:
                if any(isinstance(v, dict) for v in value.values()):
                    inside = ', '.join([f"{sql_repr(v, True, k)}" for k, v in value.items()])
                else:
                    inside = ', '.join([f"{sql_repr(v, k)} AS `{k}`" for k, v in value.items()])
                return f"STRUCT({inside})"
            else:
                return 'NULL'
        elif isinstance(value, str):
            if value == '[None]':
                return 'NULL'
            if is_float(value) and not inside_dict:
                return float(value)
            if is_int(value) and not inside_dict:
                return int(value)
            return f"'{value}'"
        elif isinstance(value, list):
            if not value or value == 'None':
                return 'NULL'
            else:
                    converted_list = [sql_repr(v, key=key) for v in value]
                    return f"({', '.join(converted_list)})"
        else:
            return str(value)

    keys = ', '.join([f"`{k}`" for k in items[0].keys()])
    sql_string = f"INSERT INTO {DATASET_NAME}.{TABLE_NAME}({keys}) VALUES "
    for item in items:
        values = ', '.join([str(sql_repr(item.get(k, None), None, k)) for k in items[0].keys()])
        sql_string += f"({values}),"
    sql_string = re.sub(",$", ";", sql_string)
    return sql_string


def insert_data(data):
    bq_hook = bigquery.Client()
    job_config = QueryJobConfig(use_legacy_sql=False)

    chunk_size = 200

    for chunk in chunks(data, chunk_size):
        sql_string = transform(chunk)
        query_job = bq_hook.query(sql_string, job_config=job_config)
        if query_job.error_result:
            print(sql_string)
        query_job.result()

with DAG(
    dag_id="Rebilly.customers",
    start_date=datetime.now() - timedelta(days=730),
    schedule="@daily",
    catchup=True,
    max_active_tasks=10,
    max_active_runs=2,
    concurrency=3,
) as dag:
    get_data_task = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        op_args=["{{ ts_nodash }}"],
        do_xcom_push=True,
    )

    # create_table_task = BigQueryCreateEmptyTableOperator(
    #     task_id="create_customers_table",
    #     dataset_id=DATASET_NAME,
    #     table_id="customers",
    #     schema_fields=schema,
    # )

    def execute_insert_queries(**kwargs):
        data = kwargs["ti"].xcom_pull(key="return_value", task_ids="get_data")
        if data:
            insert_data(data)
        else:
            print("No data")

    insert_data_task = PythonOperator(
        task_id="execute_insert_queries",
        python_callable=execute_insert_queries,
        execution_timeout=timedelta(hours=2)
    )

    get_data_task >> insert_data_task