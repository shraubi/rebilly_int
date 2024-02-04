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
TABLE_NAME = "invoices"
headers = {"REB-APIKEY": "sk_live_LO-uyzhEfsz9NY5jR7dqNb10ko-PpPSAhf1D2dj"}
organization_id = "b5672fbd-8023-4d9b-a31b-1d7eaf9edb8b"
schema = [
    {"name": "id", "type": "STRING"},
    {"name": "dueTime", "type": "STRING"},
    {"name": "issuedTime", "type": "STRING"},
    {"name": "abandonedTime", "type": "STRING"},
    {"name": "voidedTime", "type": "STRING"},
    {"name": "paidTime", "type": "STRING"},
    {"name": "currency", "type": "STRING"},
    {"name": "invoiceNumber", "type": "INTEGER"},
    {"name": "customerId", "type": "STRING"},
    {"name": "websiteId", "type": "STRING"},
    {"name": "organizationId", "type": "STRING"},
    {"name": "subscriptionId", "type": "STRING"},
    {"name": "billingAddress", "type": "RECORD", "fields": [
        {"name": "country", "type": "STRING"},
        {"name": "firstName", "type": "STRING"},
        {"name": "lastName", "type": "STRING"},
        {"name": "organization", "type": "STRING"},
        {"name": "address", "type": "STRING"},
        {"name": "address2", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "region", "type": "STRING"},
        {"name": "postalCode", "type": "STRING"},
        {"name": "phoneNumbers", "type": "STRING"},
        {"name": "emails", "type": "RECORD", "fields": [
            {"name": "label", "type": "STRING"},
            {"name": "primary", "type": "BOOLEAN"},
            {"name": "value", "type": "STRING"}
        ]},
        {"name": "dob", "type": "STRING"},
        {"name": "jobTitle", "type": "STRING"},
        {"name": "hash", "type": "STRING"}
    ]},
    {"name": "deliveryAddress", "type": "RECORD", "fields": [
        {"name": "country", "type": "STRING"},
        {"name": "firstName", "type": "STRING"},
        {"name": "lastName", "type": "STRING"},
        {"name": "organization", "type": "STRING"},
        {"name": "address", "type": "STRING"},
        {"name": "address2", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "region", "type": "STRING"},
        {"name": "postalCode", "type": "STRING"},
        {"name": "phoneNumbers", "type": "STRING"},
        {"name": "emails", "type": "RECORD", "fields": [
            {"name": "label", "type": "STRING"},
            {"name": "primary", "type": "BOOLEAN"},
            {"name": "value", "type": "STRING"}
        ]},
        {"name": "dob", "type": "STRING"},
        {"name": "jobTitle", "type": "STRING"},
        {"name": "hash", "type": "STRING"}
    ]},
    {"name": "amount", "type": "FLOAT"},
    {"name": "amountDue", "type": "FLOAT"},
    {"name": "subtotalAmount", "type": "FLOAT"},
    {"name": "shipping", "type": "RECORD", "fields": [
        {"name": "calculator", "type": "STRING"},
        {"name": "amount", "type": "FLOAT"},
        {"name": "rateId", "type": "STRING"}
    ]},
    {"name": "poNumber", "type": "STRING"},
    {"name": "notes", "type": "STRING"},
    {"name": "items", "type": "RECORD", "fields": [
        {"name": "id","type": "STRING" }, 
        {"name": "type","type": "STRING" },
        {"name": "description","type": "STRING" }, 
        {"name": "unitPrice","type": "FLOAT" }, 
        {"name": "quantity","type": "INTEGER" }, 
        {"name": "price","type": "FLOAT" }, 
        {"name": "subscriptionId","type": "STRING" }, 
        {"name": "productId","type": "STRING" }, 
        {"name": "planId","type": "STRING" }, 
        {"name": "periodStartTime","type": "STRING" }, 
        {"name": "periodEndTime","type": "STRING" }, 
        {"name": "periodNumber","type": "INTEGER" }, 
        {"name": "tax","type": "FLOAT" }, 
        {"name": "createdTime","type": "STRING" }, 
        {"name": "updatedTime","type": "STRING" }, 
        {"name": "subscription","type": "STRING" }, 
        {"name": "rebillNumber","type": "INTEGER" }, 
        {"name": "_links","type": "RECORD","fields": [  
            {"name": "rel","type": "STRING"  },  
            {"name": "href","type": "STRING"  }] }
    ]}, 
    {"name": "discountAmount", "type": "FLOAT"},
    {"name": "discounts", "type": "RECORD", "mode":"NULLABLE", "fields": [
        {"name": "value", "type": "STRING"}
    ]},
    {"name": "tax", "type": "RECORD", "fields": [
        {"name": "calculator", "type": "STRING"},
        {"name": "amount", "type": "FLOAT"},
        {"name": "items", "type": "RECORD", "mode": "NULLABLE", "fields": [
            {"name": "value", "type": "STRING"}
        ]}
    ]},
    {"name": "autopayScheduledTime", "type": "STRING"},
    {"name": "autopayRetryNumber", "type": "INTEGER"},
    {"name": "retryInstruction", "type": "STRING"},
    {"name": "revision", "type": "INTEGER"},
    {"name": "status", "type": "STRING"},
    {"name": "type", "type": "STRING"},
    {"name": "delinquentCollectionPeriod", "type": "INTEGER"},
    {"name": "collectionPeriod", "type": "INTEGER"},
    {"name": "dueReminderTime", "type": "STRING"},
    {"name": "dueReminderNumber", "type": "INTEGER"},
    {"name": "paymentFormUrl", "type": "STRING"},
    {"name": "createdTime", "type": "STRING"},
    {"name": "updatedTime", "type": "STRING"},
    {"name": "rebillNumber", "type": "INTEGER"},
    {"name": "customerTaxIdNumber", "type": "STRING"},
    {"name": "organizationTaxIdNumber", "type": "STRING"},
    {"name": "_links", "type": "RECORD", "fields": [
        {"name": "rel", "type": "STRING"},
        {"name": "href", "type": "STRING"}]},
    {"name": "customer", "type": "STRING"},
    {"name": "website", "type": "STRING"},
    {"name": "closedTime", "type": "STRING"},
    {"name": "taxAmount", "type": "FLOAT"},
    {"name": "shippingAmount", "type": "FLOAT"}
]

def get_data(start_time: str, end_time: str = None) -> list:
    start_dt = datetime.strptime(start_time, "%Y%m%dT%H%M%S")
    if not end_time:
        end_dt = start_dt + timedelta(days=1)
    else:
        end_dt = datetime.strptime(end_time, "%Y%m%dT%H%M%S")
    url = "https://api.rebilly.com/organizations/" + organization_id + "/invoices"
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

def transform(items, schema):
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

    def get_subfield_value(value, key, field_schema):
        if value is None and field_schema['type'] == 'STRING':
            return "'None'"
        elif key in ['address', 'postalCode']:
            return f"""'{str(value).replace("'", " ")}'"""
        else:
            return sql_repr(value, key=key, field_schema=field_schema)

    def sql_repr(value, key=None, field_schema=None):
        if value is None:
            return 'NULL'
        elif isinstance(value, dict):
            subfields = []
            for field in field_schema.get('fields', []):
                k = field['name']
                v = value.get(k, None)
                subfield_schema = next((field for field in field_schema['fields'] if field['name'] == k), None)
                subfields.append(f"{get_subfield_value(v, k, subfield_schema)} AS `{k}`")
            if subfields:
                return f"STRUCT({', '.join(subfields)})"
            else:
                return 'NULL'
        elif isinstance(value, str):
            if value == '[None]':
                return 'NULL'
            if is_int(value) and key not in ['message', 'originalMessage', 'code', 'city', 'region', 'country', 'address', 'address2', 'originalCode', 'postalCode']:
                return int(value)
            if is_float(value) and key not in ['message', 'originalMessage', 'code', 'city', 'region', 'country', 'address', 'address2', 'originalCode', 'postalCode']:
                return float(value)
            value = str(value).replace("'", " ")
            return f"'{value}'"
        elif isinstance(value, list):
            if not value or value == 'None':
                return 'NULL'
            else:
                if key == '_links' and all(isinstance(v, dict) for v in value):
                    first_link = sql_repr(value[0], key=key, field_schema=field_schema)
                    return f"({first_link})"
                else:
                    converted_list = [sql_repr(v, key=key, field_schema=field_schema) for v in value]
                    return f"({', '.join(converted_list)})"
        else:
            return str(value)

    keys = ', '.join([f"`{k}`" for k in items[0].keys() if k not in ('transactions', 'taxes')])
    sql_string = f"INSERT INTO {DATASET_NAME}.{TABLE_NAME}({keys}) VALUES "
    for item in items:
        values = ', '.join([str(sql_repr(item.get(k, None), key=k, field_schema=next((field for field in schema if field['name'] == k), None))) for k in items[0].keys() if k not in ('transactions', 'taxes')])
        sql_string += f"({values}),"
    sql_string = re.sub(",$", ";", sql_string)
    return sql_string

def insert_data(data):
    bq_hook = bigquery.Client()
    job_config = QueryJobConfig(use_legacy_sql=False)

    chunk_size = 1000
    while chunk_size > 0:
        success = True
        for chunk in chunks(data, chunk_size):
            sql_string = transform(chunk, schema)
            if len(sql_string) <= 1020000:
                try:
                    query_job = bq_hook.query(sql_string, job_config=job_config)
                    query_job.result()
                except BadRequest as e:
                    print(e, '\n', sql_string)
                    success = False
                    chunk_size = chunk_size // 2
                    break
            else:
                success = False
                chunk_size = chunk_size // 2
                break
        if success:
            break

    
with DAG(
    dag_id="Rebilly.invoices",
    start_date=datetime.now() - timedelta(days=730),
    schedule="@daily",
    catchup=True,
    max_active_tasks=10,
    max_active_runs=1,
    concurrency=2,
) as dag:
    get_data_task = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        op_args=["{{ ts_nodash }}"],
        do_xcom_push=True,
    )
    # create_table_task = BigQueryCreateEmptyTableOperator(
    #     task_id="create_invoices_table",
    #     dataset_id=DATASET_NAME,
    #     table_id="invoices",
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
