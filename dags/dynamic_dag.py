from datetime import datetime, timedelta
import requests
import json
import csv
import io

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.api_core.exceptions import BadRequest

from utils import chunks

endpoints = ["customers", "disputes", "invoices", "credit-memos", "tags", "subscriptions", "transactions"]
headers = {"REB-APIKEY": "sk_live_LO-uyzhEfsz9NY5jR7dqNb10ko-PpPSAhf1D2dj"}
organization_id = "b5672fbd-8023-4d9b-a31b-1d7eaf9edb8b"

def get_data(point, start_time: str, end_time: str = None) -> list:
    start_dt = datetime.strptime(start_time, "%Y%m%dT%H%M%S")
    if not end_time:
        end_dt = start_dt + timedelta(days=1)
    else:
        end_dt = datetime.strptime(end_time, "%Y%m%dT%H%M%S")
    url = f"https://api.rebilly.com/organizations/{organization_id}/{point}"
    query = {
        "limit": "100",
        "offset": "0",
        "filter": f"createdTime:{start_dt.strftime('%Y-%m-%dT%H:%M:%S')}Z..{end_dt.strftime('%Y-%m-%dT%H:%M:%S')}Z",
        "sort": "createdTime",
    }
    response = requests.get(url, headers=headers, params=query)
    if int(response.headers["Pagination-total"]) <= 1000:
        return json.loads(response.text)
    delta = end_dt - start_dt
    return get_data(point,
        start_dt.strftime("%Y%m%dT%H%M%S"), (start_dt + delta / 2).strftime("%Y%m%dT%H%M%S")
    ) + get_data(point,
        (start_dt + delta / 2).strftime("%Y%m%dT%H%M%S"), end_dt.strftime("%Y%m%dT%H%M%S")
    )

def replace_blank_dict(d):
    if isinstance(d, list):
        for i, list_item in enumerate(d):
            if isinstance(list_item, dict):
                d[i] = replace_blank_dict(list_item)
    elif isinstance(d, dict):
        for k, v in d.items():
            if v and (isinstance(v, dict) or isinstance(v, list)):
                d[k] = replace_blank_dict(v)
            elif not v:
                d[k] = None
    return d

def convert_to_csv(data):
    data = replace_blank_dict(data)
    # Convert any nested dictionaries to strings first
    for row in data:
        for key, value in row.items():
            if isinstance(value, dict):
                # Convert dictionary to string
                row[key] = json.dumps(value)

    # Continue as before
    fieldnames = list(data[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()

def execute_insert_queries(point, **kwargs):
    data = convert_to_csv(kwargs["ti"].xcom_pull(key="return_value", task_ids="get_data"))
    client = bigquery.Client("big-query-database-376613")
    client.create_table(f"big-query-database-376613.Rebilly.auto{point}", exists_ok=True)
    table_ref = client.get_table(f"big-query-database-376613.Rebilly.auto{point}")
    job_config = bigquery.LoadJobConfig(
autodetect=True, source_format=bigquery.SourceFormat.CSV, schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION)
    with open("output.csv", 'rb') as file:
        csvreader = csv.reader(file)
        load_job = client.load_table_from_file(file, table_ref, job_config=job_config)
        load_job.result()
        print(load_job.result().errors)


for point in endpoints:
    with DAG(
        dag_id=f"Rebilly.auto_{point}",
        start_date=datetime.now() - timedelta(days=3),
        schedule="@daily",
        catchup=True,
    ) as dag:
        
        get_data_task = PythonOperator(
            task_id="get_data",
            python_callable=get_data,
            op_args=[point, "{{ ts_nodash }}"],
            do_xcom_push=True)

        insert_data_task = PythonOperator(
        task_id="execute_insert_queries",
        python_callable=execute_insert_queries,
        op_args=[point])

        get_data_task >> insert_data_task

#print(convert_to_csv(data=get_data('customers', '20230512T000000')))
#print(get_data('customers', '20230512T000000').__class__)
# generate_csv_data(data=normalize_json(data=get_data('customers', '20230512T000000')))