import csv
import airflow
import time
import pandas as pd
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates impport days_ago

# airflow arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

data_source = '/opt/airflow/dags/data/'

# DAG
dag_lab6_etl = DAG(dag_id = 'lab6-dw-postgres',
                   default_args = default_args,
                   schedule_interval = '0 0/2 * * *',
                   dagrun_timeout = timedelta(minutes=60),
                   description = 'Loading into DW',
                   start_date = airflow.utils.dates.days_ago(1)
                   )

def CSV_file_extraction():
    values = []
    
    with open(data_source + 'DIM_CLIENTE.csv', 'r') as file:
        reader = csv.reader(file)
        
        for row in reader:
            values.append(tuple(row))
        
    return values

# Python operator
read_csv_task = PythonOperator(task_id = 'read_csv_task',
                               python_callable = CSV_file_extraction,
                               dag = dag_lab6_etl)
    
# Postgres Operator
insert_data_postgres = PostgresOperator(task_id = 'ins_data_postgres',
                                        sql = 'insert into lab6.DIM_CLIENTE (id_cliente, nome_cliente, sobrenome_cliente) values (%s,%s,%s)',
                                        params = read_csv_task.python_callable(),
                                        postgres_conn_id = 'dw-postgresDB',
                                        dag = dag_lab6_etl)

# upstram
read_csv_task >> insert_data_postgres

if __name__ == "__main__":
    dag_lab6_etl.cli()