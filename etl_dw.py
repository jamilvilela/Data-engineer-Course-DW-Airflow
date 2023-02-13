import csv
import airflow
import time
import pandas as pd
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

# airflow arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

mapping = {'params': 
            {'csv_file_path': '/opt/airflow/dags/data/',
             '0': {'csv_file_name' : 'DIM_CLIENTE.csv',
                    'table_name'   : 'DIM_CLIENTE',
                    'fields'  : {'id_cliente'       : 'int',
                                 'nome_cliente'     : 'string',
                                 'sobrenome_cliente': 'string'}
                    },
             '1': {'csv_file_name': 'DIM_DATA.csv',
                   'table_name'   : 'DIM_DATA'},
             '2': {'csv_file_name': 'DIM_DEPOSITO.csv',
                   'table_name'   : 'DIM_DEPOSITO'},
             '3': {'csv_file_name': 'DIM_ENTREGA.csv',
                   'table_name'   : 'DIM_ENTREGA'},
             '4': {'csv_file_name': 'DIM_FRETE.csv',
                   'table_name'   : 'DIM_FRETE'},
             '5': {'csv_file_name': 'DIM_PAGAMENTO.csv',
                   'table_name'   : 'DIM_PAGAMENTO'},
             '6': {'csv_file_name': 'DIM_TRANSPORTADORA.csv',
                   'table_name'   : 'DIM_TRANSPORTADORA'},
             '7': {'csv_file_name': 'TB_FATO.csv',
                   'table_name'   : 'TB_FATO'}
            }
        }

# DAG
dag_dw_load = DAG(dag_id = 'dw-load',
                   default_args = default_args,
                   schedule_interval = '0 0/2 * * *',
                   dagrun_timeout = timedelta(minutes=60),
                   description = 'Loading into DW',
                   start_date = airflow.utils.dates.days_ago(1)
                   )

def quotify(line):
### This function will place quotes before and after the values if it is a string type
    
    for field in line:

        if mapping['params']['0']['fields'][field] == 'string':
            line[field] = '\'%s\'' % (line[field])
            
    return line
    
def csv_file_extraction(**kwargs):
### This function read the CSV file for loading into DW 
    data_map = kwargs['params']
    path     = data_map['csv_file_path']
    file     = data_map['0']['csv_file_name']
    table    = data_map['0']['table_name']
    fields_name = data_map['0']['fields'].keys()
    
    with open(path + file, 'r') as file:
        reader = csv.DictReader(file)
        
        # file data
        for item in reader:
            data = dict(item)
        
            line = quotify(line)
        
            query = query + 'insert into lab6.%s (%s) values (%s); \n' % (table, 
                                                                          ','.join(fields_name), 
                                                                          ','.join([value for value in line.values()])) 
        print(query)

# Python operator
read_csv_task = PythonOperator(task_id = 'read_csv_task',
                               python_callable = csv_file_extraction,
                               provide_context = True,
                               op_kwargs = mapping,
                               dag = dag_dw_load)

# Postgres Operator
insert_data_postgres = PostgresOperator(task_id = 'ins_data_postgres',
                                        sql = get_current_context()['query'],
                                        postgres_conn_id = 'dw-postgresDB',
                                        dag = dag_dw_load)

#insert_data_postgres.execute(context=kwargs)


        

    
# upstram
read_csv_task >> insert_data_postgres

if __name__ == "__main__":
    dag_dw_load.cli()