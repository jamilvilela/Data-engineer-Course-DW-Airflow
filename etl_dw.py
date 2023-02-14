import csv
from datetime import datetime, timedelta
import airflow
import airflow.utils as airflow_utils
from airflow.decorators import dag, task
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

@dag(
      default_args = default_args,
      schedule_interval = '0 0/2 * * *',
      dagrun_timeout = timedelta(minutes=60),
      description = 'Loading into DW',
      start_date = airflow.utils.dates.days_ago(1)
)
def dag_dw_load():
      
      def quotify(line):
      ### This function will place quotes before and after the values if it is a string type

            for field in line:

                  if mapping['params']['0']['fields'][field] == 'string':
                        line[field] = '\"%s\"' % (line[field])
            
            return line

      @task()    
      def read_csv(map: dict):
      ### This function read the CSV file for loading into DW 
            data_map = map['params']
            path     = data_map['csv_file_path']
            file     = data_map['0']['csv_file_name']
            data = list()
            
            with open(path + file, 'r') as file:
                  reader = csv.DictReader(file)
                  # file data
                  for line in reader:
                        line = quotify(dict(line))
                        data.append(line)
            return data

      @task()
      def create_sql_cmd(data: list, map: dict):
            """
            this function receives the all csv file content in a list 
            and generates one sql command for each line of the file
            """                  
            data_map    = map['params']
            table       = data_map['0']['table_name']
            fields_name = data_map['0']['fields'].keys()

            sql_cmd = ''
            for line in data:
                  sql_cmd = sql_cmd + 'insert into lab6.%s (%s) values (%s); \n' % (table, 
                                                                                    ','.join(fields_name), 
                                                                                    ','.join([value for value in line.values()]))
            return sql_cmd

      @task()
      def load_to_postgres(sql_cmd: str):
            load = PostgresOperator(task_id = 'load_data_postgres',
                                        sql = sql_cmd,
                                        postgres_conn_id = 'dw-postgresDB',
                                        dag = dag_dw_load)

            return load.execute()

      # upstram
      data_file = read_csv(mapping)
      sql_cmd   = create_sql_cmd(data_file, mapping)
      load_to_postgres(sql_cmd)

dag_dw_load.cli()