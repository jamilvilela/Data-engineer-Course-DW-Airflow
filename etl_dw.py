import csv
import json
import collections
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

with open('/opt/airflow/dags/data-map/map.config', 'r') as conf:
      mapping = json.loads(conf.read())

@dag(
      default_args = default_args,
      schedule_interval = '0 0/2 * * *',
      dagrun_timeout = timedelta(minutes=60),
      description = 'Loading into DW',
      start_date = days_ago(1)
)
def dag_dw_load():
      
      def quotify(line, map_number):
            """
            This function will place quotes before and after the values if it is a string type
            """

            for field, value in line.items():

                  if mapping['params'][str(map_number)]['fields'][field] in ['string', 'datetime']:
                        line[field] = f'"{value}"'
            
            return line
      
      @task()    
      def read_csv(map_number: int):
            """
            This function read the CSV file for loading into DW.
            If the column names are different from data mapping config, this function will return an empty list
            """
             
            data_map = mapping['params']
            path     = data_map['csv_file_path']
            file     = data_map[str(map_number)]['csv_file_name']
            fields_name = data_map[str(map_number)]['fields'].keys()
            data = []
            
            with open(f'{path}{file}', 'r') as file:
                  reader = csv.DictReader(file)
                                    
                  if collections.Counter(reader.fieldnames) != collections.Counter(fields_name):   
                        print('The file columns are different from data mapping. \nIngestion process aborted.')
                        return []

                  for line in reader:
                        line = quotify(dict(line), map_number)
                        data.append(line)
            
            return data

      @task()
      def create_sql_cmd(data: list, map_number: int):
            """
            this function receives the all csv file content in a list 
            and generates one sql command for each line of the file
            """                  
            data_map    = mapping['params']
            table       = data_map[str(map_number)]['table_name']
            fields_name = data_map[str(map_number)]['fields'].keys()
            unique_key  = data_map[str(map_number)]['unique_key']
            sql_cmd     = ''
            update_fields=''
                        
            for line in data:
                  
                  insert_values = [value for value in line.values()]                  
                  update_fields = [ f'{key}={value}' for key,value in line.items() if key != unique_key ]

                  sql_cmd +=  '''
                              insert into lab6.%s (%s) values (%s) 
                              on conflict(%s) 
                              do update set (%s); \n
                              ''' % (table, 
                                     ','.join(fields_name), 
                                     ','.join(insert_values),
                                     unique_key,
                                     ','.join(update_fields)
                                    )
            return sql_cmd

      @task()
      def load_to_postgres(sql_cmd: str):
            """
            This function loads the data into Postgres using the SQL command.
            """
            load = PostgresOperator(task_id = 'load_data_postgres',
                                        sql = sql_cmd,
                                        postgres_conn_id = 'dw-postgresDB',
                                        dag = dag_dw_load)

            return load.execute()

      # upstram
      # loop reading all source files
      for file_number in range(8):
            data_file = read_csv(file_number)
            sql_cmd   = create_sql_cmd(data_file, file_number)
            load_to_postgres(sql_cmd)

dag_dw_load_cli = dag_dw_load()