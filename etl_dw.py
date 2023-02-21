import csv
import json
import collections
import logging
import airflow
import airflow.utils as airflow_utils
from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

#logging config
dt_now = datetime.now()
year = dt_now.strftime('%Y')
month = dt_now.strftime('%m')
day = dt_now.strftime('%d')

project_path = '/opt/airflow/dags/logistics'
data_path= f'{project_path}/data/'
log_path = f'{project_path}/log/{year}-{month}-{day}-log.txt'
map_path = f'{project_path}/data-map/map.json'

logging.basicConfig(level=logging.INFO, 
                    filename=log_path, 
                    format='%(asctime)s | %(levelname)s | %(message)s',
                    filemode='a')

# airflow arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
      default_args = default_args,
      schedule_interval = '0 0/2 * * *',
      dagrun_timeout = timedelta(minutes=60),
      description = 'Loading into DW',
      start_date = days_ago(1)
)
def dag_dw_load():

      def openFile(path: str):
            map = ''
            try:
                  with open(path, 'r') as conf:
                        map = json.loads(conf.read())
                        logging.info(f'Open file: {path}')
                        
            except FileNotFoundError:
                  logging.error(f'File {path} not found.')    

            return map
      
      def quotify(line, data_map):
            """
            This function will place quotes before and after the values if it is a string type
            """

            for field, value in line.items():

                  if data_map['fields'][field] in ['string', 'datetime']:
                        line[field] = f"'{value}'"
            
            return line
      
      @task()    
      def read_csv(data_map: dict):
            """
            This function read the CSV file for loading into DW.
            If the column names are different from data mapping config, this function will return an empty list
            """
             
            file_name   = data_map['csv_file_name']
            fields_name = data_map['fields'].keys()
            data = []
            
            try:
                  with open(f'{data_path}{file_name}', 'r') as file:
                        reader = csv.DictReader(file)
                        logging.info(f'Open file: {file_name}')
            
                        if collections.Counter(reader.fieldnames) != collections.Counter(fields_name):   
                              logging.error('The file columns are different from data mapping. \nIngestion process aborted.')
                              return []

                        for line in reader:
                              line = quotify(dict(line), data_map)
                              data.append(line)
            
            except FileNotFoundError:
                  logging.error(f'File {file_name} not found.')
                  return []
                                    
            logging.info(f'Total lines: {len(data)}')
            return data

      @task()
      def create_sql_cmd(data: list, data_map: dict):
            """
            this function receives the all csv file content in a list 
            and generates one sql command for each line of the file
            """                  

            table       = data_map['table_name']
            fields_name = data_map['fields'].keys()
            fields_name = ','.join(fields_name)
            unique_key  = data_map['unique_key']
            sql_cmd     = ''
            update_fields=''
                        
            for line in data:
                  
                  insert_values = [value for value in line.values()] 
                  insert_values = ','.join(insert_values)                 

                  update_fields = [ f'{key}={value}' for key,value in line.items() if key not in unique_key ]
                  update_fields = ','.join(update_fields)

                  sql_cmd +=  f'''
                              insert into lab6.{table} ({fields_name}) 
                              values ({insert_values}) 
                              on conflict({unique_key}) 
                              do update set {update_fields};
                              ''' 

            logging.debug( f'Total commands: {len(data)}' )

            return sql_cmd

      @task()
      def load_to_postgres(sql_cmd: str):
            """
            This function loads the data into Postgres using the SQL command.
            """
            logging.debug( sql_cmd )
            
            load = PostgresOperator(task_id = 'load_to_postgres',
                                    sql = sql_cmd,
                                    postgres_conn_id = 'dw-postgresDB',
                                    dag = dag_dw_load)

            return load.execute()

      # upstram
      # loop reading all source files      
      file = openFile(map_path)
      file_map = file['params']

      logging.info('--------------------- Starting the DAG process ---------------------')
            
      for file_number in range(8):
            data_map = file_map[str(file_number)]
            data     = read_csv(data_map)
            sql_cmd  = create_sql_cmd(data, data_map)
            load_to_postgres(sql_cmd)

      logging.info('----------------------- DAG process finished -----------------------')


dag_dw_load_cli = dag_dw_load()