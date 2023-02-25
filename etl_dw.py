import csv
import json
import collections 
import logging
import os
import shutil
import pytz
import airflow
import airflow.utils as airflow_utils
from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

#logging config
br_tz = pytz.timezone('America/Sao_Paulo')
dt_now = datetime.now(br_tz)
year = dt_now.strftime('%Y')
month = dt_now.strftime('%m')
day = dt_now.strftime('%d')

project_path = '/opt/airflow/dags/logistics'
map_path     = f'{project_path}/data-map/map.json'
data_path    = f'{project_path}/data/'
log_path     = f'{project_path}/log/{year}-{month}-{day}-log.txt'
error_path   = f'{project_path}/error/{year}-{month}-{day}/'
process_path = f'{project_path}/processed/{year}-{month}-{day}/'

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

      def raiseOnError(error_message: str):
            """
            This function will be executed after error handling through the flow
            """
            logging.error(error_message)
            raise ValueError(error_message)
      
      def quotify(line: dict, data_map: dict):
            """
            This function will place quotes before and after the values if it is a string type
            """

            for field, value in line.items():

                  if data_map['fields'][field] in ['string', 'datetime']:
                        line[field] = f"'{value}'"
            
            return line
        
      @task()
      def open_map_file(path: str):
            try:
                  
                  with open(path, 'r') as conf:
                        map = json.loads(conf.read())
                        logging.debug(f'Open file: {path}')
                        
            except FileNotFoundError as e:
                  raiseOnError(f'File not found. \nMSG: {e}')
            except IOError as e:
                  raiseOnError(f'Open error: {path}. \nMSG: {e}')
                  
            return map

      @task()    
      def read_csv(data_map: dict):
            """
            This function read the CSV file for loading into DW.
            If the column names are different from data mapping config, this function will return an empty list
            """
            
            data = []

            if data_map == '':
                  raiseOnError('Data map file is invalid.')
            else:
                  file_name   = data_map['csv_file_name']
                  fields_name = data_map['fields'].keys()
                  
                  try:
                        with open(f'{data_path}{file_name}', 'r') as file:
                              reader = csv.DictReader(file)
                              logging.info(f'Open file: {file_name}')

                              if collections.Counter(reader.fieldnames) != collections.Counter(fields_name):   
                                    raiseOnError('The file columns are different from the data mapping. \nIngestion can not continue.')

                              for line in reader:
                                    line = quotify(dict(line), data_map)
                                    data.append(line)
                              
                              lines_qty = len(data)
                              logging.info(f'Total lines read: {lines_qty}')
                  
                              # if file exists but no data
                              if lines_qty <= 0:
                                    raiseOnError(f'File {file_name} is empty.')
                                    
                  except FileNotFoundError as e:
                        raiseOnError(f'File not found. \nMSG: {e}') 
                  except IOError as e:
                        raiseOnError(f'Open error: {file_name}. \nMSG: {e}') 

            return data

      @task()
      def create_sql_cmd(data: list, data_map: dict) -> str:
            """
            this function receives the all csv file content in a list 
            and generates one sql command for each line of the file
            """                  

            table       = data_map['table_name']
            fields_name = data_map['fields'].keys()
            fields_name = ','.join(fields_name)
            unique_key  = data_map['unique_key']
            sql_cmd     = ''
            
            if len(data) == 0:
                  raiseOnError(f'There is no data to insert into {table}.')
            
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
                                    
            logging.info( f'Total commands: {len(data)}' )

            return sql_cmd

      @task()
      def load_to_postgres(sql_cmd: str):
            """
            This function loads the data into Postgres using the SQL command.
            """
            logging.debug( sql_cmd )
            
            if sql_cmd == '':
                  raiseOnError(f'There is no SQL command to insert.')

            try:
                load = PostgresOperator(task_id = 'load_to_postgres',
                                        sql = sql_cmd,
                                        postgres_conn_id = 'dw-postgresDB',
                                        dag = dag_dw_load)
                load.execute()
            except Exception as e:
                raiseOnError(f'Insert/update execution failed. \nMSG: {e}')

            return True

      @task
      def move_file(file: str, destiny: str):
            '''
            This function moves the file to the detiny directory
            '''
        
            dt = datetime.now(br_tz)
            yy = dt.strftime('%Y')
            mm = dt.strftime('%m')
            dd = dt.strftime('%d')
            hh = dt.strftime('%H')
            mi = dt.strftime('%M')
            ss = dt.strftime('%S')
        
            name, extension = file.split(sep='.')
            
            new_file = f'{name}-{yy}{mm}{dd}-{hh}{mi}{ss}.{extension}'
            
            try:
                  
                  if not os.path.exists(destiny):
                        os.makedirs(destiny)
                  
                  shutil.copyfile(f'{data_path}{file}', f'{destiny}{new_file}')
                  #shutil.move(f'{data_path}{file}', f'{destiny}{new_file}')
                  logging.info(f'File {file} moved to {new_file}.')
                  
            except Exception as e:
                  raiseOnError(f'Copy file error. \nMSG: {e}')

      @task
      def send_email(dest: str):
            EmailOperator(task_id = 'send_email',
                          to =  dest,
                          subject = 'Logistics - DW load' 
                          html_content = 'The data load on PostgreSQL has been successfully completed.',
                          )

      # upstram
      # loop reading all source files      

      logging.info('--------------------- Starting the DAG process ---------------------')
            
      run_this_first = EmptyOperator(task_id="run_this_first")
      
      map_file = open_map_file(map_path)
      
      run_this_first >> map_file

      for file_number in range(8):

            load_done = False
            sql_cmd = ''
            data = []
            data_map = {}
            
            data_map = map_file['params'][str(file_number)]
            data     = read_csv(data_map)
            sql_cmd  = create_sql_cmd(data, data_map)
            load_done= load_to_postgres(sql_cmd)

            destiny = process_path 
            if not load_done: #  there is an error
                  destiny = error_path   
            
            move_file(data_map['csv_file_name'], destiny)
            
            if load_done:
                  send_email('jamilvilela@gmail.com')

            read_csv >> create_sql_cmd >> load_to_postgres >> move_file >> send_email
      
      logging.info('----------------------- DAG process finished -----------------------')

dag_dw_load_cli = dag_dw_load()