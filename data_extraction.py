import yaml
import pandas as pd
from database_utils import DatabaseConnector
from sqlalchemy import inspect
import tabula
import requests
import boto3
import io



class DataExtractor:

    

    def read_rds_table(self, table_name, database_connector):
        engine = database_connector.init_db_engine()
        df = pd.read_sql_table(table_name, con=engine, index_col='index')
        return df

    def retrieve_pdf_data(self,url):
        df_list = tabula.read_pdf(url, pages='all', multiple_tables=True)
        df = pd.concat(df_list)
        return df
    
        
    

    def list_number_of_stores(self):
        number_store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}   

        number_store = requests.get(number_store_url, headers=headers)
  
        if number_store.status_code == 200:
            number_of_stores = number_store.json()
            return number_of_stores
        else:
            print(f"Error: {number_store.text}")
            return None
        
    def retrieve_stores_data(self,store_number):
        retrive_a_store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        url = f'{retrive_a_store_url}+{store_number}'
        retrive_store_data = requests.get(url, headers=headers)

        if retrive_store_data.status_code == 200:
            retrive_store_data = retrive_store_data.json()
            return retrive_store_data
        else:
            print(f'Error:{retrive_store_data.text}')
            return None
        

    def extract_from_s3(self, s3_address):
        bucket, key = s3_address.replace('s3://','').split('/',1)
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(data))
        return df
    
    def retrieve_json_data(self):
        retrive_a_data_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        url = retrive_a_data_url
        retrive_json_data = requests.get(url, headers=headers)

        if retrive_json_data.status_code == 200:
            retrive_json_data = retrive_json_data.json()
            retrive_json_data = pd.DataFrame(retrive_json_data)
            return retrive_json_data
        else:
            print(f'Error:{retrive_json_data.text}')
            return None

    

