import yaml
from sqlalchemy.engine import create_engine
from sqlalchemy import inspect
import pandas as pd
import pymysql




class DatabaseConnector:
    '''This is DatabaseConnector class that contains four mothods for connecting with and upload data to the database.'''

    def read_db_creds(self, file):
        '''To read the credentials yaml file and return a dictionary of the credentials'''
        with open(file, 'r') as file:
            credential = yaml.safe_load(file)
        return credential
            
    def init_db_engine(self):
        '''To read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine'''
        credential = self.read_db_creds("db_creds.yaml")
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'  
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{credential['RDS_USER']}:{credential['RDS_PASSWORD']}@{credential['RDS_HOST']}:{credential['RDS_PORT']}/{credential['RDS_DATABASE']}")
        engine.connect()
        return engine
    
    def list_db_tables(self):
        '''To list all the tables in the database'''
        credential = self.read_db_creds("db_creds.yaml")
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        '''This method will take in a Pandas DataFrame and table name to upload to as an argument'''
        credential = self.read_db_creds("my_db_creds.yaml")
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'  
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{credential['RDS_USER']}:{credential['RDS_PASSWORD']}@{credential['RDS_HOST']}:{credential['RDS_PORT']}/{credential['RDS_DATABASE']}")
        engine.connect()
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        return engine


