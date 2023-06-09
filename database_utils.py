import yaml
from sqlalchemy.engine import create_engine
from sqlalchemy import inspect
import pandas as pd
import pymysql

'''
read_db_creds()
To read credentials from YAML file 
return dict

init_db_engine()
To Initialize database engine 
return sqlalchemy.engine.base.Engine

list_db_tables
To list tables in the database
'''




class DatabaseConnector:

    def read_db_creds(self, file):
        with open(file, 'r') as file:
            credential = yaml.safe_load(file)
        return credential
            
    def init_db_engine(self):
        credential = self.read_db_creds("db_creds.yaml")
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'  
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{credential['RDS_USER']}:{credential['RDS_PASSWORD']}@{credential['RDS_HOST']}:{credential['RDS_PORT']}/{credential['RDS_DATABASE']}")
        engine.connect()
        return engine
    
    def list_db_tables(self):
        credential = self.read_db_creds("db_creds.yaml")
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        credential = self.read_db_creds("my_db_creds.yaml")
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'  
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{credential['RDS_USER']}:{credential['RDS_PASSWORD']}@{credential['RDS_HOST']}:{credential['RDS_PORT']}/{credential['RDS_DATABASE']}")
        engine.connect()
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        return engine


