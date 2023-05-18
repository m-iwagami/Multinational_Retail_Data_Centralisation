import yaml
from sqlalchemy.engine import create_engine
from sqlalchemy import inspect
import pandas as pd

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
            credentials = yaml.safe_load(file)
        return credentials
            
    def init_db_engine(self):
        credentials = self.read_db_creds("db_creds.yaml")
      
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'  
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}")
        engine.connect()
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_df(self, df, table_name):
        engine = self.init_db_engine()
        uploaded_df = df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(uploaded_df)


