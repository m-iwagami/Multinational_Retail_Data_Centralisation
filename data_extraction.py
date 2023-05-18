import yaml
import pandas as pd
from database_utils import DatabaseConnector
from sqlalchemy import inspect


class DataExtractor:

    
    def read_rds_table(self, table_name, database_connector):
        engine = database_connector.init_db_engine()
        df = pd.read_sql_table(table_name, con=engine, index_col='index')
        return df

    



def test():
    

    #test = DataExtractor()
    #engine = test.engine()
    ##data_test = test.list_db_tables(engine)
    #df = test.read_rds_table()
    #print(df)


    connect_engine = DatabaseConnector()
    credential = connect_engine.read_db_creds(test)
    engine = connect_engine.init_db_engine(credential) #Connecting engine

    test = DataExtractor()
    data_test = test.list_db_tables(engine)
    df  = test.read_db_table()
    print(df)


if __name__ == '__main__':
    test()