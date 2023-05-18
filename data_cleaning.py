import pandas as pd
import numpy as np
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:

    def clean_user_data(self, df,database_connector):
        engine = database_connector.init_db_engine()
        #look for NUll value
        df = df.fillna("NA", inplace = True)
        #error with dates
        df = pd.to_datetime(df, errors='raise')
        # incorrectly typed values and rows filled with the wrong information.
        df = pd.to_numeric(df, errors='coerce')
        df = df.dropna()
        cleaned_df = df 
        return cleaned_df
