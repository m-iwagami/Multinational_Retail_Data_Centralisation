import pandas as pd
import numpy as np
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import re

class DataCleaning:
    
    def clean_user_data(self, df):
        df = df[df['date_of_birth'] != 'NULL']
        
        df_alph_columns = ['first_name', 'last_name', 'country']
        df = self.clean_letters(df, df_alph_columns)
       
        df = self.clean_country_code(df, 'country_code')
       
        df = self.clean_email(df, 'email_address')
       
        df = self.clean_uuid(df, 'user_uuid')
       
        df = self.clean_address(df, 'address')
       
        df['date_of_birth'] = self.fixing_date(df,'date_of_birth')
        df['join_date'] = self.fixing_date(df,'join_date')
       
        df = self.clean_phone_number(df, 'phone_number','country_code')
       
        return df

    
    def clean_letters(self,df,column):
        return df[df[column].apply(lambda x:x.str.match(r'^[a-zA-Z\s]+$')).all(axis=1)] 
    
    def clean_country_code(self, df, column):
        country_code = r'\b[A-Z]{2}\b'
        return df[df[column].str.match(country_code, na=False)]

    def fixing_date(self, df, column):
        return pd.to_datetime(df[column], format='%Y-%m-%d', errors='coerce')
        

    def clean_phone_number(self, df, phone_column, country_code_column):
        isd_code_map = {"GB": "+44", "DE": "+49", "US": "+1"}

        df[phone_column] = df[phone_column].astype(str)
        df[country_code_column] = df[country_code_column].astype(str)

        for index, row in df.iterrows():
            phone_number = row[phone_column]
            country_code = row[country_code_column]

            phone_number = re.sub(r'[\sx+()-.]','', phone_number)[:12]
        
            if len(phone_number) >= 10:
                if country_code in isd_code_map:
                    isd_code = isd_code_map[country_code]
                    if not phone_number.startswith(isd_code):
                        phone_number = isd_code + ' ' + phone_number
            else:
                phone_number = 'N/A'

            df.at[index, phone_column] = phone_number

        return df


    def clean_email(self, df, columns):
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        return df[df[columns].str.match(regex, na=False)]

    def clean_address(self, df, column):
        return df[df[column].apply(lambda x: re.match(r'^[a-zA-Z0-9\s\.\,\\\ä\ö\ü\ß]+$', x) is not None)]

    def clean_uuid(self, df, columns):
        id_pattern = r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}'
        return df[df[columns].str.match(id_pattern, na=False)]
