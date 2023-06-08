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

    
    def called_clean_store_data(self,df):


        df = df[df['address'] != 'NULL']
        df = self.clean_country_code(df, 'country_code')
        df['opening_date'] = self.fixing_date(df,'opening_date')
        df_alph_columns = ['store_type', 'locality']
        df = self.clean_letters(df, df_alph_columns)
        df = self.clean_address(df, 'address')
        df = self.clean_store_code(df, 'store_code')
        df = self.clean_numbers(df, 'staff_numbers')
        df = self.clean_latitude(df, 'latitude')
        df = self.clean_longitude(df, 'longitude')
        df = self.clean_continent(df, 'continent')
        df = pd.DataFrame(df)
        return df

    
    def clean_products_data(self, df):
        df = df.replace(['NaN', 'NULL'], np.nan)
        df = df.dropna()
        df = self.clean_price(df,'product_price')
        df['date_added'] = self.fixing_date(df, 'date_added')
        df = self.clean_ean_number(df, 'EAN')
        df = self.convert_product_weights(df, 'weight')

        return df
    
    def clean_data_details(self, df):
        df = df.replace(['Nan','NULL'], np.nan)
        df = df.dropna()
        df = self.clean_numbers(df,'month')
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

    def clean_card_data(self,df):
        df = df[df['card_number'] != 'NULL']
        df = df[df['card_number'].str.len().between(16,19)]
        #df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%d', errors='coerce')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce')
        return df
    
    def clean_store_code(self, df, columns):
        id_pattern = r'^[a-zA-Z]{2}-[a-zA-Z0-9]{8}'
        return df[df[columns].str.match(id_pattern, na=False)]
    
    def clean_numbers(self, df, column):
        return df[df[column].str.isdigit()]

    def clean_continent(self, df, column):
        continents = ['Asia', 'Africa', 'America', 'South America', 'Europe','Antarctica', 'Australia']
        return df[df[column].isin(continents)]
    
    def clean_latitude(self, df, column):
        df[column] = pd.to_numeric(df[column], errors='coerce')
        return df[df[column].apply(lambda x: -90 <= x <= 90)]

    def clean_longitude(self, df, column):
        df[column] = pd.to_numeric(df[column], errors='coerce')
        return df[df[column].apply(lambda x: -180 <= x <= 180)]
    
        
    def clean_price(seld, df, column):
        pattern = r'^£(\d+(\.\d{2}))$'
        clean_price = df[column].str.match(pattern, na=False)
        return df[clean_price]

    def clean_ean_number(self, df, column):
        df = df[df[column].str.len() ==13]
        return df



    def convert_product_weights(self, df, column):
        kg_rows = df[column].str.match(r'^\d+(\.\d+)?\s*kg$')
        g_rows = df[column].str.match(r'^\d+(\.\d+)?\s*g$')
        ml_rows = df[column].str.endswith('ml')
        
        df[column] = df[column].str.replace('[^\d.]', '', regex=True)
        df[column] = df[column].astype(float)

        df.loc[ml_rows, column] = df.loc[ml_rows, column] / 1000
        df.loc[g_rows, column] = df.loc[g_rows, column] / 1000
        df.loc[kg_rows, column] = df.loc[kg_rows, column]
        return df

    def clean_orders_data(self,df):
        return df


