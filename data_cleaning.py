import pandas as pd
import numpy as np
import re

from geopy.geocoders import Nominatim

from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    '''This is a DataCleaning Class that performs that cleaning of the data '''
    
    def clean_user_data(self, df):
        '''
        To clean users dataframe
        Takes dataframe
        Return dataframe
        '''
        df = df.replace(['NaN', 'NULL','NaT'], np.nan)
        df = df.dropna()
        df = self._clean_letters(df, 'first_name')
        df = self._clean_letters(df, 'last_name')
        df = self._clean_letters(df, 'country')
        df = self._clean_country_code(df, 'country_code')
        df['date_of_birth'] = self._fixing_date(df,'date_of_birth')
        df['join_date'] = self._fixing_date(df,'join_date')
        df = self._clean_uuid(df, 'user_uuid')
        df = self._clean_phone_number(df, 'phone_number','country_code')
        #df = self.clean_email(df, 'email_address')
        #df = self.clean_address(df, 'address')
        return df

    
    def called_clean_store_data(self,df):
        '''
        To clean store dataframe
        Takes dataframe
        Return dataframe
        '''

        df = df[df['store_code'] != 'NULL']
        df = self._clean_country_code(df, 'country_code')
        df['opening_date'] = self._fixing_date(df,'opening_date')
        
        df = self._clean_letters(df, 'store_type')
        df = self._clean_letters(df, 'locality')
        df = self._clean_continent(df, 'continent')
        df = self._clean_latitude(df, 'latitude')
        df = self._clean_longitude(df, 'longitude')

        df = self.clean_numbers(df, 'staff_numbers')
        df = pd.DataFrame(df)
        #df = self.clean_code(df, 'store_code')
        #df = self.clean_address(df, 'address')
        return df


    
    def clean_products_data(self, df):
        '''
        To clean product dataframe
        Takes dataframe
        Return dataframe
        '''
        df = df.replace(['NaN', 'NULL','NaT'], np.nan)
        df = df.dropna()
    
        df = self._clean_price(df,'product_price')
        df['date_added'] = self.fixing_date(df, 'date_added')
        df = self._clean_ean_number(df, 'EAN')
        df = self._convert_product_weights(df, 'weight')
#
        return df
    
    def clean_card_data(self,df):
        '''
        To clean card dataframe
        Takes dataframe
        Return dataframe
        '''
        df = df[df['card_number'] != 'NULL']
        df = self._clean_card_number(df, 'card_number')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce')
        return df

    def clean_data_details(self, df):
        '''
        To clean store data
        Takes dataframe
        Return dataframe
        '''
        df = df.replace(['Nan','NULL','N/A'], np.nan)
        df = df.dropna()
        df = self._clean_numbers(df,'month')
        return df
    
    def _clean_card_number(self,df,column):
        '''
        To clean card numbers
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = df[column].astype(str)
        df[column] = df[column].str.replace('?','')
        card_pattern = r'^[0-9]{9,20}'
        return df[df[column].str.match(card_pattern, na=False)]
    
    

    def _clean_letters(self, df, column):
        '''
        To clean letters
        Takes dataframe and column
        Return dataframe
        '''
        df = df.replace(['Nan','NULL','N/A'], np.nan)
        df[column] = df[column].astype(str)
        return df[df[column].str.contains(r'^[a-zA-ZÄÖÜäöüßé\s\.\-\']*$', na=False)]
        

    def _clean_country_code(self, df, column):
        '''
        To clean country code
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = df[column].str.replace('GGB','GB')
        country_code = r'\b[A-Z]{2}\b'
        return df[df[column].str.match(country_code, na=False)]

    def _fixing_date(self, df, column):
        '''
        To clean a column with date 
        Takes dataframe and column
        Return dataframe
        '''
        return pd.to_datetime(df[column], format='%Y-%m-%d', errors='coerce')
        

    def _clean_phone_number(self, df, phone_column, country_code_column):
        '''
        To clean phone numbers
        Takes dataframe and column
        Return dataframe
        '''
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


    def _clean_email(self, df, column):
        '''
        To clean emails
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = df[column].astype(str)
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        return df[df[column].str.match(email_pattern, na=False)]
 
    def _clean_address(self, df, column):
        '''
        To clean address
        Takes dataframe and column
        Return dataframe
        '''
        cleaned_df = df.copy()
        geolocator = Nominatim(user_agent="address_cleaner")
        for index, address in enumerate(cleaned_df[column]):
            if pd.notna(address):
                location = geolocator.geocode(address, addressdetails=True, language="en")
                if location and 'address' in location.raw:
                    address_parts = location.raw['address']
                    cleaned_address = ', '.join(filter(lambda x: x.strip(), [address_parts.get('road', ''), address_parts.get('house_number', ''), address_parts.get('city', ''), address_parts.get('state', ''), address_parts.get('postcode', ''), address_parts.get('country', '')]))
                    cleaned_df.at[index, column] = cleaned_address
        return cleaned_df
        return df[df[column].apply(lambda x: pd.notna(x) and re.match(r'^[a-zA-Z0-9\s\.\,\\\ä\ö\ü\ß]+$', x) is not None)]


    def _clean_uuid(self, df, column):
        '''
        To clean uuid
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = df[column].astype(str)
        id_pattern = r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}'
        return df[df[column].str.match(id_pattern, na=False)]
    
    
    def _clean_code(self, df, column):
        '''
        To clean store code
        Takes dataframe and column
        Return dataframe
        '''
        id_pattern = r'^[a-zA-Z]{2,3}-[a-zA-Z0-9]{6,8}'
        df[column] = np.where(df[df[column].str.match(id_pattern)], df[column], np.nan)
        return df
    
    def _clean_numbers(self, df, column):
        '''
        To clean data without numbers
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = pd.to_numeric(df[column], errors='coerce')
        return df
    

    def _clean_continent(self, df, column):
        '''
        To clean continent column
        Takes dataframe and column
        Return dataframe
        '''
        continents = ['Asia', 'Africa', 'America', 'South America', 'Europe','Antarctica', 'Australia']
        df[column] = df[column].str.replace('eeEurope','Europe')
        df[column] = df[column].str.replace('eeAmerica','America')
        return df[df[column].isin(continents) | df[column].isnull()]
    
    def _clean_latitude(self, df, column):
        '''
        To clean latitude
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = np.where((df[column] >= -90), df[column], np.nan )
        return df
    
    def _clean_longitude(self, df, column):
        '''
        To clean longtitude
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = pd.to_numeric(df[column], errors='coerce')
        return df[(df[column] >= -180) & (df[column] <= 180) | pd.isna(df[column])]
    
    def _clean_price(seld, df, column):
        '''
        To clean price data
        Takes dataframe and column
        Return dataframe
        '''
        pattern = r'^£(\d+(\.\d{2}))$'
        df[column] = np.where(df[column].str.match(pattern), df[column], np.nan)
        df = df.dropna(subset='product_price')

        return df

    def _clean_ean_number(self, df, column):
        '''
        To clean ean numbers
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = np.where(len(df[column]) <= 13, df[column], np.nan)
        return df



    def _convert_product_weights(self, df, column):
        '''
        To clean weights and convert measurments to kg
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = df[column].str.replace(r'^\d+\s[x]\s\d+', '0', regex=True)

        kg_rows = df[column].str.match(r'^\d+(\.\d+)?\s*kg$')
        g_rows = df[column].str.match(r'^\d+(\.\d+)?\s*g$')
        ml_rows = df[column].str.endswith('ml')
        
        df[column] = df[column].str.replace('[^\d.]', '', regex=True)
        df[column] = df[column].astype(float, errors='ignore')
#
        df.loc[ml_rows, column] = df.loc[ml_rows, column] / 1000
        df.loc[g_rows, column] = df.loc[g_rows, column] / 1000
        df.loc[kg_rows, column] = df.loc[kg_rows, column]
        return df

    def clean_orders_data(self,df):
        '''
        Return dataframe
        '''
        return df


