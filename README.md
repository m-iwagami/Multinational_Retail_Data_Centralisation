# Multinational_Retail_Data_Centralisation
    - A project from AiCore course
## Project Introduction
This project is from a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, making its sales data accessible from one centralised location helps their work. Producing a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. Querying the database to get up-to-date metrics for the business is the goal of the project. 

## Project Outline
1. Extract all the data from the multitude of data sources.
2. Clean the data and store them in the database
3. Develop the star-based schema of the database, ensuring that the colums are of the correct data types.
4. Querying the data
5. Answering business questions and extracting the data from the database using SQL.


## Requirements
- pip install requirements.txt
- python3 main.py

## Modules

#### database_utils.py: 
This is DatabaseConnector class that contains four mothods for connecting with and upload data to the database.

1. read_db_creds: To read the credentials yaml file and return a dictionary of the credentials
2. init_db_engine: To read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
3. list_db_tables: To list all the tables in the database
4. upload_to_db: This method will take in a Pandas DataFrame and table name to upload to as an argument


#### data_extraction.py:
This file is a DataExtractor Class that contains methods that help extract data from different data sources such as CSV files, an API and an S3 bucket
1. read_rds_table: extracts the database table to a pandas DataFrame
2. retrieve_pdf_data: takes in a link as an argument and returns a pandas DataFrame
3. list_number_of_stores: returns the number of stores to extract
4. retrieve_stores_data: takes the retrieve a store endpoint as an argument and extracts all the stores from the API saving them in a pandas DataFrame.
5. extract_from_s3: uses the boto3 package to download and extract the information returning a pandas DataFrame.
6. retrieve_json_data: extract data from json file stored on S3


#### data_cleaning.py: 
This is a DataCleaning Class that performs that cleaning of the data 
1. clean_user_data: 
2. called_clean_store_data: 
3. clean_products_data: 
4. clean_card_data:
5. clean_card_number:
6. clean_data_details:
7. clean_letters:  
8. clean_country_code: 
9. fixing_date:
10. clean_phone_number:
11. clean_email:
13. clean_address:
14. clean_uuid:
15. clean_code:
16. clean_numbers:
17. clean_continent:
18. clean_latitude:
19. clean_longitude:
20. clean_price:
21. clean_ean_number:
22. convert_product_weights:
23. clean_orders_data


#### main.py:
This file executes the code from the three remaining files. It calls DataExtract and DataConnect classes to extract the original data, DataClean class to clean it and DataConnect again to upload them to a local postgres dataframe.

#### db_creds.yaml & my_db_creds.yaml:
Containing the database credentials. These files are in .gitignore, so they are not in my public GitHub repository.




## Libraries
- import pandas as pd
- import yaml
- import pymysql
- import numpy as np
- import io
- import re
- from sqlalchemy.engine import create_engine
- from sqlalchemy import inspect
- import tabula
- import requests
- import boto3
- from geopy.geocoders import Nominatim


## Furthur work
- Clean data with usaddress library or geopy