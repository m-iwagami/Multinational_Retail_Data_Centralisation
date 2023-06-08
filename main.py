from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


db_con = DatabaseConnector()
db_ex = DataExtractor()
db_cleaning = DataCleaning()

credential = 'my_db_creds.yaml'
engine = db_con.init_db_engine()

print(db_con.list_db_tables())

#cleaned_db = db_cleaning.clean_user_data(user_df)
#cleaned_db.to_string('cleaned_users.txt')

#upload_df = db_con.upload_to_db(user_df,'dim_users') to upload df to adminpg


# Clean and Upload card details from PDF file
#pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
#df = db_ex.retrieve_pdf_data(pdf_link)
#clean_card_df = db_cleaning.clean_card_data(df)
##clean_card_df.to_string('credit_card.txt')
#upload_df = db_con.upload_to_db(clean_card_df,'dim_card_details')


#store_df = db_ex.read_rds_table('legacy_store_details',db_con)
#store_df.to_string('store_details.txt')
#number_of_stores = db_ex.list_number_of_stores()
#retrive_stores_data = db_ex.retrieve_stores_data(430)
#print(f"Number of Stores: {retrive_stores_data}")

#clean_store_df = db_cleaning.called_clean_store_data(store_df)
#clean_store_df.to_string('clean_store.txt')
#upload_df = db_con.upload_to_db(clean_store_df,'dim_store_details')

#s3_address = 's3://data-handling-public/products.csv'
#df = db_ex.extract_from_s3(s3_address)
#df.to_string('products.txt')

#clean_product_df = db_cleaning.clean_products_data(df)
#clean_product_df = clean_product_df.drop(columns=['Unnamed: 0'])
#clean_product_df.to_string('clean_products.txt')
#upload_df = db_con.upload_to_db(clean_product_df,'dim_products')

user_df = db_ex.read_rds_table('orders_table',db_con)
user_df.to_string('orders.csv') #To see the data in a file
#clean_order_df = db_cleaning.clean_orders_data(user_df)

#clean_order_df = user_df.drop(columns=['1','first_name','last_name'])
#clean_order_df.to_string('clean_order_data.txt')
#upload_df = db_con.upload_to_db(clean_order_df,'orders_table')

s3_address = 's3://data-handling-public/products.csv'
df = db_ex.extract_from_s3(s3_address)
df.to_string('products.txt')

store_data = db_ex.retrieve_json_data()
clean_store_data = db_cleaning.clean_data_details(store_data)
#clean_store_data.to_string('data.csv')
upload_df = db_con.upload_to_db(clean_store_data,'dim_date_times')


