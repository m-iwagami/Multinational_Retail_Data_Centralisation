from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


db_con = DatabaseConnector()
db_ex = DataExtractor()
db_cleaning = DataCleaning()

#print(db_con.list_db_tables())
#user_df = db_ex.read_rds_table('legacy_users',db_con)
#print(user_df)

#cleaned_df = db_cleaning.clean_user_data(user_df, db_con)
#print(cleaned_df)

upload_df = db_con.upload_to_df('legacy_users','test_table')
print(upload_df)