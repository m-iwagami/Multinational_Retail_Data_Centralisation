from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


db_con = DatabaseConnector()
db_ex = DataExtractor()
db_cleaning = DataCleaning()

engine = db_con.init_db_engine()

#print(db_con.list_db_tables())
user_df = db_ex.read_rds_table('legacy_users',db_con)
#user_df.to_string('users.csv') #To see the data in a file

cleaned_db = db_cleaning.clean_user_data(user_df)

cleaned_db.to_string('cleaned_users.txt')
# print(db_con.upload_to_df('legacy_users','test_table'))
