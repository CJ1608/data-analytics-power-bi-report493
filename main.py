from db_utils import RDSDatabaseConnector
from data_extraction import DataExtractor
import pandas as pd

if __name__ == '__main__':
    
    #  Get credentials and connect to database
     db_connector = RDSDatabaseConnector('credentials.yaml')
     db_connector.read_db_creds()
     db_connector.init_db_engine()

     # print list of the tables in orders table
     table_names = db_connector.list_db_tables()
    #  print(table_names)
     
    # print all of tables to csv for later use
     db_extractor = DataExtractor()
    #  for element in table_names:
    #     df_extracted= db_extractor.read_rds_table(db_connector, element)
    #     df_extracted.to_csv(f'{element}.csv')
        
    ##write query on csv file using sql language
        ##save results of query into another csv file
     df = pd.read_sql("select * from  orders", con=db_connector.init_db_engine())
     print(df)
     