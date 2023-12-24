import pandas as pd


class DataExtractor():
    """
    Method to extract data from database and return it as a pd.dataframe. 
    """
    
    def read_rds_table(self, db_connector, table_name): 
        """
        Connects to database of data to be cleaned and returns information as a pandas dataframe. 

        Args:
            db_connector (database connection): database connection from init_db_engine method. 
            table_name (literal): name of table in database to be accessed. 
        Returns:
            data_frame (pd.dataframe): dataframe of data to be cleaned. 
        """
        db_connected = db_connector.engine.connect()
        data_frame = pd.read_sql_table(table_name, db_connected)
        db_connected.close()
        return data_frame