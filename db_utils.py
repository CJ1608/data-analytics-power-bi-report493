from sqlalchemy import create_engine, inspect
import yaml

class RDSDatabaseConnector():
    """
    Methods to connect to database. 
    """
    def __init__(self, yaml_file):
            """
            See help(DatabaseConnector) for more detail. 

            Args:
                yaml_file: connection details and passwords to connect to database. 
            """
            self.yaml_file = yaml_file

    def read_db_creds(self):
        """
        Reads and returns sensitive database connection credentials, called in the init_db_engine method. 
        Yaml files added to git ignore file.

        Returns: 
            db_credentials: credentials to connect to a database. 
        """
        with open(self.yaml_file, 'r') as file:
            self.db_credentials = yaml.safe_load(file)
            return self.db_credentials

    def init_db_engine(self):
        """
        Read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine to get data from 
        database. 

        Returns:
            engine (engine): database connection to an Amazon database via a sqlalchemy engine. 
        """
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.db_credentials['HOST']
        USER = self.db_credentials['USER']
        PASSWORD = self.db_credentials['PASSWORD']
        DATABASE = self.db_credentials['DATABASE']
        PORT = self.db_credentials['PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}") 
        return self.engine 
    
    def list_db_tables(self):
        """
        Use engine returned from init_db_engine to list all the tables in the database. 

        Returns:
            table_names (list[str]): list of table names in Amazon database. 
        """
        inspector = inspect(self.engine)
        self.table_names = inspector.get_table_names()
        return self.table_names