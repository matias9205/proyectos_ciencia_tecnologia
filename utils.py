import pandas as pd
from sqlalchemy import create_engine, exc

class Utils:
    def __init__(self, db_user, db_password, db_host, db_name):
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_name = db_name
        self.db_url = f"mssql+pyodbc://{self.db_user}:{self.db_password}@{self.db_host}/{self.db_name}?driver=ODBC+Driver+17+for+SQL+Server"

    def transform_dates(self, df_: pd.DataFrame, col_:str):
        df_[col_] = pd.to_datetime(df_[col_])
        return df_
    
    def db_connection(self):
        try:
            engine = create_engine(self.db_url)
            with engine.connect():
                return engine
        except exc.SQLAlchemyError as e:
            print(f"❌ Error al conectar con la base de datos: {e}")
            raise