import os
import pandas as pd
from datetime import datetime
import requests
from io import StringIO
from sqlalchemy import create_engine
from dotenv import load_dotenv

from db import create_sql_connection

load_dotenv()

BASE_DIR = "C:\\Users\\PC\\Documents\\Matias\\data_projects\\ETL_proyectos_ciencia_tecnologia"
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
SQL_SERVER_PASS = os.getenv("SQL_SERVER_PASS")
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST")
SQL_SERVER_DB = os.getenv("SQL_SERVER_DB")

DB_URL = f"mssql+pyodbc://{SQL_SERVER_USER}:{SQL_SERVER_PASS}@{SQL_SERVER_HOST}/{SQL_SERVER_DB}?driver=ODBC+Driver+17+for+SQL+Server"

files_dir = [file_ for file_ in os.listdir(BASE_DIR+"\\CSV") if 'csv' in file_]
print(files_dir)
proyectos_anios_files = [file_ for file_ in os.listdir(BASE_DIR+"\\CSV\\proyectos_anios\\") if 'csv' in file_]
print(proyectos_anios_files)
schema = 'proyectos_ciencia_tecnologia'

def load_data(db_url, df, schema_, table):
  try:
    engine = create_sql_connection(db_url)
    df.to_sql(table, engine, if_exists='replace', schema=schema_, index=False)
    print(f"TABLE [{schema_}].[{table}] WAS CREATED")
  except Exception as e:
    print(f"Error al cargar los datos en la tabla {table}: str({e})")

def extract_transform_data():
  proyectos_anios = pd.DataFrame()
  for file_ in files_dir:
    file_url = BASE_DIR+"\\CSV\\"+file_
    print({'file': file_, 'file_url': file_url})
    df = pd.read_csv(file_url, sep=";")
    print(df.shape)
    print(df.head(2))
    print(df.info())
    load_data(DB_URL, df, schema, file_.replace(".csv", ""))
  for file_ in proyectos_anios_files:
    file_url = BASE_DIR+"\\CSV\\proyectos_anios\\"+file_
    df = pd.read_csv(file_url, sep=";")
    anio = file_.split("_")
    print({'file': file_, 'file_url': file_url, 'año': anio[1].replace(".csv", "")})
    df['anio'] = anio[1].replace(".csv", "")
    print(df.shape)
    print(df.head(2))
    proyectos_anios = pd.concat([proyectos_anios, df], ignore_index=True)
  print(proyectos_anios.info())
  load_data(DB_URL, proyectos_anios, schema, "proyectos_anios")

if __name__ == '__main__':
  print("-----------------------EMPEZO A CORRER----------------------")
  extract_transform_data()