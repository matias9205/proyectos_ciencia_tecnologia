import io
import logging
import os
import random
import time
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv

from utils import Utils

load_dotenv()

logging.basicConfig(
    level=logging.INFO,  # Nivel mínimo a mostrar
    format="%(asctime)s - %(levelname)s - %(message)s",  # Formato del log
    datefmt="%Y-%m-%d %H:%M:%S"  # Formato de fecha
)

BASE_URL = "https://datos.gob.ar/dataset/mincyt-proyectos-ciencia-tecnologia-e-innovacion"
BASE_DIR = "C:\\Users\\PC\\Documents\\Matias\\data_projects\\ETL_proyectos_ciencia_tecnologia\\CSV"
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
SQL_SERVER_PASS = os.getenv("SQL_SERVER_PASS")
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST")
SQL_SERVER_DB = os.getenv("SQL_SERVER_DB")

class Etl:
    def __init__(self, url_, dir_):
        self.url = url_
        self.dir = dir_
        self.files_list = [os.path.join(dir_, file) for file in os.listdir(self.dir)]
        self.utils = Utils(SQL_SERVER_USER, SQL_SERVER_PASS, SQL_SERVER_HOST, SQL_SERVER_DB)
        self.options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome(options=self.options)
        self.options.add_argument("--headless")
        self.options.add_argument("user-agent=Mozilla/5.0")
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.links = []
        self.data = {}
        self.df_proyectos_x_anio = pd.DataFrame()
    
    def get_links(self, url) -> list:
        if len(self.files_list) > 5:
            logging.info(f"CSVS DESCARGADOS MANUALMENTE: {self.files_list}")
            return self.files_list
        else:
            logging.info(url)
            self.driver.get(self.url)
            time.sleep(random.uniform(2, 5))
            divs = self.driver.find_elements(By.CLASS_NAME, "pkg-file-img")
            for div in divs:
                if div.get_attribute("data-format") == "csv":
                    container = div.find_element(By.XPATH, "./ancestor::div[contains(@class, 'pkg-container')]")
                    if container:
                        try:
                            download_btn = container.find_element(By.LINK_TEXT, "DESCARGAR")
                            file_name = container.find_element(By.TAG_NAME, "h3")
                            self.links.append(
                                {
                                "file_name": file_name.text,
                                "type": "csv",
                                "link": download_btn.get_attribute("href")
                                }
                            )
                        except:
                            pass
            self.driver.quit()
            return self.links

    def download_csv_resilient(self, url) -> pd.DataFrame | None:
        try:
            response = self.session.get(url, stream=True, timeout=20)
            response.raise_for_status()
            return pd.read_csv(io.BytesIO(response.content), sep=";", engine="python")
        except Exception as e:
            logging.error(f"❌ Error al procesar CSV desde {url}: {e}")
            return None
    
    def read_data(self, csv_list_: list) -> dict:
        if len(self.files_list) > 5:
            for link in self.files_list:
                logging.info(f"LINK CSV DESCARGARDO: {link}")
                df = pd.read_csv(link, sep=";", engine="python")
                if 'proyectos_20' in link:
                    self.df_proyectos_x_anio = pd.concat([self.df_proyectos_x_anio, df], ignore_index=True)
                    self.data['proyectos_x_anio'] = self.df_proyectos_x_anio
                elif 'proyecto_participante' in link:
                    self.data['proyecto_participante'] = df
                elif "proyecto_disciplina" in link:
                    self.data['proyecto_disciplina'] = df
                elif "proyecto_beneficiario" in link:
                    self.data['proyecto_beneficiario'] = df
                elif "moneda" in link:
                    self.data['ref_moneda'] = df
                elif "tipo_proyecto" in link:
                    self.data['ref_tipo_proyecto'] = df
                elif "estado_proyecto" in link:
                    self.data['ref_estado_proyecto'] = df
                elif "funcion" in link:
                    self.data['ref_funcion'] = df
                else:
                    self.data['ref_disciplina'] = df
        else:
            for csv in csv_list_:
                file = csv['file_name']
                url = csv['link']
                logging.info(f"---------------------------FILE: {file}, URL: {url}---------------------------")
                df = self.download_csv_resilient(url)
                if 'proyectos_20' in file:
                    self.df_proyectos_x_anio = pd.concat([self.df_proyectos_x_anio, df], ignore_index=True)
                    self.data['proyectos_x_anio'] = self.df_proyectos_x_anio
                elif 'proyecto_participante' in file:
                    self.data['proyecto_participante'] = df
                elif "proyecto_disciplina" in file:
                    self.data['proyecto_disciplina'] = df
                elif "proyecto_beneficiario" in file:
                    self.data['proyecto_beneficiario'] = df
                elif "MONEDA" in file:
                    self.data['ref_moneda'] = df
                elif "TIPO_PROYECTO" in file:
                    self.data['ref_tipo_proyecto'] = df
                elif "ESTADO_PROYECTO" in file:
                    self.data['ref_estado_proyecto'] = df
                elif "FUNCION" in file:
                    self.data['ref_funcion'] = df
                else:
                    self.data['ref_disciplina'] = df
        return self.data
    
    def transform_data(self, dict_: dict) -> dict:
        for name, df in dict_.items():
            for col in df.columns:
                if 'fecha' in col.lower():
                    logging.info(f"Transformando columna '{col}' en tabla '{name}'")
                    self.utils.transform_dates(df, col)
            logging.info(f"Estructura de '{name}':\n{df.info()}")
        return dict_
    
    def load_data(self, dict_: dict):
        engine = self.utils.db_connection()
        for name, df in dict_.items():
            table = name
            df.to_sql(table, con=engine, if_exists='replace', index=False)
            print(f"TABLE {table} WAS SAVED SUCCESFULLY")

if __name__ == "__main__":
    etl_process = Etl(BASE_URL, BASE_DIR)
    csv_links = etl_process.get_links(BASE_URL)
    logging.info(csv_links)
    data_projetcs = etl_process.read_data(csv_links)
    transformed_data = etl_process.transform_data(data_projetcs)
    etl_process.load_data(transformed_data)