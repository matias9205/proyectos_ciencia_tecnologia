import io
import json
import os
import random
import time
import pandas as pd
from datetime import datetime
import requests
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.by import By
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv

from db import create_sql_connection

load_dotenv()

SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
SQL_SERVER_PASS = os.getenv("SQL_SERVER_PASS")
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST")
SQL_SERVER_DB = os.getenv("SQL_SERVER_DB")

DB_URL = f"mssql+pyodbc://{SQL_SERVER_USER}:{SQL_SERVER_PASS}@{SQL_SERVER_HOST}/{SQL_SERVER_DB}?driver=ODBC+Driver+17+for+SQL+Server"

options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("user-agent=Mozilla/5.0")
driver = webdriver.Chrome(options=options)

BASE_URL = "https://datos.gob.ar/dataset/mincyt-proyectos-ciencia-tecnologia-e-innovacion"

session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,  # Espera incremental entre reintentos
    status_forcelist=[500, 502, 503, 504],
    raise_on_status=False
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)

def get_json_links(url):
  driver.get(url)
  time.sleep(random.uniform(2, 5))
  divs = driver.find_elements(By.CLASS_NAME, "pkg-file-img")
  links = []
  for div in divs:
    if div.get_attribute("data-format") == "json":
      container = div.find_element(By.XPATH, "./ancestor::div[contains(@class, 'pkg-container')]")
      if container:
        try:
          download_btn = container.find_element(By.LINK_TEXT, "DESCARGAR")
          file_name = container.find_element(By.TAG_NAME, "h3")
          links.append(
            {
              "file_name": file_name.text,
              "type": "json",
              "link": download_btn.get_attribute("href")
            }
          )
        except:
          pass
    elif div.get_attribute("data-format") == "csv":
      container = div.find_element(By.XPATH, "./ancestor::div[contains(@class, 'pkg-container')]")
      if container:
        try:
          download_btn = container.find_element(By.LINK_TEXT, "DESCARGAR")
          file_name = container.find_element(By.TAG_NAME, "h3")
          links.append(
            {
              "file_name": file_name.text,
              "type": "csv",
              "link": download_btn.get_attribute("href")
            }
          )
        except:
          pass
  driver.quit()
  return links

def download_csv_resilient(url, session_):
    try:
        response = session_.get(url, stream=True, timeout=20)
        response.raise_for_status()
        return pd.read_csv(io.BytesIO(response.content), sep=";", engine="python")
    except Exception as e:
        print(f"❌ Error al procesar CSV desde {url}: {e}")
        return None

def parse_json(links_list, link):
  try:
    if link['type'] == 'json':
      headers = {
              "User-Agent": "Mozilla/5.0",
              "Accept-Encoding": "identity"
          }
      r = session.get(link['link'], headers=headers, timeout=15)
      r.raise_for_status()
      data = r.json()
      print(f"---------------------------------------------JSON PARSED DATA of {link['file_name']}------------------------------------------------------")
      print(pd.json_normalize(data['data']).head())
  except requests.exceptions.RequestException as e:
    print(f"Error al descargar {link['link']}: {e}")
    csv_link = next((l for l in links_list if l['type'] == 'csv' and l['file_name'] == link['file_name']), None)
    if csv_link:
      print(f"➡️ Intentando fallback con CSV: {csv_link['link']}")
      try:
          df = download_csv_resilient(csv_link['link'], session)
          print(f"--- CSV LOADED DATA of {csv_link['file_name']} ---")
          print(df.head())
      except Exception as ce:
          print(f"❌ Error al procesar CSV desde {csv_link['link']}: {ce}")
    else:
        print("❗ No se encontró link alternativo CSV.")
  except ValueError as ve:
    print(f"Respuesta no es JSON válido desde {link['link']}: {ve}")

if __name__ == "__main__":
  json_links = get_json_links(BASE_URL)
  print(json_links)
  for elem in json_links:
    parse_json(json_links, elem)