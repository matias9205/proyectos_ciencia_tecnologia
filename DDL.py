from dotenv import load_dotenv
import pyodbc
import os

from db import create_sql_connection

load_dotenv()

db_user = os.getenv('SQL_SERVER_USER')
db_pass = os.getenv('SQL_SERVER_PASS')
db_host = os.getenv('SQL_SERVER_HOST')
db_name = os.getenv('SQL_SERVER_DB')

def execute_sql_code(cursor_, query):
    print(query)
    try:
        cursor_.execute(query)
        print(f"Query was executed succesfully")
    except pyodbc.Error as e:
        print(f"Error creating database: {e}")

connection = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_host};UID={db_user};PWD={db_pass};DATABASE={db_name}'
)
cursor = connection.cursor()
connection.autocommit = True

execute_sql_code(cursor, f"USE {db_name}")

print("---------------------------------------------------CREATE SCHEMA--------------------------------------------------------")
schema = 'proyectos_ciencia_tecnologia'
create_schema = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = '{schema}'
)
BEGIN
    EXEC('CREATE SCHEMA [{schema}]');
END;
"""
execute_sql_code(cursor, create_schema)
print("------------------------------------------------------------------------------------------------------------------------")

print("---------------------------------------------------CREATE TABLES--------------------------------------------------------")
print("----------------------------CREATE PROYECTO_BENEFICIARIO TABLE---------------------------------------------")
proyecto_beneficiario_table = 'proyecto_beneficiario'
create_proyecto_beneficiario_table = f"""
IF OBJECT_ID('{schema}.{proyecto_beneficiario_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{proyecto_beneficiario_table}] (
		proyecto_id INT,
		organizacion_id INT NULL,
        persona_id INT NULL,
        financiadora CHAR,
        ejecutora CHAR,
        evaluadora CHAR,
        adoptante CHAR,
        beneficiaria CHAR,
        adquiriente CHAR,
        porcentaje_financiamiento INT NULL
	)
END
"""
execute_sql_code(cursor, create_proyecto_beneficiario_table)
print("-----------------------------------------------------------------------------------------------------")

print("----------------------------CREATE DISCIPLINA TABLE---------------------------------------------")
disciplina_table = 'ref_disciplina'
create_disciplina_table = f"""
IF OBJECT_ID('{schema}.{disciplina_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{disciplina_table}] (
		disciplina_id INT PRIMARY KEY,
        gran_area_codigo INT NULL,
        gran_area_descripcion TEXT NULL,
        area_codigo DECIMAL(6,2) NULL,
        area_descripcion TEXT NULL,
        disciplina_codigo VARCHAR(MAX) NULL,
        disciplina_descripcion TEXT NULL
	)
END
"""
execute_sql_code(cursor, create_disciplina_table)
print("-----------------------------------------------------------------------------------------------------")

print("----------------------------CREATE ESTADO_PROYECTO TABLE---------------------------------------------")
estado_proyecto_table = 'ref_estado_proyecto'
create_estado_proyecto_table = f"""
IF OBJECT_ID('{schema}.{estado_proyecto_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{estado_proyecto_table}] (
		id INT PRIMARY KEY,
        descripcion TEXT,
	)
END
"""
execute_sql_code(cursor, create_estado_proyecto_table)
print("-----------------------------------------------------------------------------------------------------")

print("----------------------------CREATE FUNCION TABLE---------------------------------------------")
funcion_table = 'ref_funcion'
create_funcion_table = f"""
IF OBJECT_ID('{schema}.{funcion_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{funcion_table}] (
		funcion_id INT PRIMARY KEY,
        funcion_desc TEXT,
	)
END
"""
execute_sql_code(cursor, create_funcion_table)
print("-----------------------------------------------------------------------------------------------------")

print("----------------------------CREATE MONEDA TABLE---------------------------------------------")
moneda_table = 'ref_moneda'
create_moneda_table = f"""
IF OBJECT_ID('{schema}.{moneda_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{moneda_table}] (
		moneda_id INT PRIMARY KEY,
        moneda_desc TEXT,
        codigo_iso VARCHAR(MAX) NULL
	)
END
"""
execute_sql_code(cursor, create_moneda_table)
print("-----------------------------------------------------------------------------------------------------")

print("----------------------------CREATE TIPO_PROYECTO TABLE---------------------------------------------")
tipo_proyecto_table = 'ref_tipo_proyecto'
create_tipo_proyecto_table = f"""
IF OBJECT_ID('{schema}.{tipo_proyecto_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{tipo_proyecto_table}] (
		id INT PRIMARY KEY,
        sigla VARCHAR(MAX),
        descripcion TEXT,
        tipo_proyecto_cyt_id INT,
        tipo_proyecto_cyt_desc TEXT
	)
END
"""
execute_sql_code(cursor, create_tipo_proyecto_table)
print("-----------------------------------------------------------------------------------------------------")

print("----------------------------CREATE PROYECTO_PARTICIPANTE TABLE---------------------------------------------")
proyecto_participante_table = 'proyecto_participante'
create_proyecto_participante_table = f"""
IF OBJECT_ID('{schema}.{proyecto_participante_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{proyecto_participante_table}] (
		proyecto_id INT,
        persona_id INT,
        funcion_id INT,
		CONSTRAINT Fk_funcion FOREIGN KEY (funcion_id)
			REFERENCES [proyectos_ciencia_tecnologia].[ref_funcion] (funcion_id),
        fecha_inicio DATETIME NULL,
        fecha_fin DATETIME NULL
	)
END
"""
execute_sql_code(cursor, create_proyecto_participante_table)
print("-----------------------------------------------------------------------------------------------------")

print("----------------------------CREATE PROYECTO_DISCIPLINA TABLE---------------------------------------------")
proyecto_disciplina_table = 'proyecto_disciplina'
create_proyecto_disciplina_table = f"""
IF OBJECT_ID('{schema}.{proyecto_disciplina_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{proyecto_disciplina_table}] (
		proyecto_id INT PRIMARY KEY,
        disciplina_id INT,
		CONSTRAINT Fk_disciplina FOREIGN KEY (disciplina_id)
			REFERENCES [proyectos_ciencia_tecnologia].[ref_disciplina] (disciplina_id),
	)
END
"""
execute_sql_code(cursor, create_proyecto_disciplina_table)
print("-----------------------------------------------------------------------------------------------------")

print("----------------------------CREATE PROYECTOS_ANIOS TABLE---------------------------------------------")
proyectos_anios_table = 'proyectos_anios'
create_proyectos_anios_table = f"""
IF OBJECT_ID('{schema}.{proyectos_anios_table}', 'U') IS NULL
BEGIN
	CREATE TABLE [{schema}].[{proyectos_anios_table}] (
		proyecto_id INT PRIMARY KEY,
		proyecto_fuente VARCHAR(MAX),
		titulo VARCHAR(MAX),
		fecha_inicio DATETIME,
		fecha_finalizacion DATETIME,
		resumen VARCHAR(MAX),
		moneda_id INT,
		CONSTRAINT Fk_moneda FOREIGN KEY (moneda_id)
			REFERENCES [proyectos_ciencia_tecnologia].[ref_moneda] (moneda_id),
		monto_total_solicitado DECIMAL(18,2),
		monto_total_adjudicado DECIMAL(18,2),
		monto_financiado_solicitado DECIMAL(18,2),
		monto_financiado_adjudicado DECIMAL(18,2),
		tipo_proyecto_id INT,
		CONSTRAINT Fk_tipo_proyecto FOREIGN KEY (tipo_proyecto_id)
			REFERENCES [proyectos_ciencia_tecnologia].[ref_tipo_proyecto] (id),
		codigo_identificacion VARCHAR(MAX),
		palabras_clave TEXT NULL,
		estado_id INT,
		CONSTRAINT Fk_estado_proyecto FOREIGN KEY (estado_id)
			REFERENCES [proyectos_ciencia_tecnologia].[ref_estado_proyecto] (id),
		fondo_anpcyt VARCHAR(MAX),
		cantidad_miembros_F INT NULL,
		cantidad_miembros_M INT NULL,
		sexo_director CHAR NULL
	)
END
"""
execute_sql_code(cursor, create_proyectos_anios_table)
print("-----------------------------------------------------------------------------------------------------")