USE data_practise

--Se crea el schema "proyectos_ciencia_tecnologia" validando si existe
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'proyectos_ciencia_tecnologia'
)
BEGIN
    EXEC('CREATE SCHEMA [proyectos_ciencia_tecnologia]');
END;

------------------------------------SE CREAN LAS TABLAS-----------------------------------------------------------
--Se crea la tabla "funcion" validando si existe
IF OBJECT_ID('proyectos_ciencia_tecnologia.funcion', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[funcion] (
		funcion_id INT PRIMARY KEY,
		funcion_desc VARCHAR(250)
	)
END

--Se crea la tabla "estado_proyecto" validando si existe
IF OBJECT_ID('proyectos_ciencia_tecnologia.estado_proyecto', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[estado_proyecto] (
		id INT PRIMARY KEY,
		descripcion VARCHAR(100)
	)
END

--Se crea la tabla "disciplina" validando si existe
IF OBJECT_ID('proyectos_ciencia_tecnologia.disciplina', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[disciplina] (
		disciplina_id INT PRIMARY KEY,
		gran_area_codigo INT,
		gran_area_descripcion VARCHAR(100),
		area_codigo INT,
		area_descripcion VARCHAR(250),
		disciplina_codigo VARCHAR(10),
		disciplina_descripcion VARCHAR(250)
	)
END

--Se crea la tabla "moneda" validando si existe
IF OBJECT_ID('proyectos_ciencia_tecnologia.moneda', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[moneda] (
		moneda_id INT PRIMARY KEY,
		moneda_desc VARCHAR(50) NULL,
		codigo_iso VARCHAR(50) NULL,
	)
END

--Se crea la tabla "tipo_proyecto" validando si existe
IF OBJECT_ID('proyectos_ciencia_tecnologia.tipo_proyecto', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[tipo_proyecto] (
		id INT PRIMARY KEY,
		sigla VARCHAR(50),
	)
END