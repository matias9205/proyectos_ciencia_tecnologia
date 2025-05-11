--IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'data_practise')
--BEGIN
--    CREATE DATABASE data_practise;
--END;

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
IF OBJECT_ID('[proyectos_ciencia_tecnologia].[proyecto_beneficiario]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[proyecto_beneficiario] (
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

--Se crea la tabla "estado_proyecto" validando si existe
IF OBJECT_ID('[proyectos_ciencia_tecnologia].[ref_disciplina]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[ref_disciplina] (
		disciplina_id INT,
        gran_area_codigo INT NULL,
        gran_area_descripcion TEXT NULL,
        area_codigo INT NULL,
        area_descripcion TEXT NULL,
        disciplina_codigo VARCHAR(8) NULL,
        disciplina_descripcion TEXT NULL
	)
END

--Se crea la tabla "disciplina" validando si existe
IF OBJECT_ID('[proyectos_ciencia_tecnologia].[ref_estado_proyecto]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[ref_estado_proyecto] (
		id INT PRIMARY KEY,
        descripcion TEXT,
	)
END

--Se crea la tabla "moneda" validando si existe
IF OBJECT_ID('[proyectos_ciencia_tecnologia].[ref_funcion]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[ref_funcion] (
		funcion_id INT PRIMARY KEY,
        funcion_desc TEXT,
	)
END

--Se crea la tabla "tipo_proyecto" validando si existe
IF OBJECT_ID('[proyectos_ciencia_tecnologia].[ref_moneda]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[ref_moneda] (
		moneda_id INT PRIMARY KEY,
        moneda_desc TEXT,
        codigo_iso VARCHAR(MAX) NULL
	)
END

--Se crea la tabla "proyectos_anios" validando si existe
IF OBJECT_ID('[proyectos_ciencia_tecnologia].[ref_tipo_proyecto]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[ref_tipo_proyecto] (
		id INT PRIMARY KEY,
        sigla VARCHAR(MAX),
        descripcion TEXT,
        tipo_proyecto_cyt_id INT,
        tipo_proyecto_cyt_desc TEXT
	)
END

IF OBJECT_ID('[proyectos_ciencia_tecnologia].[proyecto_participante]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[proyecto_participante] (
		proyecto_id INT,
        persona_id INT,
        funcion_id INT,
		CONSTRAINT Fk_funcion FOREIGN KEY (funcion_id)
			REFERENCES [proyectos_ciencia_tecnologia].[ref_funcion] (funcion_id),
        fecha_inicio DATETIME NULL,
        fecha_fin DATETIME NULL
	)
END

IF OBJECT_ID('[proyectos_ciencia_tecnologia].[proyecto_disciplina]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[proyecto_disciplina] (
		proyecto_id INT,
        disciplina_id INT,
		CONSTRAINT Fk_disciplina FOREIGN KEY (disciplina_id)
			REFERENCES [proyectos_ciencia_tecnologia].[ref_disciplina] (disciplina_id),
	)
END

IF OBJECT_ID('[proyectos_ciencia_tecnologia].[proyectos_anios]', 'U') IS NULL
BEGIN
	CREATE TABLE [proyectos_ciencia_tecnologia].[proyectos_anios] (
		proyecto_id INT PRIMARY KEY,
		proyecto_fuente VARCHAR(MAX),
		titulo VARCHAR(MAX),
		fecha_inicio DATE,
		fecha_finalizacion DATE,
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

CREATE OR ALTER VIEW [proyectos_ciencia_tecnologia].[proyectos_disciplina] AS
SELECT
	pd.proyecto_id,
	rd.gran_area_codigo,
	rd.gran_area_descripcion,
	rd.area_codigo,
	rd.area_descripcion,
	rd.disciplina_codigo,
	rd.disciplina_descripcion
FROM 
    [proyectos_ciencia_tecnologia].[proyecto_disciplina] pd
LEFT JOIN 
    [proyectos_ciencia_tecnologia].[ref_disciplina] rd
ON 
    pd.disciplina_id = rd.disciplina_id;

CREATE OR ALTER VIEW [proyectos_ciencia_tecnologia].[proyectos_participantes] AS
SELECT
	pp.persona_id,
	rf.funcion_desc,
	pp.fecha_inicio,
	pp.fecha_fin
FROM 
    [proyectos_ciencia_tecnologia].[proyecto_participante] pp
LEFT JOIN 
    [proyectos_ciencia_tecnologia].[ref_funcion] rf
ON 
    pp.funcion_id = rf.funcion_id;

CREATE OR ALTER VIEW [proyectos_ciencia_tecnologia].[proyectos_por_anio] AS
SELECT
	pa.proyecto_fuente,
	pa.titulo,
	pa.fecha_inicio,
	pa.fecha_finalizacion,
	pa.anio,
	pa.resumen,
	rm.codigo_iso,
	rm.moneda_desc,
	pa.monto_total_solicitado,
	pa.monto_total_adjudicado,
	pa.monto_financiado_solicitado,
	pa.monto_financiado_adjudicado,
	rtp.sigla,
	rtp.descripcion AS tipo_proyecto_desc,
	rtp.tipo_proyecto_cyt_id,
	rtp.tipo_proyecto_cyt_desc,
	pa.codigo_identificacion,
	pa.palabras_clave,
	rep.descripcion AS estado_proyecto_desc,
	pa.fondo_anpcyt,
	pa.cantidad_miembros_F,
	pa.cantidad_miembros_M,
	pa.sexo_director
FROM 
    [proyectos_ciencia_tecnologia].[proyectos_anios] pa
LEFT JOIN 
    [proyectos_ciencia_tecnologia].[ref_moneda] rm
ON 
    pa.moneda_id = rm.moneda_id
LEFT JOIN 
    [proyectos_ciencia_tecnologia].[ref_tipo_proyecto] rtp
ON 
    pa.tipo_proyecto_id = rtp.id
LEFT JOIN 
    [proyectos_ciencia_tecnologia].[ref_estado_proyecto] rep
ON 
    pa.estado_id = rep.id;

--SELECT COLUMN_NAME, DATA_TYPE
--FROM INFORMATION_SCHEMA.COLUMNS
--WHERE TABLE_NAME = 'ref_disciplina' AND COLUMN_NAME = 'disciplina_id';

--SELECT COLUMN_NAME, DATA_TYPE
--FROM INFORMATION_SCHEMA.COLUMNS
--WHERE TABLE_NAME = 'proyecto_disciplina' AND COLUMN_NAME = 'disciplina_id';


--DELETE FROM [proyectos_ciencia_tecnologia].[proyecto_beneficiario];
--DELETE FROM [proyectos_ciencia_tecnologia].[proyecto_disciplina];
--DELETE FROM [proyectos_ciencia_tecnologia].[proyecto_participante]
--DELETE FROM [proyectos_ciencia_tecnologia].[proyectos_anios];
--DELETE FROM [proyectos_ciencia_tecnologia].[ref_disciplina]
--DELETE FROM [proyectos_ciencia_tecnologia].[ref_estado_proyecto]
--DELETE FROM [proyectos_ciencia_tecnologia].[ref_funcion]
--DELETE FROM [proyectos_ciencia_tecnologia].[ref_moneda]
--DELETE FROM [proyectos_ciencia_tecnologia].[ref_tipo_proyecto]

--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[proyecto_beneficiario]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[proyecto_beneficiario];
--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[proyecto_disciplina]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[proyecto_disciplina];
--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[proyecto_participante]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[proyecto_participante];
--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[proyectos_anios]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[proyectos_anios];
--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[ref_disciplina]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[ref_disciplina];
--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[ref_estado_proyecto]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[ref_estado_proyecto];
--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[ref_funcion]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[ref_funcion];
--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[ref_moneda]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[ref_moneda];
--IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[proyectos_ciencia_tecnologia].[ref_tipo_proyecto]') AND type = 'U')
--DROP TABLE [proyectos_ciencia_tecnologia].[ref_tipo_proyecto];