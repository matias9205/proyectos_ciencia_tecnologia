use data_practise

------------------------------------------------------------------------------------------------------------------------
-- Paso 1: Actualizar los valores nulos en las tablas de referencia antes de agregar las claves foráneas

-- Actualizar la tabla proyecto_disciplina
UPDATE pd
SET pd.disciplina_id = NULL
FROM proyectos_ciencia_tecnologia.proyecto_disciplina pd
LEFT JOIN proyectos_ciencia_tecnologia.ref_disciplina rd
ON pd.disciplina_id = rd.disciplina_id
WHERE rd.disciplina_id IS NULL;

-- Actualizar la tabla proyecto_participante
UPDATE pp
SET pp.funcion_id = NULL
FROM proyectos_ciencia_tecnologia.proyecto_participante pp
LEFT JOIN proyectos_ciencia_tecnologia.ref_funcion rf
ON pp.funcion_id = rf.funcion_id
WHERE rf.funcion_id IS NULL;

------------------------------------------------------------------------------------------------------------------------
-- Paso 2: Modificar las columnas de las tablas de referencia para que no permitan valores nulos

-- Modificar las columnas en las tablas de referencia
ALTER TABLE proyectos_ciencia_tecnologia.ref_moneda
ALTER COLUMN moneda_id INT NOT NULL;

ALTER TABLE proyectos_ciencia_tecnologia.ref_tipo_proyecto
ALTER COLUMN id INT NOT NULL;

ALTER TABLE proyectos_ciencia_tecnologia.ref_estado_proyecto
ALTER COLUMN id INT NOT NULL;

ALTER TABLE proyectos_ciencia_tecnologia.ref_disciplina
ALTER COLUMN disciplina_id INT NOT NULL;

ALTER TABLE proyectos_ciencia_tecnologia.ref_funcion
ALTER COLUMN funcion_id INT NOT NULL;

------------------------------------------------------------------------------------------------------------------------
-- Paso 3: Modificar las columnas en la tabla proyectos_anios, proyecto_disciplina, proyecto_participante
-- para que las columnas coincidan con las restricciones de claves foráneas

ALTER TABLE proyectos_ciencia_tecnologia.proyectos_anios
ALTER COLUMN moneda_id INT;

ALTER TABLE proyectos_ciencia_tecnologia.proyectos_anios
ALTER COLUMN tipo_proyecto_id INT;

ALTER TABLE proyectos_ciencia_tecnologia.proyectos_anios
ALTER COLUMN estado_id INT;

ALTER TABLE proyectos_ciencia_tecnologia.proyecto_disciplina
ALTER COLUMN disciplina_id INT;

ALTER TABLE proyectos_ciencia_tecnologia.proyecto_participante
ALTER COLUMN funcion_id INT;

------------------------------------------------------------------------------------------------------------------------
-- Paso 4: Agregar las claves primarias en las tablas de referencia

ALTER TABLE proyectos_ciencia_tecnologia.ref_moneda
ADD CONSTRAINT PK_moneda_id PRIMARY KEY (moneda_id);

ALTER TABLE proyectos_ciencia_tecnologia.ref_tipo_proyecto
ADD CONSTRAINT PK_id PRIMARY KEY (id);

ALTER TABLE proyectos_ciencia_tecnologia.ref_estado_proyecto
ADD CONSTRAINT PK_estado_id PRIMARY KEY (id);

ALTER TABLE proyectos_ciencia_tecnologia.ref_disciplina
ADD CONSTRAINT PK_disciplina_id PRIMARY KEY (disciplina_id);

ALTER TABLE proyectos_ciencia_tecnologia.ref_funcion
ADD CONSTRAINT PK_funcion_id PRIMARY KEY (funcion_id);

------------------------------------------------------------------------------------------------------------------------
-- Paso 5: Agregar las restricciones de claves foráneas

-- Agregar la primera restricción Fk_moneda
ALTER TABLE proyectos_ciencia_tecnologia.proyectos_anios
ADD CONSTRAINT Fk_moneda
FOREIGN KEY (moneda_id)
REFERENCES proyectos_ciencia_tecnologia.ref_moneda (moneda_id);

-- Agregar la segunda restricción Fk_tipo_proyecto
ALTER TABLE proyectos_ciencia_tecnologia.proyectos_anios
ADD CONSTRAINT Fk_tipo_proyecto
FOREIGN KEY (tipo_proyecto_id)
REFERENCES proyectos_ciencia_tecnologia.ref_tipo_proyecto (id);

-- Agregar la tercera restricción Fk_estado_proyecto
ALTER TABLE proyectos_ciencia_tecnologia.proyectos_anios
ADD CONSTRAINT Fk_estado_proyecto
FOREIGN KEY (estado_id)
REFERENCES proyectos_ciencia_tecnologia.ref_estado_proyecto (id);

-- Agregar la cuarta restricción Fk_discip
ALTER TABLE proyectos_ciencia_tecnologia.proyecto_disciplina
ADD CONSTRAINT Fk_discip
FOREIGN KEY (disciplina_id)
REFERENCES proyectos_ciencia_tecnologia.ref_disciplina (disciplina_id);

-- Agregar la quinta restricción Fk_funcion
ALTER TABLE proyectos_ciencia_tecnologia.proyecto_participante
ADD CONSTRAINT Fk_funcion
FOREIGN KEY (funcion_id)
REFERENCES proyectos_ciencia_tecnologia.ref_funcion (funcion_id);