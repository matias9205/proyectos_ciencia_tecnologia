-- Active: 1729102093806@@127.0.0.1@3306@mysql_database
SELECT anio, SUM(monto_total_adjudicado) AS "monto_total_año" FROM proyectos_ciencia_tecnologia.detalle_proyectos
GROUP BY anio
ORDER BY anio;

SELECT COUNT(*) 
FROM (SELECT DISTINCT proyecto_id, proyecto_fuente, titulo, fecha_inicio, fecha_finalizacion, resumen, 
             moneda_nombre, moneda_simbolo, monto_total_solicitado, monto_total_adjudicado, 
             monto_financiado_solicitado, monto_financiado_adjudicado, tipo_proyecto_sigla, 
             tipo_proyecto_descripcion, estado_proyecto_descripcion, codigo_identificacion, 
             palabras_clave, fondo_anpcyt, cantidad_miembros_F, cantidad_miembros_M, 
             sexo_director, anio
      FROM proyectos_ciencia_tecnologia.detalle_proyectos) AS unique_projects;

SELECT DISTINCT(estado_proyecto_descripcion) FROM proyectos_ciencia_tecnologia.detalle_proyectos

select * from proyectos_ciencia_tecnologia.proyectos where estado_id = 2

UPDATE proyectos_ciencia_tecnologia.proyectos
SET duracion = CASE
    WHEN estado_id = 1 THEN DATEDIFF(fecha_finalizacion, fecha_inicio)  -- si 'Finalizado' es 1
    WHEN estado_id = 2 THEN 0                                           -- si 'En ejecucion' es 2
    ELSE NULL
END;