USE data_practise

-- Tomamos una muestra de 10
SELECT TOP (10) * 
FROM [proyectos_ciencia_tecnologia].[proyectos_por_anio]
ORDER BY NEWID()

-- Cuántos proyectos se iniciaron por año y cuántos finalizaron por año?
SELECT * FROM (
	SELECT
		YEAR([fecha_inicio]) AS anio,
		COUNT(*) AS proyectos_iniciados, 
		'INICIADOS' AS status
	FROM
		[proyectos_ciencia_tecnologia].[proyectos_por_anio]
	WHERE
		[fecha_inicio] IS NOT NULL
	GROUP BY
		YEAR([fecha_inicio])
	UNION ALL
	SELECT
		YEAR([fecha_fin]) AS anio,
		COUNT(*) AS proyectos_finalizados, 
		'FINALIZADOS' AS status
	FROM
		[proyectos_ciencia_tecnologia].[proyectos_por_anio]
	WHERE
		[fecha_fin] IS NOT NULL
	GROUP BY
		YEAR([fecha_fin])
) ORDER BY anio