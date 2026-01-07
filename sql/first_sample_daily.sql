SELECT *
//"Momento", "Descripcion",  "Valor", "Unidades"

FROM (
    SELECT 
        *,
        //"Momento", "Descripcion",  "Valor", "Unidades",
        ROW_NUMBER() OVER (
            PARTITION BY "Descripcion", DATE_TRUNC('DAY', "Momento")
            ORDER BY "Momento" ASC
        ) AS rn
    FROM "I+D+I"."Histórico señales CI"
    WHERE "Fuente" = 'PPL'
    AND "Tag" LIKE '45PPL.HTU.%'
    //AND "Unidades" = 'ºC'
    //AND "Descripcion" like '%TEMPERATURA%'
    AND "Año" = 2025
    //AND "Mes" <= 4
    //AND "Mes" > 1 
) t
WHERE rn = 1
ORDER BY "Momento" DESC
