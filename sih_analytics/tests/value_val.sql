SELECT *
FROM {{ source('public', 'internacoes') }}
WHERE "VAL_SH" < 0 OR "VAL_SP" < 0 OR "VAL_TOT" < 0
