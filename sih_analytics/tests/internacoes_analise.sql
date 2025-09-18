SELECT
    *
FROM {{ source('public', 'internacoes') }}
WHERE "DT_SAIDA" < "DT_INTER"