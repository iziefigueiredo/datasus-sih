-- tests/datas_invalidas.sql
SELECT
    "N_AIH",
    "DT_INTER",
    "DT_SAIDA"
FROM
    {{ source('public', 'internacoes') }}
WHERE
    "DT_SAIDA" < "DT_INTER"
