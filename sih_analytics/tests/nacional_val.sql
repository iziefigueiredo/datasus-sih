SELECT
    "NACIONAL"
FROM
    {{ source('public', 'internacoes') }}
WHERE
    "NACIONAL" <= 0 OR "NACIONAL" > 350