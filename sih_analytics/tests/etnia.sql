SELECT
    "ETNIA"
FROM
    {{ source('public', 'etnia') }}
WHERE
    "ETNIA" <= 0 OR "ETNIA" > 270