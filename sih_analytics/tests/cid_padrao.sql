-- tests/test_cid_padrao.sql
SELECT
    "DIAG_PRINC"
FROM
    {{ source('public', 'internacoes') }}
WHERE
    "DIAG_PRINC" IS NOT NULL
    AND "DIAG_PRINC" !~ '^[A-Z][0-9]{2,3}(\.[0-9])?$'
