-- tests/diag_nao_mapeado.sql
SELECT
    i."DIAG_PRINC"
FROM
    {{ source('public', 'internacoes') }} AS i
LEFT JOIN
    {{ source('public', 'cid10') }} AS c
ON
    i."DIAG_PRINC" = c."CID"
WHERE
    c."CID" IS NULL
