-- tests/hospital_cnes_municipio.sql
SELECT
    "CNES"
FROM
    {{ source('public', 'hospital') }}
GROUP BY
    "CNES"
HAVING
    COUNT(DISTINCT "MUNIC_MOV") > 2;
