-- models/staging/stg_contraceptivos.sql
-- T2: regras de negócio aplicadas sobre contraceptivos (pós-carga)
--
-- CONTRACEPTIVO: valor 0 inserido como sentinela "não informado"
--                Causa: internações sem método contraceptivo preenchido
--                       chegam com CONTRACEP1/CONTRACEP2 = 0, ausente na fonte

{{ config(materialized='table') }}

SELECT
    CONTRACEPTIVO,
    DESCRICAO
FROM {{ source('main', 'contraceptivos') }}

UNION ALL

SELECT
    0    AS CONTRACEPTIVO,
    'Não informado' AS DESCRICAO