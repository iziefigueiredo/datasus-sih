-- stg_etnia.sql
-- models/staging/stg_etnia.sql
-- Adicionada sentinela "Não informado" para etnia
-- Auditoria: 2026-03-28

{{ config(materialized='table') }}

SELECT
    ETNIA,
    DESCRICAO
FROM {{ source('main', 'etnia') }}

UNION ALL

SELECT
    0    AS ETNIA,
    'Não informado' AS DESCRICAO