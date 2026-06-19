-- stg_complexidade.sql
-- models/staging/stg_complexidade.sql
-- Adicionada sentinela "Não informado" para complexidade
-- Auditoria: 2026-03-28

{{ config(materialized='table') }}

SELECT
    COMPLEX,
    DESCRICAO
FROM {{ source('main', 'complexidade') }}

UNION ALL

SELECT
    '00' AS COMPLEX,
    'Não informado' AS DESCRICAO