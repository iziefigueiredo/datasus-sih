-- models/staging/stg_nacionalidade.sql
-- T2: regras de negócio aplicadas sobre nacionalidade (pós-carga)
--
-- NACIONAL: valor 0 inserido como sentinela "não informado"
--           Causa: 16 internações sem nacionalidade preenchida na fonte
--           Auditoria: 2026-03-27

{{ config(materialized='table') }}

SELECT
    NACIONAL,
    DESCRICAO
FROM {{ source('main', 'nacionalidade') }}

UNION ALL

SELECT
    0    AS NACIONAL,
    'Não informado' AS DESCRICAO