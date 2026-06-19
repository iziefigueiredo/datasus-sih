-- stg_especialidade.sql
-- models/staging/stg_especialidade.sql
-- 2818 registros com ESPEC = '17' e outros fora do domínio válido → ESPEC = '0' (Sem informação)
-- Auditoria: 2026-03-28

{{ config(materialized='table') }}

SELECT
    ESPEC,
    DESCRICAO
FROM {{ source('main', 'especialidade') }}

UNION ALL

SELECT
    0    AS ESPEC,
    'Não informado' AS DESCRICAO