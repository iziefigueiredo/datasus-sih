-- models/staging/stg_raca_cor.sql
-- T2: regras de negócio aplicadas sobre raca_cor (pós-carga)
--
-- RACA_COR: valor 0 inserido como sentinela "não informado"
--           Causa: stg_internacoes valores fora de [1-5] para 0,
--                  ausente na fonte TAB_SIH / RACACOR.cnv

{{ config(materialized='table') }}

SELECT
    RACA_COR,
    DESCRICAO
FROM {{ source('main', 'raca_cor') }}

UNION ALL

SELECT
    0    AS RACA_COR,
    'Não informado' AS DESCRICAO