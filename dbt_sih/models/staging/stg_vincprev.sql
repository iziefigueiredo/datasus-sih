-- models/staging/stg_vincprev.sql
-- T2: regras de negócio aplicadas sobre vincprev (pós-carga)
--
-- VINCPREV: valor 0 inserido como sentinela "não informado"
--           Causa: stg_internacoes valores fora de [1-6] para 0,
--                  ausente na fonte TAB_SIH / VINCPREV.cnv

{{ config(materialized='table') }}

SELECT
    VINCPREV,
    DESCRICAO
FROM {{ source('main', 'vincprev') }}

UNION ALL

SELECT
    0    AS VINCPREV,
    'Não informado' AS DESCRICAO