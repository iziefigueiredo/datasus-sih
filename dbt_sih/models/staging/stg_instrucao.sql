-- models/staging/stg_instrucao.sql
-- T2: regras de negócio aplicadas sobre instrucao (pós-carga)
--
-- INSTRU: valor 0 inserido como sentinela "não informado"
--         Causa: 195M internações com INSTRU = 0 na fonte,
--                ausente na tabela TAB_SIH / INSTRU.cnv
--                7 valores de INSTRU fora de [1-4], indicando que o campo foi preenchido com um código inválido
--         Auditoria: 2026-03-27

{{ config(materialized='table') }}

SELECT
    INSTRU,
    DESCRICAO
FROM {{ source('main', 'instrucao') }}

UNION ALL

SELECT
    0    AS INSTRU,
    'Não informado' AS DESCRICAO