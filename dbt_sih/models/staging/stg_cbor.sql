-- models/staging/stg_cbor.sql
-- T2: regras de negócio aplicadas sobre cbor (pós-carga)
--
-- CBOR: valor '000000' inserido como sentinela "não informado"
--       Causa: 197M internações com CBOR = '000000' na fonte,
--              ausente na tabela TAB_SIH / CBO.cnv
 
{{ config(materialized='table') }}
 
SELECT
    CBOR,
    DESCRICAO
FROM {{ source('main', 'cbor') }}
 
UNION ALL
 
SELECT
    '000000' AS CBOR,
    'Não informado' AS DESCRICAO