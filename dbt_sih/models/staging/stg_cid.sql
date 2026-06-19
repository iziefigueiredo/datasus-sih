-- models/staging/stg_cid.sql
-- T2: regras de negócio aplicadas sobre cid (pós-carga)
--
-- Injeta códigos CID-10 ausentes no S_CID.DBF do DATASUS via seed cid_manuais.
-- Origem das ausências: códigos OMS adicionados após a versão do DBF disponível
-- no FTP DATASUS (COVID-19, doença renal crônica por estádio, resistência antimicrobiana).
--
-- Códigos injetados (12):
--   U04, U09, U099, U10, U109        — COVID-19 e condições pós-COVID (OMS 2003-2021)
--   U80, U81, U89                    — Resistência antimicrobiana (OMS 2010)
--   N182, N183, N184, N185           — Doença renal crônica por estádio (OMS 2010)
--
-- Auditoria: 2026-03-27

{{ config(materialized='table') }}

SELECT
    CID,
    DESCRICAO,
    TP_NIVEL,
    RESTRSEXO,
    DS_CATEGORIA,
    DS_GRUPO,
    DS_CAPITULO
FROM {{ source('main', 'cid') }}

UNION ALL

SELECT
    CID,
    DESCRICAO,
    TP_NIVEL,
    RESTRSEXO,
    DS_CATEGORIA,
    DS_GRUPO,
    DS_CAPITULO
FROM {{ ref('cid_manuais') }}