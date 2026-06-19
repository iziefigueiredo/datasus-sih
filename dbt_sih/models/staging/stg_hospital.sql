-- models/staging/stg_hospital.sql
-- T2: regras de negócio aplicadas sobre hospital (pós-carga)
--
-- MUNIC_MOV: códigos 530xxx (Regiões Administrativas do DF) → 530010 (Brasília)
--            Causa: DF possui Regiões Administrativas sem código IBGE próprio

{{ config(materialized='table') }}

SELECT
    CNES,
    NO_HOSPITAL,
    CASE WHEN MUNIC_MOV BETWEEN 530000 AND 539999 THEN 530010 ELSE MUNIC_MOV END AS MUNIC_MOV,
    NATUREZA,
    GESTAO,
    NAT_JUR
FROM {{ source('main', 'hospital') }}