-- models/staging/stg_procedimentos.sql
-- procedimento inseridos apos Aud 1 
{{ config(materialized='table', tags=['t2']) }}

SELECT PROC_REA, NOME_PROC
FROM {{ source('main', 'procedimentos') }}

UNION ALL

SELECT PROC_REA, NOME_PROC
FROM {{ ref('procedimentos_manuais') }}