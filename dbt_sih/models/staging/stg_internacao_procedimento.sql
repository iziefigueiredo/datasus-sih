-- models/staging/stg_internacao_procedimento.sql
{{ config(materialized='table', tags=['t2']) }}

SELECT *
FROM {{ source('main', 'internacao_procedimento') }}