-- models/staging/stg_sexo.sql
-- Apos: Aud 1 é aplicada transformacao sobre a tabela de domínio sexo (pós-carga)
--
-- SEXO: valor 2 removido da tabela de domínio.
--       Causa: DATASUS entrega valor 2 no csv mas o domínio oficial do SIH é:
--              0 = Sem informação
--              1 = Masculino  
--              3 = Feminino
--       Registros em internacoes com SEXO=2 são normalizados para 3
--       em stg_internacoes.sql.

{{ config(materialized='table') }}

SELECT * FROM {{ source('main', 'sexo') }}
WHERE SEXO != 2