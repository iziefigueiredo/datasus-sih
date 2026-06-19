-- models/staging/stg_internacoes.sql
-- T2: regras de negócio aplicadas sobre internacoes (pós-carga)
--
-- RACA_COR: valores fora de [0,1,2,3,4,5] → 0 (Sem informação)
--           Causa: dados brutos SIH usam 99 para "Sem informação"
--
-- MUNIC_RES: códigos 530xxx (Regiões Administrativas do DF) → 530010 (Brasília)
--            Causa: DF possui Regiões Administrativas sem código IBGE próprio
--
-- CONTRACEP1/2: valores fora de [0,12] → 0 (Sem informação)
--               
--
-- CID (DIAG_SECUN, CID_MORTE, CID_NOTIF, DIAGSEC1-9):
--     string vazia ou código ausente na tabela cid → NULL
--     Causa: campos nullable no SIH chegam como string vazia no raw;
--            4 registros com W46 (categoria pai sem subdígito de local)
--            ausente no S_CID.DBF do DATASUS
--
-- DIAS_PERM: recalculado a partir das datas de internação e saída.
--            Quando UTI_INT_TO > 0, aplica DT_SAIDA - DT_INTER + 1
--            para alinhar com o total de diárias de UTI.
--            Nos demais casos, aplica DT_SAIDA - DT_INTER.
--            Auditoria: 2026-03-10

-- INSC_PN: 3.582 registros de sexo masculino com inscrição pré-natal preenchida
--          com valores aparentemente válidos permanecem sem correção —
--          inconsistência irresolvível sem identificação do paciente.
--          Auditoria: 2026-03-29

-- MORTE: corrigido para true quando CID_MORTE está preenchido e MORTE = false
--        Causa: 322 registros com CID_MORTE válido mas MORTE = false (erro de digitação)
--        Auditoria: 2026-03-27

-- NACIONAL: valor não presente na tabela de nacionalidade → 0 (Sem informação)
--           Causa: 1 internação com código de nacionalidade ausente na fonte
--           Auditoria: 2026-03-27

-- CID_MORTE: corrigido para NULL quando CID ausente na tabela de CID
--            Causa: 194468408 registros com CID_MORTE ausente na tabela de CID,
--                   indicando que o campo foi preenchido com um código inválido
--            Auditoria: 2026-03-27

-- INSTRU: valor não presente na tabela de instrução → 0 (Sem informação)
--         Causa: 195M internações com INSTRU = 0 na fonte, ausente na tabela de instrução e 7 valores de INSTRU fora de [1-4], indicando que o campo foi preenchido com um código inválido
--         Auditoria: 2026-03-27

-- ESPEC: valores fora de [1-16, 31-49, 61-95] → '0' (Sem informação)
--        Causa: 2818 registros com ESPEC = '17' e outros fora do domínio válido
--        Auditoria: 2026-03-28

-- IDADE: recalculado como DT_INTER - NASC (anos completos, considerando mês e dia)
--        Causa: 1.865.277 registros com IDADE inconsistente na fonte
--        Auditoria: 2026-03-28
{{ config(materialized='table') }}

SELECT
    N_AIH,
    CNES,
    DT_INTER,
    DT_SAIDA,
    CASE 
        WHEN UTI_INT_TO > 0 THEN DATEDIFF('day', DT_INTER, DT_SAIDA) + 1
        ELSE DATEDIFF('day', DT_INTER, DT_SAIDA)
    END AS DIAS_PERM,
    DIAR_ACOM,
    CAR_INT,
    CASE 
        WHEN CAST(ESPEC AS INTEGER) BETWEEN 1 AND 16 THEN ESPEC
        WHEN CAST(ESPEC AS INTEGER) BETWEEN 31 AND 49 THEN ESPEC
        WHEN CAST(ESPEC AS INTEGER) BETWEEN 61 AND 95 THEN ESPEC
        ELSE '0'
    END AS ESPEC,
    COMPLEX,
    MARCA_UTI,
    UTI_INT_TO,
    IND_VDRL,
    CASE 
        WHEN MORTE = 0 
        AND NULLIF(CID_MORTE, '') IN (SELECT CID FROM {{ source('main', 'cid') }})
        THEN 1
        ELSE MORTE 
    END AS MORTE,
    
        
    GESTRISCO,
    DIAG_PRINC,
    CASE WHEN NULLIF(DIAG_SECUN, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAG_SECUN ELSE NULL END AS DIAG_SECUN,
    CASE WHEN NULLIF(CID_MORTE, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN CID_MORTE ELSE NULL END AS CID_MORTE,
    CASE WHEN NULLIF(CID_NOTIF, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN CID_NOTIF ELSE NULL END AS CID_NOTIF,
    CASE WHEN NULLIF(DIAGSEC1, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC1 ELSE NULL END AS DIAGSEC1,
    CASE WHEN NULLIF(DIAGSEC2, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC2 ELSE NULL END AS DIAGSEC2,
    CASE WHEN NULLIF(DIAGSEC3, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC3 ELSE NULL END AS DIAGSEC3,
    CASE WHEN NULLIF(DIAGSEC4, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC4 ELSE NULL END AS DIAGSEC4,
    CASE WHEN NULLIF(DIAGSEC5, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC5 ELSE NULL END AS DIAGSEC5,
    CASE WHEN NULLIF(DIAGSEC6, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC6 ELSE NULL END AS DIAGSEC6,
    CASE WHEN NULLIF(DIAGSEC7, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC7 ELSE NULL END AS DIAGSEC7,
    CASE WHEN NULLIF(DIAGSEC8, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC8 ELSE NULL END AS DIAGSEC8,
    CASE WHEN NULLIF(DIAGSEC9, '') IN (SELECT CID FROM {{ source('main', 'cid') }}) THEN DIAGSEC9 ELSE NULL END AS DIAGSEC9,
    VAL_SH,
    VAL_SP,
    VAL_UTI,
    VAL_TOT,
    NASC,
    DATEDIFF('year', NASC, DT_INTER) -
        CASE 
            WHEN MONTH(DT_INTER) < MONTH(NASC) THEN 1
            WHEN MONTH(DT_INTER) = MONTH(NASC) 
            AND DAY(DT_INTER)  < DAY(NASC)   THEN 1
            ELSE 0
        END AS IDADE,
    SEXO,
    CASE WHEN RACA_COR IN (1,2,3,4,5) THEN RACA_COR ELSE 0 END AS RACA_COR,
    CASE WHEN ETNIA BETWEEN 0 AND 264 THEN ETNIA ELSE 0 END AS ETNIA,
    CASE WHEN NACIONAL IN (SELECT NACIONAL FROM {{ source('main', 'nacionalidade') }}) 
        THEN NACIONAL 
        ELSE 0 
    END AS NACIONAL,
    
    CASE WHEN INSTRU IN (1,2,3,4) THEN INSTRU ELSE 0 END AS INSTRU,
    VINCPREV,

    CASE WHEN CBOR IN (SELECT CBOR FROM {{ source('main', 'cbor') }}) 
         THEN CBOR 
         ELSE '000000' 
    END AS CBOR,

    CASE WHEN MUNIC_RES BETWEEN 530000 AND 539999 THEN 530010 ELSE MUNIC_RES END AS MUNIC_RES,
    CEP,
    NUM_FILHOS,
    CASE WHEN CONTRACEP1 BETWEEN 0 AND 12 THEN CONTRACEP1 ELSE 0 END AS CONTRACEP1,
    CASE WHEN CONTRACEP2 BETWEEN 0 AND 12 THEN CONTRACEP2 ELSE 0 END AS CONTRACEP2,
    CASE 
        WHEN LENGTH(TRIM(REPLACE(INSC_PN, '0', ''))) = 0 THEN NULL
        WHEN LENGTH(TRIM(REPLACE(INSC_PN, '0', ''))) < 4 THEN NULL
        ELSE INSC_PN 
    END AS INSC_PN

FROM {{ source('main', 'internacoes') }}