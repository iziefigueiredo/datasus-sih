# src/database/schema.py
#
# Definição canônica do esquema do banco analítico SIH/SUS (DuckDB).
#
# Princípio DAMA: o schema é metadado de estrutura — deve documentar
# não apenas tipos, mas a origem e o papel de cada entidade no modelo.
#
# Modelo: Snowflake Schema — nomenclatura MAD (Modelo Analítico de Dados)
#   TF_ Fato central  : internacoes
#   RL_ Tabela ponte  : internacao_procedimento (M:N internação ↔ procedimento)
#   TD_ Dimensões     : hospital, municipios, cid, procedimentos, especialidade,
#                       complexidade, sexo, raca_cor, etnia, nacionalidade,
#                       instrucao, vincprev, contraceptivos, car_int, cbor,
#                       marca_uti, tempo
#   TD_ Analítico     : socioeconomico (wide — uma métrica por coluna, V5)
#
# Convenção de idioma (C1):
#   Nomes de tabelas e colunas seguem nomenclatura DATASUS/IBGE (PT)
#   quando provenientes das fontes; EN para chaves técnicas geradas
#   pelo pipeline (ex: id_atendimento).
#
# Notas de versão:
#   [P6]  DIAG_SECUN zerado a partir de 2015. Os 9 slots DIAGSEC1-DIAGSEC9
#         existem como colunas a partir de 2014 mas ficam vazios — preenchidos
#         efetivamente a partir de 2015. Auditoria empírica RS 2014 confirma:
#         DIAGSEC1-9 = string vazia em 100% dos registros de 2014.
#         DIAG_SECUN mantido para compatibilidade retroativa 2008-2014.
#   [D34] Leitos psiquiátricos excluídos do VL_LEITOS_SUS_1000 por decisão
#         metodológica (ref: ODR/MDR TAB_0135).
#   [V5]  Modelo socioeconômico wide com 4 métricas anuais: PIB per capita,
#         mortalidade infantil, leitos SUS/1000 hab, médicos/1000 hab.
#         IDH e cobertura de saneamento descartados (cobertura insuficiente).

import polars as pl
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Tipo auxiliar para legibilidade
# ---------------------------------------------------------------------------

TableSchema = Dict[str, Any]


# ---------------------------------------------------------------------------
# TD_ DIMENSÕES — tabelas de domínio estáticas
# Fonte: TAB_SIH.zip (FTP DATASUS), exceto onde indicado.
# ---------------------------------------------------------------------------

_TD_MUNICIPIO: TableSchema = {
    # Fonte: IBGE — 5.570 municípios brasileiros
    # CO_MUNICIPIO_6D : código sem dígito verificador — chave de junção no pipeline
    # CO_MUNICIPIO_7D : código completo IBGE (7 dígitos, max 5.300.108 < 2,1e9)
    # NO_REGIAO_SAUDE : enriquecimento externo — permite análises por região de saúde
    "columns": {
        "CO_MUNICIPIO_6D": pl.Int32,
        "CO_MUNICIPIO_7D": pl.Int32,
        "NO_MUNICIPIO":    pl.String,
        "SG_UF":           pl.String,
        "NO_REGIAO_SAUDE": pl.String,   # nullable
        "latitude":        pl.Float32,
        "longitude":       pl.Float32,
    },
    "primary_key": ["CO_MUNICIPIO_6D"],
    "foreign_keys": [],
}

_TD_CID: TableSchema = {
    # Fonte: DATASUS S_CID.DBF (TAB_SIH.zip) — ~14.705 registros
    # Hierarquia CID-10 OMS (4 níveis): Capítulo > Grupo > Categoria > Subcategoria
    # DS_CATEGORIA: descrição da categoria (3 chars), derivada do próprio S_CID.DBF
    # DS_GRUPO: faixa de categorias (ex: "J09-J18 Influenza e pneumonia")
    # DS_CAPITULO: capítulo CID-10 (ex: "X. Doenças do aparelho respiratório")
    "columns": {
        "CID":           pl.String,   # código 3d (categoria) ou 4d (subcategoria)
        "DESCRICAO":     pl.String,   # descrição DATASUS
        "TP_NIVEL":      pl.String,   # 'CAT' ou 'SUBCAT'
        "RESTRSEXO":     pl.String,   # 1=masc, 3=fem, 5=ambos
        "DS_CATEGORIA":  pl.String,   # descrição da categoria (3 chars)
        "DS_GRUPO":      pl.String,   # grupo CID-10 (faixa de categorias)
        "DS_CAPITULO":   pl.String,   # capítulo CID-10 (nível mais alto)
    },
    "primary_key": ["CID"],
    "foreign_keys": [],
}

_TD_PROCEDIMENTO: TableSchema = {
    # Fonte: SIGTAP — tabela unificada de procedimentos SUS (~5.394 registros)
    # PROC_REA: código SIGTAP 10 dígitos. Armazenado como Int64 para economia
    # de espaço (~200 MB em 183M registros). Zeros à esquerda descartados —
    # join funciona com Int64 em ambos os lados; dashboard exibe NOME_PROC.
    "columns": {
        "PROC_REA":  pl.String, #código 10 dígitos, armazenado como String para preservar zeros à esquerda
        "NOME_PROC": pl.String,
    },
    "primary_key": ["PROC_REA"],
    "foreign_keys": [],
}

_TD_COMPLEXIDADE: TableSchema = {
    # Fonte: Fiocruz/PCDaS — 3 níveis de complexidade assistencial
    # Domínio: '01'=Atenção Básica, '02'=Média Complexidade, '03'=Alta Complexidade
    "columns": {
        "COMPLEX":    pl.Int8,   # C(02)
        "DESCRICAO":  pl.String,
    },
    "primary_key": ["COMPLEX"],
    "foreign_keys": [],
}

_TD_ESPECIALIDADE: TableSchema = {
    # Fonte: Fiocruz/PCDAS — 39 especialidades (campo ESPEC do SIH)
    "columns": {
        "ESPEC":     pl.Int8,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["ESPEC"],
    "foreign_keys": [],
}

_TD_SEXO: TableSchema = {
    # Fonte: TAB_SIH / SEXO.cnv
    # Domínio: 1=Masculino, 2=Feminino, 3=Feminino (duplicado no CNV)
    "columns": {
        "SEXO":      pl.Int8,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["SEXO"],
    "foreign_keys": [],
}

_TD_RACA_COR: TableSchema = {
    # Fonte: TAB_SIH / RACACOR.cnv
    # RACA_COR: valores 1-5; 0 = Sem informação (sentinela). Armazenado como Int8.
    # Atenção: dados brutos usam 99 para "Sem informação" — preprocess.py
    # normaliza 99 → 0 (bug de .clip() corrigido em 2026-03).
    "columns": {
        "RACA_COR":  pl.Int8,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["RACA_COR"],
    "foreign_keys": [],
}

_TD_ETNIA: TableSchema = {
    # Fonte: FUNAI/IBGE — etnias indígenas (~2.812 registros)
    # Int16: códigos FUNAI chegam a 4 dígitos (max ~9.999 > limite Int8=127)
    "columns": {
        "ETNIA":     pl.Int16,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["ETNIA"],
    "foreign_keys": [],
}

_TD_NACIONALIDADE: TableSchema = {
    # Fonte: TAB_SIH / NACION3D.cnv — 333 nacionalidades
    # Int16: códigos até ~300+ (> limite Int8=127)
    "columns": {
        "NACIONAL":  pl.Int16,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["NACIONAL"],
    "foreign_keys": [],
}

_TD_INSTRUCAO: TableSchema = {
    # Fonte: TAB_SIH / INSTRU.cnv — grau de instrução
    # Qualidade: ~99% dos registros com código 0 (Não informado) — confirmado
    # empiricamente: 182M registros com INSTRU=0 em Brasil completo 2008-2023.
    # Domínio real nos dados SIH: apenas 0-4 (instrucao.csv). Códigos 5,6,8
    # encontrados em 6 registros — ruído de digitação, não adicionados à dimensão.
    "columns": {
        "INSTRU":    pl.Int8,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["INSTRU"],
    "foreign_keys": [],
}

_TD_VINCPREV: TableSchema = {
    # Fonte: TAB_SIH / VINCPREV.cnv — vínculo previdenciário
    "columns": {
        "VINCPREV":  pl.Int8,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["VINCPREV"],
    "foreign_keys": [],
}

_TD_CONTRACEPTIVO: TableSchema = {
    # Fonte: TAB_SIH / CONTRAC.cnv — método contraceptivo
    "columns": {
        "CONTRACEPTIVO": pl.Int8,
        "DESCRICAO":     pl.String,
    },
    "primary_key": ["CONTRACEPTIVO"],
    "foreign_keys": [],
}

_TD_CAR_INT: TableSchema = {
    # Fonte: TAB_SIH / CARATEND.cnv — caráter da internação
    # Domínio pós Portaria MS 719/2007:
    #   01=Eletivo, 02=Urgência, 03=Acidente trabalho,
    #   04=Acidente trânsito, 05=Outros acidentes, 06=Cirurgia eletiva
    "columns": {
        "CAR_INT":   pl.Int8,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["CAR_INT"],
    "foreign_keys": [],
}

_TD_OCUPACAO: TableSchema = {
    # Fonte: TAB_SIH / CBO.cnv — ocupação do paciente (subgrupo CBO 3 dígitos)
    # CBOR: alfanumérico — CBO 1994 tem sufixos como '1999A2'; CBO 2002 é numérico.
    # String absorve ambas as versões históricas sem perda.
    "columns": {
        "CBOR":      pl.String,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["CBOR"],
    "foreign_keys": [],
}

_TD_MARCA_UTI: TableSchema = {
    # Fonte: TAB_SIH / MARCAUTI.cnv — tipo de UTI utilizada
    # Domínio pós-2008: 0=sem UTI, 74-83=tipos de UTI, 99=não informado
    "columns": {
        "MARCA_UTI": pl.Int8,
        "DESCRICAO": pl.String,
    },
    "primary_key": ["MARCA_UTI"],
    "foreign_keys": [],
}

_TD_TEMPO: TableSchema = {
    # Gerado pelo pipeline (split.py) — um registro por dia, 2008-01-01 a 2023-12-31
    "columns": {
        "data":       pl.Date,
        "ano":        pl.Int16,
        "mes":        pl.Int8,
        "trimestre":  pl.Int8,
        "dia_semana": pl.Int8,   # 0=segunda ... 6=domingo (ISO)
    },
    "primary_key": ["data"],
    "foreign_keys": [],
}


# ---------------------------------------------------------------------------
# DIMENSÃO DEGENERADA — derivada do SIH (não de tabela de domínio externa)
# ---------------------------------------------------------------------------

_TD_HOSPITAL: TableSchema = {
    # Dimensão degenerada — derivada dos dados transacionais do SIH.
    # Granularidade: CNES único (valor modal por coluna após split.py).
    # CNES       : C(07) — 7 dígitos numéricos, Int32 suficiente.
    # NO_HOSPITAL: enriquecimento externo via CNES cadastral (nullable).
    "columns": {
        "CNES":        pl.String,
        "NO_HOSPITAL": pl.String,   # nullable — fonte externa CNES
        "MUNIC_MOV":   pl.Int32,    # código 6 dígitos — Int32 suficiente
        "NATUREZA":    pl.String,
        "GESTAO":      pl.String,
        "NAT_JUR":     pl.String,   # código CONCLA 4 dígitos (ex: '1023')
    },
    "primary_key": ["CNES"],
    "foreign_keys": [
        {"column": "MUNIC_MOV", "references_table": "municipios", "references_column": "CO_MUNICIPIO_6D"},
    ],
}


# ---------------------------------------------------------------------------
# TF_ FATOS — tabelas transacionais do SIH
# ---------------------------------------------------------------------------

_TF_INTERNACAO: TableSchema = {
    # Fato central do modelo — uma linha por AIH contraída.
    # Granularidade: internação (N_AIH único após aggregate.py).
    #
    # PK: N_AIH como String — char(13) no IT_SIHSUS, overflow silencioso se int.
    # A PK composta do raw (N_AIH + UF + competência) é resolvida pelo aggregate.py;
    # a tabela analítica final tem granularidade de internação, não de arquivo mensal.
    #
    # Diagnósticos secundários [P6]:
    #   DIAG_SECUN : campo original 2008-2014, zerado a partir de 2015
    #   DIAGSEC1-9 : campos introduzidos em 2015, esparsos (nullable)
    #   Ambos coexistem para cobertura completa do período 2008-2023.
    "columns": {
        # --- Identificação ---
        "N_AIH":  pl.Int64,  # 13 dígitos — nenhuma UF começa com 0; Int64 economiza ~900 MB
        "CNES":   pl.String,   # 7 dígitos — Int32 suficiente

        # --- Datas e permanência ---
        "DT_INTER":  pl.Date,
        "DT_SAIDA":  pl.Date,
        "DIAS_PERM": pl.Int16,
        "DIAR_ACOM": pl.Int16,  # diárias de acompanhante

        # --- Classificação assistencial ---
        "CAR_INT":    pl.Int8,   # caráter da internação (FK -> car_int)
        "ESPEC":      pl.Int8,   # especialidade do leito (FK -> especialidade)
        "COMPLEX":    pl.Int8, # complexidade assistencial (FK -> complexidade)
        "MARCA_UTI":  pl.Int8,   # tipo de UTI (FK -> marca_uti)
        "UTI_INT_TO": pl.Int8,   # total de diárias de UTI

        # --- Indicadores clínicos ---
        "IND_VDRL":  pl.Boolean,  # exame VDRL — 'S'/'N' no raw convertido para Boolean
        "MORTE":     pl.Boolean,  # óbito — 'S'/'N' no raw convertido para Boolean
        "GESTRISCO": pl.Boolean,  # gestação de risco — 'S'/'N' no raw convertido para Boolean

        # --- Diagnósticos ---
        "DIAG_PRINC": pl.String,  # CID-10 principal (FK -> cid)
        "DIAG_SECUN": pl.String,  # CID-10 secundário 2008-2014 [P6] (FK -> cid)
        "CID_MORTE":  pl.String,  # causa da morte, nullable (FK -> cid)
        "CID_NOTIF":  pl.String,  # notificação compulsória, nullable (FK -> cid)
        # Slots DIAGSEC: colunas existem a partir de 2014, preenchidas a partir de 2015 [P6]
        "DIAGSEC1": pl.String,
        "DIAGSEC2": pl.String,
        "DIAGSEC3": pl.String,
        "DIAGSEC4": pl.String,
        "DIAGSEC5": pl.String,
        "DIAGSEC6": pl.String,
        "DIAGSEC7": pl.String,
        "DIAGSEC8": pl.String,
        "DIAGSEC9": pl.String,

        # --- Valores financeiros ---
        "VAL_SH":  pl.Float64,
        "VAL_SP":  pl.Float64,
        "VAL_UTI": pl.Float64,
        "VAL_TOT": pl.Float64,

        # --- Dados do paciente ---
        "NASC":    pl.Date,
        "IDADE":   pl.Int16,
        "COD_IDADE": pl.Int8,   
        "SEXO":    pl.Int8,    # FK -> sexo
        "RACA_COR":  pl.Int8,  # valores 0-5 (0=Sem informação); FK -> raca_cor
        "ETNIA":     pl.Int16,  # nullable — só se indígena; FK -> etnia
        "NACIONAL":  pl.Int16,  # FK -> nacionalidade
        "INSTRU":    pl.Int8,   # FK -> instrucao
        "VINCPREV":  pl.Int8,   # FK -> vincprev
        "CBOR":      pl.String, # ocupação CBO, nullable (FK -> cbor)
        "MUNIC_RES": pl.Int32,  # código 6 dígitos; FK -> municipios
        # CEP: C(08) com zeros à esquerda (ex: '01310100' = Av. Paulista, SP)
        "CEP":       pl.String,
        "NUM_FILHOS":  pl.Int8,
        "CONTRACEP1":  pl.Int8,   # FK -> contraceptivos
        "CONTRACEP2":  pl.Int8,   # FK -> contraceptivos
        "INSC_PN":     pl.String, # inscrição pré-natal, nullable
    },
    "primary_key": ["N_AIH"],
    "foreign_keys": [
        {"column": "CNES",      "references_table": "hospital",      "references_column": "CNES"},
        {"column": "CAR_INT",   "references_table": "car_int",       "references_column": "CAR_INT"},
        {"column": "ESPEC",     "references_table": "especialidade",  "references_column": "ESPEC"},
        {"column": "COMPLEX",   "references_table": "complexidade",   "references_column": "COMPLEX"},
        {"column": "MARCA_UTI", "references_table": "marca_uti",     "references_column": "MARCA_UTI"},
        {"column": "DIAG_PRINC","references_table": "cid",           "references_column": "CID"},
        {"column": "DIAG_SECUN","references_table": "cid",           "references_column": "CID"},
        {"column": "CID_MORTE", "references_table": "cid",           "references_column": "CID"},
        {"column": "CID_NOTIF", "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC1",  "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC2",  "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC3",  "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC4",  "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC5",  "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC6",  "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC7",  "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC8",  "references_table": "cid",           "references_column": "CID"},
        {"column": "DIAGSEC9",  "references_table": "cid",           "references_column": "CID"},
        {"column": "SEXO",      "references_table": "sexo",          "references_column": "SEXO"},
        {"column": "RACA_COR",  "references_table": "raca_cor",      "references_column": "RACA_COR"},
        {"column": "ETNIA",     "references_table": "etnia",         "references_column": "ETNIA"},
        {"column": "NACIONAL",  "references_table": "nacionalidade", "references_column": "NACIONAL"},
        {"column": "INSTRU",    "references_table": "instrucao",     "references_column": "INSTRU"},
        {"column": "VINCPREV",  "references_table": "vincprev",      "references_column": "VINCPREV"},
        {"column": "CBOR",      "references_table": "cbor",          "references_column": "CBOR"},
        {"column": "MUNIC_RES", "references_table": "municipios",    "references_column": "CO_MUNICIPIO_6D"},
        {"column": "CONTRACEP1","references_table": "contraceptivos","references_column": "CONTRACEPTIVO"},
        {"column": "CONTRACEP2","references_table": "contraceptivos","references_column": "CONTRACEPTIVO"},
    ],
}

_RL_INTERNACAO_PROCEDIMENTO: TableSchema = {
    # Tabela ponte M:N — um registro por procedimento realizado por internação.
    # Granularidade: N_AIH x PROC_REA.
    # id_atendimento: chave surrogate gerada pelo pipeline (split.py).
    "columns": {
        "id_atendimento": pl.UInt64,
        "N_AIH":          pl.Int64,   # consistente com internacoes
        "PROC_REA":       pl.String,   # consistente com procedimentos
    },
    "primary_key": ["id_atendimento"],
    "foreign_keys": [
        {"column": "N_AIH",    "references_table": "internacoes",   "references_column": "N_AIH"},
        {"column": "PROC_REA", "references_table": "procedimentos", "references_column": "PROC_REA"},
    ],
}


# ---------------------------------------------------------------------------
# TD_ ANALÍTICO — tabela wide socioeconômica (V5)
# ---------------------------------------------------------------------------

_TD_SOCIOECONOMICO: TableSchema = {
    # Modelo wide — uma métrica por coluna.
    # Granularidade: município x ano.
    # Fonte: pipeline socioeconômico (extrair_populacao, extrair_pib,
    #        extrair_mort_infantil, extrair_leitos, extrair_medicos).
    #
    # Cobertura temporal por métrica:
    #   VL_PIB_PERCAPITA   : 2008-2021 (lag ~2 anos — IBGE PIB Municipal)
    #   VL_MORT_INFANTIL   : 2008-2023 (lag ~15 meses — SIM + SINASC)
    #   VL_LEITOS_SUS_1000 : 2008-2023 (lag mensal — CNES/LT, excl. psiquiátrico [D34])
    #   VL_MEDICOS_1000    : 2008-2023 (lag mensal — CNES/PF, dedup por CPF x município)
    #
    # IDH e cobertura de saneamento descartados [V5]:
    #   IDH        — calculado apenas a cada 10 anos (censo), incompatível com série anual.
    #   Saneamento — lacunas extensas em municípios pequenos (<5.000 hab).
    "columns": {
        "CO_MUNICIPIO_6D": pl.Int32,
        "NU_ANO":          pl.Int16,
        "QT_POPULACAO":       pl.Int64,
        "VL_PIB_PERCAPITA":   pl.Float64,  # R$ correntes — NULL 2022+ ate IBGE publicar
        "QT_OBITOS_INFANTIS": pl.Int32,
        "QT_NASCIDOS_VIVOS":  pl.Int32,
        "VL_MORT_INFANTIL":   pl.Float64,  # obitos <1 ano / nascidos vivos x 1.000
        "QT_LEITOS_SUS":      pl.Int32,
        "VL_LEITOS_SUS_1000": pl.Float64,  # leitos SUS (excl. psiq.) / pop x 1.000
        "QT_MEDICOS":         pl.Int32,
        "VL_MEDICOS_1000":    pl.Float64,  # medicos unicos (CPF) / pop x 1.000
        "QT_BENEFICIARIOS_PLANO_SAUDE": pl.Int64,    # beneficiários planos privados (ANS)
        "QT_ESTAB_INTERNACAO_SUS":      pl.Int32,    # estabelecimentos de internação SUS
        "QT_ESTAB_SAUDE":               pl.Int32,    # estabelecimentos de saúde (total)
        "QT_ESTAB_URGENCIA_SUS":        pl.Int32,    # estabelecimentos de urgência SUS
        "VL_ENFERMEIROS_1000":          pl.Float64,  # enfermeiros / pop x 1.000
        "VL_TECNICOS_SAUDE_1000":       pl.Float64,  # aux./técnicos de saúde / pop x 1.000
        "VL_LEITOS_UTI_SUS_1000":       pl.Float64,  # leitos UTI SUS / pop x 1.000
    },
    "primary_key": ["CO_MUNICIPIO_6D", "NU_ANO"],
    "foreign_keys": [
        {"column": "CO_MUNICIPIO_6D", "references_table": "municipios", "references_column": "CO_MUNICIPIO_6D"},
    ],
}


# ---------------------------------------------------------------------------
# REGISTRO CENTRAL — ordem de carga respeita dependências FK
#
# Regra DAMA: a ordem de carga é metadado de linhagem —
# deve ser explícita e documentada, não implícita no código de carga.
# ---------------------------------------------------------------------------

TABLE_SCHEMAS: Dict[str, TableSchema] = {
    # 1. TD_ Dimensões sem dependências (podem ser carregadas em paralelo)
    "municipios":     _TD_MUNICIPIO,
    "cid":            _TD_CID,
    "procedimentos":  _TD_PROCEDIMENTO,
    "complexidade":   _TD_COMPLEXIDADE,
    "especialidade":  _TD_ESPECIALIDADE,
    "sexo":           _TD_SEXO,
    "raca_cor":       _TD_RACA_COR,
    "etnia":          _TD_ETNIA,
    "nacionalidade":  _TD_NACIONALIDADE,
    "instrucao":      _TD_INSTRUCAO,
    "vincprev":       _TD_VINCPREV,
    "contraceptivos": _TD_CONTRACEPTIVO,
    "car_int":        _TD_CAR_INT,
    "cbor":           _TD_OCUPACAO,
    "marca_uti":      _TD_MARCA_UTI,
    "tempo":          _TD_TEMPO,

    # 2. TD_ Hospital (depende: municipios)
    "hospital":    _TD_HOSPITAL,

    # 3. TF_ Fato + RL_ Ponte (dependem de dimensões acima)
    "internacoes": _TF_INTERNACAO,   # depende: hospital, municipios, cid,
                                        #   complexidade, especialidade, car_int,
                                        #   marca_uti, sexo, raca_cor, etnia,
                                        #   nacionalidade, instrucao, vincprev,
                                        #   cbor, contraceptivos
    "internacao_procedimento":_RL_INTERNACAO_PROCEDIMENTO,  # depende: internacoes, procedimentos

    # 4. TD_ Analítico (depende de municipios)
    "socioeconomico": _TD_SOCIOECONOMICO,
}


# Ordem explícita de carga — usada pelo load.py
# Garante que FKs sejam satisfeitas no momento do INSERT
LOAD_ORDER = [
    # dimensões
    "municipios", "cid", "procedimentos", "complexidade", "especialidade",
    "sexo", "raca_cor", "etnia", "nacionalidade", "instrucao",
    "vincprev", "contraceptivos", "car_int", "cbor", "marca_uti", "tempo",
    # fatos
    "hospital", "internacoes", "internacao_procedimento",
    # analítico
    "socioeconomico",
]