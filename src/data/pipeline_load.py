"""
Pipeline de Carga Incremental por UF
Localização: sihrd5/src/data/pipeline_load.py

Substitui as etapas 4-8 do pipeline original (unify → preprocess →
aggregate → split → load) por um fluxo semi-ELT incremental:

    Para cada UF:
        1. Unificar parquets raw da UF (seleção de colunas)
        2. Pré-processar em Polars (cast, fill_null, limpeza)
        3. INSERT dados completos em internacao_procedimento (N_AIH × PROC_REA)
        4. INSERT raw na staging (sem agregação)
        5. Acumular dados de hospital

    Depois de todas as UFs:
        6. DuckDB agrega staging por N_AIH → INSERT em internacoes
        7. INSERT em hospital (com enriquecimento cadhosp)
"""

import polars as pl
import duckdb
import sys
import gc
import re
import time
import logging
import subprocess
from pathlib import Path

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))

from config.settings import Settings
from database.schema import TABLE_SCHEMAS, LOAD_ORDER
from data.transform.preprocess import SIHPreprocessor

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Colunas desejadas do SIH — mesma lista do unify.py original
COLUNAS_SIH = [
    'N_AIH', 'CNES',
    'DT_INTER', 'DT_SAIDA', 'DIAS_PERM', 'DIAR_ACOM',
    'CAR_INT', 'ESPEC', 'COMPLEX', 'MARCA_UTI', 'UTI_INT_TO',
    'IND_VDRL', 'MORTE', 'GESTRISCO',
    'DIAG_PRINC', 'DIAG_SECUN', 'CID_MORTE', 'CID_NOTIF',
    'DIAGSEC1', 'DIAGSEC2', 'DIAGSEC3', 'DIAGSEC4', 'DIAGSEC5',
    'DIAGSEC6', 'DIAGSEC7', 'DIAGSEC8', 'DIAGSEC9',
    'VAL_SH', 'VAL_SP', 'VAL_UTI', 'VAL_TOT',
    'NASC', 'SEXO', 'IDADE', 'COD_IDADE', 'NACIONAL', 'RACA_COR', 'ETNIA',
    'NUM_FILHOS', 'INSTRU', 'CBOR', 'MUNIC_RES', 'CEP',
    'CONTRACEP1', 'CONTRACEP2', 'VINCPREV', 'INSC_PN',
    'CGC_HOSP', 'NATUREZA', 'GESTAO', 'NAT_JUR', 'MUNIC_MOV',
    'PROC_REA',
]

# Colunas de internacoes (fato contraído) — mesma lista do split.py
COLUNAS_INTERNACOES = [
    "N_AIH", "CNES",
    "DT_INTER", "DT_SAIDA", "DIAS_PERM", "DIAR_ACOM",
    "CAR_INT", "ESPEC", "COMPLEX", "MARCA_UTI", "UTI_INT_TO",
    "IND_VDRL", "MORTE", "GESTRISCO",
    "DIAG_PRINC", "DIAG_SECUN", "CID_MORTE", "CID_NOTIF",
    "DIAGSEC1", "DIAGSEC2", "DIAGSEC3", "DIAGSEC4", "DIAGSEC5",
    "DIAGSEC6", "DIAGSEC7", "DIAGSEC8", "DIAGSEC9",
    "VAL_SH", "VAL_SP", "VAL_UTI", "VAL_TOT",
    "NASC", "SEXO", "IDADE", 'COD_IDADE', "NACIONAL", "RACA_COR", "ETNIA",
    "NUM_FILHOS", "INSTRU", "CBOR", "MUNIC_RES", "CEP",
    "CONTRACEP1", "CONTRACEP2", "VINCPREV", "INSC_PN",
]

COLUNAS_HOSPITAL = ["CNES", "CGC_HOSP", "NATUREZA", "GESTAO", "NAT_JUR", "MUNIC_MOV"]

# Agregação — mesma lógica do aggregate.py
COLUNAS_SOMA = ['VAL_SH', 'VAL_SP', 'VAL_UTI']
COLUNAS_MEDIA = ['UTI_INT_TO', 'DIAR_ACOM']
COLUNAS_RECALCULADAS = ['IDADE', 'DIAS_PERM', 'VAL_TOT']

# Sentinelas para dimensões (mesma lógica do split.py converter_csv_parquet)
FORCE_STRING = {
    # raca_cor removido — RACA_COR é Int8 (schema.py). CSV tem valores numéricos
    # sem zeros relevantes (0-5). FORCE_STRING causava type mismatch no sentinela.
    "complexidade":  {"COMPLEX": pl.String},
    "procedimentos": {"PROC_REA": pl.String},
    # cid: RESTRSEXO inferido como Int64 pelos primeiros 10k registros (numéricos).
    # Registros N182-N185 injetados manualmente têm RESTRSEXO="F" (feminino).
    # Sem override, Polars falha com ComputeError ao carregar cid.csv.
    "cid":           {"RESTRSEXO": pl.String},
}

COLUMN_RENAME = {
    "municipios": {
        "codigo_6d":   "CO_MUNICIPIO_6D",
        "codigo_ibge": "CO_MUNICIPIO_7D",
        "nome":        "NO_MUNICIPIO",
        "estado":      "SG_UF",
    },
}

SENTINELAS = {
    # --- Confirmados úteis pelo check_sentinels.py ---
    #"instrucao":      [{"INSTRU": 0, "DESCRICAO": "Não informado"},   # 182M registros
    #                   {"INSTRU": 9, "DESCRICAO": "Ignorado"}],       # 2 registros
    #"vincprev":       [{"VINCPREV": 0, "DESCRICAO": "Não informado"}], # 183M registros
    #"complexidade":   [{"COMPLEX": "00", "DESCRICAO": "Não informado"}], # 12 registros
    #"nacionalidade":  [{"NACIONAL": 0, "DESCRICAO": "Não informado"}],   # 34 registros

    # --- Não testados ainda — manter até validar com check_sentinels.py ---
    #"marca_uti":      [{"MARCA_UTI": 0, "DESCRICAO": "Sem UTI"}],
    #"cbor":           [{"CBOR": "000000", "DESCRICAO": "Não informado"}],
    #"municipios":     [{"CO_MUNICIPIO_6D": 0, "CO_MUNICIPIO_7D": 0,
    #                    "NO_MUNICIPIO": "Não informado", "SG_UF": "NA",
    #                    "NO_REGIAO_SAUDE": None,
    #                    "latitude": None, "longitude": None}],

    # --- Reativado após correção de bug em preprocess.py (2026-03) ---
    # preprocess.py normalizava RACA_COR=99 → 5 (Indígena) via .clip(0,5).
    # Corrigido para 99 → 0. Sentinela necessário para cobrir esses registros.
    #"raca_cor": [{"RACA_COR": 0, "DESCRICAO": "Sem informação"}],

    # --- Removidos (inócuos — nunca aparecem em internacoes) ---
    # "raca_cor":     "99" — era inócuo antes do bug; agora substituído por 0 acima
    # "sexo":         "0"         — check_sentinels.py confirmou n=0
    # "car_int":      "0"         — check_sentinels.py confirmou n=0
    # "especialidade":"0"         — check_sentinels.py confirmou n=0
    # "etnia":        "0000"      — check_sentinels.py confirmou n=0
    # "contraceptivos":"0"        — check_sentinels.py confirmou n=0

    # --- Pendente inspect_orfas.py — RESOLVIDO: ruído nos microdados, não adicionar ---
    # instrucao:      INSTRU=5,6,8 (6 registros em 183M) — não existem no CNV raw.
    #                 O CNV tem apenas 1,2,3,4. Valores fora do domínio, lixo de digitação.
    # contraceptivos: CONTRACEPTIVO=13 (274 registros) — CNV tem apenas 01-12.
    #                 Código fora do domínio documentado, lixo de digitação.
    # nacionalidade:  NACIONAL=16 (1 registro) — CNV usa códigos de 3 dígitos (ex: 029),
    #                 não sequenciais. Código inexistente no domínio, lixo de digitação.
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _polars_to_duckdb(tipo):
    """Converte tipo Polars para tipo DuckDB SQL."""
    mapping = {
        pl.Int8: "TINYINT", pl.Int16: "SMALLINT", pl.Int32: "INTEGER",
        pl.Int64: "BIGINT", pl.UInt64: "UBIGINT",
        pl.Float32: "FLOAT", pl.Float64: "DOUBLE",
        pl.Boolean: "BOOLEAN", pl.Date: "DATE",
        pl.Datetime: "TIMESTAMP", pl.String: "VARCHAR",
        pl.Utf8: "VARCHAR", pl.Categorical: "VARCHAR",
    }
    return mapping.get(tipo, "VARCHAR")


def _detectar_ufs(raw_dir: Path, uf_filter: list = None) -> list:
    ufs = set()
    for arq in raw_dir.glob("*.parquet"):
        match = re.match(r'^RD([A-Z]{2})\d+', arq.stem)
        if match:
            ufs.add(match.group(1))
    ufs = sorted(ufs)
    if uf_filter:
        ufs = [u for u in ufs if u in uf_filter]
    return ufs


def _listar_parquets_uf(raw_dir: Path, uf: str) -> list:
    """Lista todos os parquets de uma UF."""
    return sorted(raw_dir.glob(f"RD{uf}*.parquet"))


# ---------------------------------------------------------------------------
# Etapa: Criar schema no DuckDB
# ---------------------------------------------------------------------------

def criar_schema(con: duckdb.DuckDBPyConnection):
    """Cria todas as tabelas definidas em TABLE_SCHEMAS."""
    n = 0
    for nome in LOAD_ORDER:
        schema = TABLE_SCHEMAS.get(nome)
        if not schema:
            continue
        colunas_sql = []
        for col_name, col_type in schema["columns"].items():
            colunas_sql.append(f'"{col_name}" {_polars_to_duckdb(col_type)}')
        pk_cols = schema.get("primary_key", [])
        if pk_cols:
            pk_str = ", ".join([f'"{c}"' for c in pk_cols])
            colunas_sql.append(f"PRIMARY KEY ({pk_str})")
        sql = f'CREATE TABLE IF NOT EXISTS "{nome}" ({", ".join(colunas_sql)});'
        con.execute(sql)
        n += 1
    logger.info(f"  Schema: {n} tabelas criadas")


# ---------------------------------------------------------------------------
# Etapa: Carregar dimensões (CSVs → DuckDB)
# ---------------------------------------------------------------------------

def carregar_dimensoes(con: duckdb.DuckDBPyConnection):
    """Converte CSVs de apoio para DataFrames e insere nas tabelas de dimensão."""
    logger.info(f"\n  {'DIMENSÃO':<25s} {'REGISTROS':>10s}")
    logger.info(f"  {'-'*37}")

    for nome, csv_nome in Settings.SUPPORT_FILES.items():
        csv_path = Settings.SUPPORT_FILES_DIR / csv_nome

        # Pula arquivos auxiliares que não são tabelas (cadhosp, regsaud)
        if nome not in TABLE_SCHEMAS:
            continue
        if not csv_path.exists():
            logger.warning(f"  ERRO {nome} — {csv_nome} não encontrado")
            continue

        overrides = FORCE_STRING.get(nome, {})
        df = pl.read_csv(csv_path, infer_schema_length=10000,
                         encoding="utf8", schema_overrides=overrides)
        
        if nome in COLUMN_RENAME:
            rename_map = {k: v for k, v in COLUMN_RENAME[nome].items() if k in df.columns}
            df = df.rename(rename_map)

        # Injeta sentinelas (verifica PK antes de inserir)
        if nome in SENTINELAS:
            schema_def = TABLE_SCHEMAS.get(nome, {})
            pk_cols = schema_def.get("primary_key", [])
            for row in SENTINELAS[nome]:
                full_row = {c: row.get(c, None) for c in df.columns}
                # Verifica se o sentinela já existe via PK
                if pk_cols:
                    pk_col = pk_cols[0]
                    pk_val = full_row.get(pk_col)
                    if pk_val is not None and pk_col in df.columns:
                        existe = df.filter(pl.col(pk_col) == pk_val).height > 0
                        if existe:
                            continue
                df = pl.concat([df, pl.DataFrame([full_row])], how="vertical_relaxed")

        # Seleciona apenas colunas que existem na tabela DuckDB
        colunas_destino = [c for c in con.table(nome).columns if c in df.columns]
        df_load = df.select(colunas_destino)

        pk_cols = TABLE_SCHEMAS.get(nome, {}).get("primary_key", [])
        pk_ausentes = [c for c in pk_cols if c not in df_load.columns]
        if pk_ausentes:
            logger.warning(f"  {nome}: PK ausente no CSV {pk_ausentes} — colunas disponíveis: {df.columns[:10]}")
        for pk_col in [c for c in pk_cols if c in df_load.columns]:
            n_antes = len(df_load)
            df_load = df_load.filter(pl.col(pk_col).is_not_null())
            if df_load[pk_col].dtype in (pl.Utf8, pl.String, pl.Categorical):
                df_load = df_load.filter(pl.col(pk_col).str.strip_chars().str.len_chars() > 0)
            n_removidas = n_antes - len(df_load)
            if n_removidas > 0:
                logger.warning(f"  {nome}: {n_removidas} linha(s) com {pk_col} nulo removidas")
        con.execute(f'INSERT INTO "{nome}" BY NAME SELECT * FROM df_load')
        logger.info(f"  {nome:<25s} {len(df_load):>10,}")

        
        del df, df_load


# ---------------------------------------------------------------------------
# Etapa: Gerar dimensão tempo
# ---------------------------------------------------------------------------

def carregar_tempo(con: duckdb.DuckDBPyConnection):
    """Gera e insere a dimensão de tempo (1 dia por linha)."""
    df = pl.DataFrame({
        "data": pl.date_range(
            start=pl.date(Settings.ANOS_INICIO, 1, 1),
            end=pl.date(Settings.ANOS_FIM, 12, 31),
            interval="1d", eager=True,
        )
    }).with_columns([
        pl.col("data").dt.year().cast(pl.Int16).alias("ano"),
        pl.col("data").dt.month().cast(pl.Int8).alias("mes"),
        ((pl.col("data").dt.month() - 1) // 3 + 1).cast(pl.Int8).alias("trimestre"),
        pl.col("data").dt.weekday().cast(pl.Int8).alias("dia_semana"),
    ])
    con.execute('INSERT INTO "tempo" BY NAME SELECT * FROM df')
    logger.info(f"  {'tempo':<25s} {len(df):>10,}")
    del df


# ---------------------------------------------------------------------------
# Etapa: Processar uma UF (unify → preprocess → aggregate → insert)
# ---------------------------------------------------------------------------

CHUNK_SIZE = 32  # parquets por batch (~3M rows, ~1-2GB RAM)


ANO_FILTRO = 2008

def _preparar_lazy_frame(arq: Path) -> pl.LazyFrame:
    """Scan um parquet e normaliza colunas para COLUNAS_SIH."""
    lf = pl.scan_parquet(arq)
    schema = lf.collect_schema()
    colunas_presentes = list(schema.keys())
    for col in COLUNAS_SIH:
        if col not in colunas_presentes:
            lf = lf.with_columns(pl.lit(None).alias(col))
    lf = lf.select(COLUNAS_SIH)

    # DT_INTER é string "YYYYMMDD" — extrai os 4 primeiros caracteres como ano
    if "DT_INTER" in colunas_presentes:
        lf = lf.filter(pl.col("DT_INTER").str.slice(0, 4) >= str(ANO_FILTRO))

    return lf


def _processar_chunk(
    con: duckdb.DuckDBPyConnection,
    lazy_frames: list,
    id_offset: int,
) -> tuple:
    """Processa um batch de LazyFrames e insere no DuckDB.

    Insere dados pré-processados (sem agregar) na staging table.
    A agregação por N_AIH é feita depois no DuckDB, garantindo
    que AIHs que cruzam fronteiras de chunk sejam agregadas corretamente.

    Retorna: (n_rows_raw, n_atendimentos, df_hospital_chunk)
    """
    df_raw = pl.concat(lazy_frames, how="vertical_relaxed").collect()

    # Pré-processar
    preprocessor = SIHPreprocessor.__new__(SIHPreprocessor)
    df = preprocessor.tratar_chunk_completo(df_raw)
    del df_raw
    gc.collect()

    # internacao_procedimento (N_AIH × PROC_REA)
    df_atend = df.select(["N_AIH", "PROC_REA"]).with_columns(
        (pl.int_range(0, pl.len(), dtype=pl.UInt64) + id_offset).alias("id_atendimento")
    ).select(["id_atendimento", "N_AIH", "PROC_REA"])

    n_atend = len(df_atend)
    con.execute('INSERT INTO "internacao_procedimento" BY NAME SELECT * FROM df_atend')
    del df_atend

    # Hospital (unique por CNES neste chunk)
    cols_hosp_presentes = [c for c in COLUNAS_HOSPITAL if c in df.columns]
    df_hosp = df.select(cols_hosp_presentes).unique(subset=["CNES"])

    # INSERT raw na staging (sem agregação — DuckDB agrega depois)
    cols_internacoes = [c for c in COLUNAS_INTERNACOES if c in df.columns]
    df_staging = df.select(cols_internacoes)
    n_rows = len(df_staging)
    del df
    gc.collect()

    con.execute('INSERT INTO "_staging_internacoes" BY NAME SELECT * FROM df_staging')
    del df_staging
    gc.collect()

    return n_rows, n_atend, df_hosp


def _criar_staging(con: duckdb.DuckDBPyConnection):
    """Cria tabela staging temporária (sem PK) para receber dados brutos."""
    # Pega colunas de internacoes mas sem PRIMARY KEY
    schema = TABLE_SCHEMAS.get("internacoes")
    if not schema:
        raise RuntimeError("Schema 'internacoes' não encontrado")
    colunas_sql = []
    for col_name, col_type in schema["columns"].items():
        # Staging só precisa das colunas de COLUNAS_INTERNACOES
        if col_name in COLUNAS_INTERNACOES:
            colunas_sql.append(f'"{col_name}" {_polars_to_duckdb(col_type)}')
    con.execute(f'DROP TABLE IF EXISTS "_staging_internacoes"')
    con.execute(f'CREATE TABLE "_staging_internacoes" ({", ".join(colunas_sql)})')


def _agregar_staging(con: duckdb.DuckDBPyConnection) -> int:
    """Agrega staging por N_AIH e insere em internacoes. Retorna n_internacoes."""
    # Construir SQL de agregação — mesma lógica do Python mas no DuckDB
    cols_soma = set(COLUNAS_SOMA)
    cols_media = set(COLUNAS_MEDIA)
    cols_recalc = set(COLUNAS_RECALCULADAS)

    # Pegar colunas que existem na staging
    staging_cols = [row[0] for row in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = '_staging_internacoes'"
    ).fetchall()]

    agg_parts = []
    for col in staging_cols:
        if col == "N_AIH" or col in cols_recalc:
            continue
        if col in cols_soma:
            agg_parts.append(f'SUM("{col}") AS "{col}"')
        elif col in cols_media:
            agg_parts.append(f'AVG("{col}") AS "{col}"')
        else:
            agg_parts.append(f'FIRST("{col}") AS "{col}"')

    agg_sql = ", ".join(agg_parts)

    sql = f"""
    INSERT INTO "internacoes" BY NAME
    SELECT
        "N_AIH",
        {agg_sql},
        (SUM("VAL_SH") + SUM("VAL_SP") + SUM("VAL_UTI")) AS "VAL_TOT",
        FIRST("DIAS_PERM") AS "DIAS_PERM",
        FIRST("IDADE") AS "IDADE"
    FROM "_staging_internacoes"
    GROUP BY "N_AIH"
    """
    con.execute(sql)
    n = con.execute('SELECT COUNT(*) FROM "internacoes"').fetchone()[0]
    con.execute('DROP TABLE IF EXISTS "_staging_internacoes"')
    return n


def processar_uf(
    con: duckdb.DuckDBPyConnection,
    uf: str,
    raw_dir: Path,
    id_offset: int,
) -> tuple:
    """Processa todos os parquets de uma UF em chunks e insere no DuckDB.

    Estratégia semi-ELT:
      1. Chunks de CHUNK_SIZE parquets → pré-processa em Polars → INSERT na staging (sem PK)
      2. DuckDB agrega staging por N_AIH → INSERT em internacoes (com PK)
      3. Drop staging

    Isso garante que AIHs que cruzam fronteiras de chunk sejam
    agregadas corretamente, sem perda de dados.

    Retorna: (n_internacoes, n_atendimentos, df_hospital_uf)
    """
    arquivos = _listar_parquets_uf(raw_dir, uf)
    if not arquivos:
        logger.warning(f"  \u2717 {uf} — nenhum parquet encontrado")
        return 0, 0, None

    # Preparar lazy frames 
    lazy_frames = []
    for arq in arquivos:
        try:
            lazy_frames.append(_preparar_lazy_frame(arq))
        except Exception as e:
            logger.warning(f"    {arq.name}: {e}")

    if not lazy_frames:
        return 0, 0, None

    # Criar staging table (sem PK, sem agregação)
    _criar_staging(con)

    # Processar em chunks → staging
    total_raw = 0
    total_atend = 0
    hospitais_uf = []
    current_offset = id_offset

    for i in range(0, len(lazy_frames), CHUNK_SIZE):
        chunk = lazy_frames[i:i + CHUNK_SIZE]
        n_raw, n_atend, df_hosp = _processar_chunk(con, chunk, current_offset)
        total_raw += n_raw
        total_atend += n_atend
        current_offset += n_atend
        if df_hosp is not None:
            hospitais_uf.append(df_hosp)

    # Agregar staging → internacoes (no DuckDB, correto mesmo com AIHs cross-chunk)
    n_inter_before = con.execute('SELECT COUNT(*) FROM "internacoes"').fetchone()[0]
    _agregar_staging(con)
    n_inter_after = con.execute('SELECT COUNT(*) FROM "internacoes"').fetchone()[0]
    total_inter = n_inter_after - n_inter_before

    # Consolidar hospitais da UF
    df_hosp_uf = None
    if hospitais_uf:
        df_hosp_uf = pl.concat(hospitais_uf, how="vertical_relaxed").unique(subset=["CNES"])

    return total_inter, total_atend, df_hosp_uf


def carregar_hospital(con: duckdb.DuckDBPyConnection, hospitais: list):
    """Concatena hospitais de todas as UFs, deduplica por CNES, enriquece."""
    if not hospitais:
        logger.warning("  ERRO hospital — nenhum dado acumulado")
        return

    df = pl.concat(hospitais, how="vertical_relaxed")

    df_grouped = (
        df.group_by("CNES")
        .agg([pl.col(c).mode().first().alias(c) for c in df.columns if c != "CNES"])
        .sort("CNES")
    )

    # Enriquecimento com cadhosp
    cadhosp_path = Settings.get_support_file_path("cadhosp")
    if cadhosp_path.exists():
        cadhosp = pl.read_csv(cadhosp_path, infer_schema_length=10000,
                              encoding="utf8", schema_overrides={"CGC_HOSP": pl.String})
        col_razao = next((c for c in cadhosp.columns if "RAZAO" in c.upper() or "NOME" in c.upper()), None)
        col_cgc = next((c for c in cadhosp.columns if "CGC" in c.upper()), None)

        if col_razao and col_cgc:
            cadhosp = cadhosp.with_columns(
                pl.col(col_cgc).cast(pl.String).str.strip_chars()
                .str.replace_all(r"[^0-9]", "").str.pad_start(14, "0")
                .alias("CGC_HOSP_PAD")
            )
            cadhosp_dedup = (
                cadhosp.sort(col_cgc, descending=True)
                .unique(subset=["CGC_HOSP_PAD"], keep="first")
                .select(["CGC_HOSP_PAD", col_razao])
                .rename({col_razao: "NO_HOSPITAL", "CGC_HOSP_PAD": "CGC_HOSP"})
            )
            df_grouped = df_grouped.join(cadhosp_dedup, on="CGC_HOSP", how="left")
            n_match = df_grouped.filter(pl.col("NO_HOSPITAL").is_not_null()).height
            logger.info(f"    Nomes: {n_match}/{len(df_grouped)} hospitais com razão social")
        else:
            df_grouped = df_grouped.with_columns(pl.lit(None).cast(pl.String).alias("NO_HOSPITAL"))
    else:
        df_grouped = df_grouped.with_columns(pl.lit(None).cast(pl.String).alias("NO_HOSPITAL"))

    # Remove CGC_HOSP (era só ponte para o merge)
    if "CGC_HOSP" in df_grouped.columns:
        df_grouped = df_grouped.drop("CGC_HOSP")

    # Inserir no DuckDB
    colunas_destino = con.table("hospital").columns
    df_load = df_grouped.select([c for c in colunas_destino if c in df_grouped.columns])
    con.execute('INSERT INTO "hospital" BY NAME SELECT * FROM df_load')
    logger.info(f"  {'hospital':<25s} {len(df_load):>10,}")

    del df, df_grouped, df_load


# ---------------------------------------------------------------------------
# Etapa: Carregar socioeconômico 
# ---------------------------------------------------------------------------

def carregar_socioeconomico(con: duckdb.DuckDBPyConnection):
    """Carrega tabela socioeconômica se o parquet existir."""
    socio_path = Settings.PROCESSED_DIR / Settings.SOCIOECONOMICO_FILENAME
    if not socio_path.exists():
        # Tenta em interim também
        socio_path = Settings.INTERIM_DIR / Settings.SOCIOECONOMICO_FILENAME
    if not socio_path.exists():
        logger.info(f"  {'socioeconomico':<25s} {'(não encontrado)':>10s}")
        return

    df = pl.read_parquet(socio_path)
    colunas_destino = con.table("socioeconomico").columns
    df_load = df.select([c for c in colunas_destino if c in df.columns])
    con.execute('INSERT INTO "socioeconomico" BY NAME SELECT * FROM df_load')
    logger.info(f"  {'socioeconomico':<25s} {len(df_load):>10,}")
    del df, df_load


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def pipeline_carga():
    """Pipeline ELT incremental por UF."""
    inicio = time.time()

    Settings.create_directories()
    raw_dir = Settings.RAW_SIH_DIR
    db_path = Settings.DB_PATH

    # Detectar UFs disponíveis
    ufs = _detectar_ufs(raw_dir, uf_filter=Settings.UF_DEFAULT) # Filtra UFs se UF_FILTER estiver definido

    if not ufs:
        logger.error(f"Nenhum parquet encontrado em {raw_dir}")
        return

    if db_path.exists():
        db_path.unlink()

    

    con = duckdb.connect(str(db_path))

    try:
        logger.info("=================================== PIPELINE ETLT ===================================")
        logger.info(f"  Banco:   {db_path}")
        logger.info(f"  Origem:  {raw_dir}")
        logger.info(f"  UFs:     {', '.join(ufs)} ({len(ufs)})")

        # --- 1. Schema ---
        criar_schema(con)

        # --- 2. Dimensões ---
        carregar_dimensoes(con)
        carregar_tempo(con)

        # --- 3. Fatos — por UF ---
        logger.info(f"\n  {'UF':<5s} {'PARQUETS':>8s} {'INTERNAÇÕES':>12s} "
                     f"{'ATENDIM.':>12s} {'TEMPO':>7s}")
        logger.info(f"  {'-'*48}")

        total_inter = 0
        total_atend = 0
        hospitais_acum = []
        id_offset = 0

        for uf in ufs:
            t0 = time.time()
            n_parquets = len(_listar_parquets_uf(raw_dir, uf))

            n_inter, n_atend, df_hosp = processar_uf(con, uf, raw_dir, id_offset)

            dt = time.time() - t0

            logger.info(f"  {uf:<5s} {n_parquets:>8d} {n_inter:>12,} "
                         f"{n_atend:>12,} {dt:>6.1f}s")

            total_inter += n_inter
            total_atend += n_atend
            id_offset += n_atend

            if df_hosp is not None:
                hospitais_acum.append(df_hosp)

            gc.collect()

        # --- 4. Hospital (dedup global) ---
        logger.info(f"\n  Pós-processamento:")
        carregar_hospital(con, hospitais_acum)
        del hospitais_acum

        # --- 5. Socioeconômico ---
        carregar_socioeconomico(con)

        # --- Resumo ---
        tempo_total = time.time() - inicio
        total_linhas = 0
        for t in LOAD_ORDER:
            try:
                res = con.execute(f'SELECT count(*) FROM "{t}"').fetchone()
                total_linhas += res[0]
            except Exception:
                pass

        tamanho_mb = db_path.stat().st_size / (1024 * 1024)

        logger.info(f"\n{'='*50}")
        logger.info(f"CARGA CONCLUÍDA")
        logger.info(f"  UFs:     {len(ufs)}")
        logger.info(f"  Fatos:   {total_inter:,} internações | {total_atend:,} atendimentos")
        logger.info(f"  Total:   {total_linhas:,} linhas no banco")
        logger.info(f"  Banco:   {db_path}")
        logger.info(f"  Disco:   {tamanho_mb:.1f} MB")
        logger.info(f"  Tempo:   {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
        logger.info(f"{'='*50}")

    except Exception as e:
        logger.critical(f"Falha no pipeline: {e}", exc_info=True)
        raise
    finally:
        con.close()

    # --- dbt (fora do try — con já fechado, DuckDB libera o lock) ---
    dbt_dir = Path(__file__).parent.parent.parent / "dbt_sih"
    sihrd5_dir = Path(__file__).parent.parent.parent
    dbt_bin = Path(sys.executable).parent / "dbt"
    dbt_env = {**__import__("os").environ, "DBT_DUCKDB_PATH": str(db_path)}

    dbt_opts = [
        "--profiles-dir", str(dbt_dir.resolve()),
        "--project-dir",  str(dbt_dir.resolve()),
    ]

    # deps — instala pacotes apenas se necessário
    packages_dir = dbt_dir / "dbt_packages"
    if not packages_dir.exists() or not any(packages_dir.iterdir()):
        logger.info("  [dbt] deps — instalando pacotes...")
        subprocess.run([str(dbt_bin), "deps"] + dbt_opts,
                       check=True, cwd=sihrd5_dir, capture_output=True,
                       text=True, env=dbt_env)
    else:
        logger.info("  [dbt] deps — pacotes já instalados, pulando.")

    # T1 — baseline antes das correções
    logger.info("  [dbt] Aud1 — testando sources (baseline)...")
    result = subprocess.run(
        [str(dbt_bin), "test"] + dbt_opts + [
            "--select", "path:models/sources",
            "--exclude", "path:models/staging path:tests/aud2",
            "--store-failures"],
        check=False, cwd=sihrd5_dir, capture_output=True,
        text=True, env=dbt_env)
    for line in result.stdout.splitlines():
        if any(k in line for k in ["PASS", "FAIL", "ERROR", "Completed", "Done"]):
            logger.info(f"  [dbt Aud1] {line.strip()}")

    # seed — carrega tabelas auxiliares (ex: cid_manuais)
    logger.info("  [dbt] seed — carregando tabelas auxiliares...")
    result = subprocess.run(
        [str(dbt_bin), "seed"] + dbt_opts,
        check=False, cwd=sihrd5_dir, capture_output=True,
        text=True, env=dbt_env)
    for line in result.stdout.splitlines():
        logger.info(f"  [dbt seed] {line.strip()}")
    if result.returncode != 0:
        for line in result.stderr.splitlines():
            logger.error(f"  [dbt seed ERROR] {line.strip()}")
        raise RuntimeError("dbt seed falhou — verifique o log acima")

    # T2 — aplicar correções (só models)
    logger.info("  [dbt] T2 — aplicando correções...")
    result = subprocess.run(
        [str(dbt_bin), "run"] + dbt_opts + [
            "--select", "path:models/staging",
            "--exclude", "path:models/sources"],
        check=False, cwd=sihrd5_dir, capture_output=True,
        text=True, env=dbt_env)
    for line in result.stdout.splitlines():
        if any(k in line for k in ["OK", "ERROR", "Completed", "Done", "Failure", "Runtime"]):
            logger.info(f"  [dbt T2 run] {line.strip()}")
    if result.returncode != 0:
        raise RuntimeError("dbt run falhou — verifique o log acima")

    # T2 — testar após correções (staging + testes aud2)
    logger.info("  [dbt] Aud2 — testando staging...")
    result = subprocess.run(
        [str(dbt_bin), "test"] + dbt_opts + [
            "--select", "path:models/staging path:tests/aud2",
            "--store-failures"],
        check=False, cwd=sihrd5_dir, capture_output=True,
        text=True, env=dbt_env)
    for line in result.stdout.splitlines():
        if any(k in line for k in ["PASS", "FAIL", "ERROR", "Completed", "Done"]):
            logger.info(f"  [dbt Aud2 test] {line.strip()}")

    # --- Contagem final das tabelas após T2 (seeds + sentinelas incluídos) ---
    con_final = duckdb.connect(str(db_path), read_only=True)
    try:
        logger.info(f"\n  CONTAGEM FINAL DAS TABELAS (pós T2)")
        logger.info(f"  {'TABELA':<25s} {'REGISTROS':>10s}")
        logger.info(f"  {'-'*37}")
        for tabela in LOAD_ORDER:
            try:
                n = con_final.execute(f'SELECT COUNT(*) FROM "stg_{tabela}"').fetchone()[0]
                logger.info(f"  {'stg_' + tabela:<25s} {n:>10,}")
            except Exception:
                try:
                    n = con_final.execute(f'SELECT COUNT(*) FROM "{tabela}"').fetchone()[0]
                    logger.info(f"  {tabela:<25s} {n:>10,}")
                except Exception:
                    pass
    finally:
        con_final.close()

def main():
    pipeline_carga()


if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging("carga_elt")
    main()