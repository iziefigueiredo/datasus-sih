"""Consultas DuckDB para o painel de extração de dados do SIH."""

import duckdb
import pandas as pd

from src.config.settings import Settings

# coluna codificada -> (tabela de domínio, coluna chave, coluna descrição)
DECODE_MAP = {
    "SEXO":      ("sexo", "SEXO", "DESCRICAO"),
    "RACA_COR":  ("raca_cor", "RACA_COR", "DESCRICAO"),
    "CAR_INT":   ("car_int", "CAR_INT", "DESCRICAO"),
    "ESPEC":     ("especialidade", "ESPEC", "DESCRICAO"),
    "COMPLEX":   ("complexidade", "COMPLEX", "DESCRICAO"),
    "MARCA_UTI": ("marca_uti", "MARCA_UTI", "DESCRICAO"),
}


def get_conn():
    """Conexão somente leitura -- não trava o banco enquanto o pipeline roda."""
    return duckdb.connect(str(Settings.DB_PATH), read_only=True)


def list_options(coluna: str) -> pd.DataFrame:
    """Popula um filtro a partir de uma dimensão do DECODE_MAP."""
    tabela, chave, desc = DECODE_MAP[coluna]
    query = f"SELECT {chave} AS codigo, {desc} AS descricao FROM {tabela} ORDER BY {desc}"
    with get_conn() as conn:
        return conn.execute(query).df()


def list_procedimentos() -> pd.DataFrame:
    query = "SELECT PROC_REA AS codigo, NOME_PROC AS nome FROM procedimentos ORDER BY NOME_PROC"
    with get_conn() as conn:
        return conn.execute(query).df()


def list_ufs() -> list:
    query = "SELECT DISTINCT SG_UF FROM municipios ORDER BY SG_UF"
    with get_conn() as conn:
        return conn.execute(query).df()["SG_UF"].tolist()


def list_cid_options(busca: str) -> pd.DataFrame:
    """Busca CIDs por trecho do código, descrição, grupo ou capítulo."""
    query = """
        SELECT CID AS codigo, DESCRICAO AS descricao
        FROM cid
        WHERE CID ILIKE ? OR DESCRICAO ILIKE ? OR DS_GRUPO ILIKE ? OR DS_CAPITULO ILIKE ?
        ORDER BY CID
    """
    termo = f"%{busca}%"
    with get_conn() as conn:
        return conn.execute(query, [termo, termo, termo, termo]).df()


def extrair_internacoes(procs, ufs, ano_ini, ano_fim, sexos, racas) -> pd.DataFrame:
    """
    Uma linha por internação x procedimento selecionado, já decodificada.
    Granularidade de fan-out: se a internação teve 2 procedimentos filtrados,
    aparece 2 vezes -- é o comportamento esperado da tabela ponte N:N.
    """
    filtros = ["i.DT_INTER BETWEEN ? AND ?"]
    params = [f"{ano_ini}-01-01", f"{ano_fim}-12-31"]

    filtros.append(f"proc.PROC_REA IN ({','.join(['?'] * len(procs))})")
    params += procs
    if ufs:
        filtros.append(f"mun.SG_UF IN ({','.join(['?'] * len(ufs))})")
        params += ufs
    if sexos:
        filtros.append(f"i.SEXO IN ({','.join(['?'] * len(sexos))})")
        params += sexos
    if racas:
        filtros.append(f"i.RACA_COR IN ({','.join(['?'] * len(racas))})")
        params += racas

    where = " AND ".join(filtros)
    query = f"""
        SELECT
            i.N_AIH, i.DT_INTER, i.DT_SAIDA, i.DIAS_PERM, i.IDADE,
            sexo.DESCRICAO   AS SEXO,
            raca.DESCRICAO   AS RACA_COR,
            car.DESCRICAO    AS CARATER_INTERNACAO,
            esp.DESCRICAO    AS ESPECIALIDADE,
            comp.DESCRICAO   AS COMPLEXIDADE,
            uti.DESCRICAO    AS MARCA_UTI,
            cid.DESCRICAO    AS DIAGNOSTICO_PRINCIPAL,
            mun.NO_MUNICIPIO AS MUNICIPIO_RESIDENCIA,
            mun.SG_UF,
            proc.NOME_PROC   AS PROCEDIMENTO,
            i.VAL_TOT, i.MORTE
        FROM internacoes i
        JOIN internacao_procedimento rl ON rl.N_AIH = i.N_AIH
        JOIN procedimentos proc         ON proc.PROC_REA = rl.PROC_REA
        LEFT JOIN sexo          sexo ON sexo.SEXO = i.SEXO
        LEFT JOIN raca_cor      raca ON raca.RACA_COR = i.RACA_COR
        LEFT JOIN car_int       car  ON car.CAR_INT = i.CAR_INT
        LEFT JOIN especialidade esp  ON esp.ESPEC = i.ESPEC
        LEFT JOIN complexidade  comp ON comp.COMPLEX = i.COMPLEX
        LEFT JOIN marca_uti     uti  ON uti.MARCA_UTI = i.MARCA_UTI
        LEFT JOIN cid           cid  ON cid.CID = i.DIAG_PRINC
        LEFT JOIN municipios    mun  ON mun.CO_MUNICIPIO_6D = i.MUNIC_RES
        WHERE {where}
    """
    with get_conn() as conn:
        return conn.execute(query, params).df()


def extrair_por_cid(cids, ufs, ano_ini, ano_fim, sexos) -> pd.DataFrame:
    """
    Réplica do desenho de Friedrich et al. (macrocosting de internações
    por CID-10, Rev Saude Publica 2026): agrega internações e custo total
    (VAL_TOT) por ano, UF, sexo e faixa etária para um grupo de CIDs --
    não entrega linha a linha, já sai pronta para análise de tendência.
    Faixas etárias replicam os cortes usados no artigo (0-18 / 19-59 / 60+).
    """
    filtros = ["i.DT_INTER BETWEEN ? AND ?"]
    params = [f"{ano_ini}-01-01", f"{ano_fim}-12-31"]

    filtros.append(f"i.DIAG_PRINC IN ({','.join(['?'] * len(cids))})")
    params += cids
    if ufs:
        filtros.append(f"mun.SG_UF IN ({','.join(['?'] * len(ufs))})")
        params += ufs
    if sexos:
        filtros.append(f"i.SEXO IN ({','.join(['?'] * len(sexos))})")
        params += sexos

    where = " AND ".join(filtros)
    query = f"""
        SELECT
            YEAR(i.DT_INTER) AS ANO,
            mun.SG_UF,
            sexo.DESCRICAO   AS SEXO,
            CASE
                WHEN i.IDADE < 19 THEN '0-18'
                WHEN i.IDADE BETWEEN 19 AND 59 THEN '19-59'
                ELSE '60+'
            END              AS FAIXA_ETARIA,
            cid.DESCRICAO    AS DIAGNOSTICO,
            COUNT(*)         AS INTERNACOES,
            SUM(i.VAL_TOT)   AS CUSTO_TOTAL,
            AVG(i.VAL_TOT)   AS CUSTO_MEDIO
        FROM internacoes i
        LEFT JOIN sexo       sexo ON sexo.SEXO = i.SEXO
        LEFT JOIN municipios mun  ON mun.CO_MUNICIPIO_6D = i.MUNIC_RES
        LEFT JOIN cid        cid  ON cid.CID = i.DIAG_PRINC
        WHERE {where}
        GROUP BY ANO, mun.SG_UF, sexo.DESCRICAO, FAIXA_ETARIA, cid.DESCRICAO
        ORDER BY ANO, mun.SG_UF
    """
    with get_conn() as conn:
        return conn.execute(query, params).df()


def extrair_socioeconomico(ufs, ano_ini, ano_fim) -> pd.DataFrame:
    filtros = ["s.NU_ANO BETWEEN ? AND ?"]
    params = [ano_ini, ano_fim]
    if ufs:
        filtros.append(f"mun.SG_UF IN ({','.join(['?'] * len(ufs))})")
        params += ufs
    where = " AND ".join(filtros)
    query = f"""
        SELECT mun.NO_MUNICIPIO AS MUNICIPIO, mun.SG_UF, s.*
        FROM socioeconomico s
        JOIN municipios mun ON mun.CO_MUNICIPIO_6D = s.CO_MUNICIPIO_6D
        WHERE {where}
    """
    with get_conn() as conn:
        return conn.execute(query, params).df()


def extrair_municipios(ufs) -> pd.DataFrame:
    with get_conn() as conn:
        if ufs:
            placeholders = ",".join(["?"] * len(ufs))
            return conn.execute(f"SELECT * FROM municipios WHERE SG_UF IN ({placeholders})", ufs).df()
        return conn.execute("SELECT * FROM municipios").df()
