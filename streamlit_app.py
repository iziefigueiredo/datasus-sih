"""
streamlit_app.py

Interface Streamlit para extração de recortes do banco analítico SIH/SUS
(DuckDB). Aplicação standalone — não depende do pacote de pipeline em src/.

Executar:
    streamlit run streamlit_app.py
"""

import io
import os
import tempfile
import zipfile
from datetime import date

import duckdb
import streamlit as st

DB_PATH = "sihrd6.duckdb"

ANO_MIN = 2008
ANO_MAX = 2024

INTERNACOES_COLUNAS = [
    "N_AIH", "CNES", "DT_INTER", "DT_SAIDA", "DIAS_PERM", "DIAR_ACOM",
    "CAR_INT", "ESPEC", "COMPLEX", "MARCA_UTI", "UTI_INT_TO",
    "IND_VDRL", "MORTE", "GESTRISCO",
    "DIAG_PRINC", "DIAG_SECUN", "CID_MORTE", "CID_NOTIF",
    "DIAGSEC1", "DIAGSEC2", "DIAGSEC3", "DIAGSEC4", "DIAGSEC5",
    "DIAGSEC6", "DIAGSEC7", "DIAGSEC8", "DIAGSEC9",
    "VAL_SH", "VAL_SP", "VAL_UTI", "VAL_TOT",
    "NASC", "IDADE", "COD_IDADE", "SEXO", "RACA_COR", "ETNIA",
    "NACIONAL", "INSTRU", "VINCPREV", "CBOR", "MUNIC_RES", "CEP",
    "NUM_FILHOS", "CONTRACEP1", "CONTRACEP2", "INSC_PN",
]

COLUNAS_PADRAO = [
    "N_AIH", "DT_INTER", "DT_SAIDA", "DIAS_PERM", "MUNIC_RES",
    "IDADE", "SEXO", "DIAG_PRINC", "VAL_TOT",
]

TABELAS_DOMINIO = {
    "municipios":     "Municípios (IBGE)",
    "cid":            "CID-10 (diagnósticos)",
    "procedimentos":  "Procedimentos (SIGTAP)",
    "complexidade":   "Complexidade assistencial",
    "especialidade":  "Especialidade do leito",
    "sexo":           "Sexo",
    "raca_cor":       "Raça/Cor",
    "etnia":          "Etnia indígena",
    "nacionalidade":  "Nacionalidade",
    "instrucao":      "Grau de instrução",
    "vincprev":       "Vínculo previdenciário",
    "contraceptivos": "Método contraceptivo",
    "car_int":        "Caráter da internação",
    "cbor":           "Ocupação (CBO)",
    "marca_uti":      "Tipo de UTI",
}


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data
def get_ufs() -> list[str]:
    con = get_connection()
    rows = con.execute(
        'SELECT DISTINCT "SG_UF" FROM "municipios" ORDER BY "SG_UF"'
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data
def get_municipios(uf: str) -> list[tuple[int, str]]:
    con = get_connection()
    return con.execute(
        'SELECT "CO_MUNICIPIO_6D", "NO_MUNICIPIO" FROM "municipios" '
        'WHERE "SG_UF" = ? ORDER BY "NO_MUNICIPIO"',
        [uf],
    ).fetchall()


@st.cache_data
def buscar_procedimentos(termo: str) -> list[tuple[str, str]]:
    """Busca por código (PROC_REA) ou nome (NOME_PROC), case-insensitive."""
    if len(termo.strip()) < 2:
        return []
    con = get_connection()
    like = f"%{termo.strip()}%"
    return con.execute(
        'SELECT "PROC_REA", "NOME_PROC" FROM "procedimentos" '
        'WHERE "PROC_REA" ILIKE ? OR "NOME_PROC" ILIKE ? '
        'ORDER BY "NOME_PROC" LIMIT 50',
        [like, like],
    ).fetchall()


def build_query(
    ano_ini: int,
    ano_fim: int,
    uf: str,
    municipios_sel: list[int],
    procedimentos_sel: list[str],
    colunas: list[str],
) -> tuple[str, str, list]:
    """Monta a projeção de colunas e a cláusula FROM/WHERE separadamente,
    para reaproveitar a mesma cláusula no SELECT de preview e no COUNT(*).
    Nenhum valor vindo do usuário entra via f-string — tudo parametrizado.
    """
    select_cols = [f'i."{c}"' for c in colunas] + ['m."NO_MUNICIPIO"', 'm."SG_UF"']

    from_sql = 'FROM "internacoes" i JOIN "municipios" m ON i."MUNIC_RES" = m."CO_MUNICIPIO_6D"'
    params: list = []

    if procedimentos_sel:
        placeholders = ", ".join("?" for _ in procedimentos_sel)
        from_sql += (
            ' JOIN "internacao_procedimento" ip'
            f' ON i."N_AIH" = ip."N_AIH" AND ip."PROC_REA" IN ({placeholders})'
        )
        params += procedimentos_sel

    where = ['i."DT_INTER" BETWEEN ? AND ?', 'm."SG_UF" = ?']
    params += [date(ano_ini, 1, 1), date(ano_fim, 12, 31), uf]

    if municipios_sel:
        placeholders = ", ".join("?" for _ in municipios_sel)
        where.append(f'i."MUNIC_RES" IN ({placeholders})')
        params += municipios_sel

    from_where_sql = f"{from_sql} WHERE {' AND '.join(where)}"
    return ", ".join(select_cols), from_where_sql, params


def export_bytes(con: duckdb.DuckDBPyConnection, sql: str, params: list, fmt: str) -> bytes:
    """Exporta via COPY — o DuckDB grava direto em disco, sem materializar
    o resultado inteiro em memória Python (pandas ou similar)."""
    suffix = ".csv" if fmt == "CSV" else ".parquet"
    copy_opts = "(FORMAT CSV, HEADER)" if fmt == "CSV" else "(FORMAT PARQUET)"

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp_path = tmp.name
    try:
        con.execute(f"COPY ({sql}) TO '{tmp_path}' {copy_opts}", params)
        with open(tmp_path, "rb") as f:
            return f.read()
    finally:
        os.unlink(tmp_path)


def render_extracao(con: duckdb.DuckDBPyConnection) -> None:
    st.header("Extrair Internações")

    col1, col2 = st.columns(2)
    ano_ini = col1.selectbox("Ano início", range(ANO_MIN, ANO_MAX + 1), index=0)
    ano_fim = col2.selectbox("Ano fim", range(ANO_MIN, ANO_MAX + 1), index=ANO_MAX - ANO_MIN)
    if ano_ini > ano_fim:
        st.error("Ano início não pode ser maior que ano fim.")
        return

    uf = st.selectbox("UF", get_ufs())
    municipios = get_municipios(uf) if uf else []
    municipios_por_label = {f"{nome} ({cod})": cod for cod, nome in municipios}
    municipios_escolhidos = st.multiselect(
        "Município (vazio = todos os municípios da UF)",
        options=list(municipios_por_label.keys()),
    )
    municipios_sel = [municipios_por_label[m] for m in municipios_escolhidos]

    termo_proc = st.text_input("Buscar procedimento por código ou nome — opcional")
    procedimentos_sel: list[str] = []
    if termo_proc:
        resultados = buscar_procedimentos(termo_proc)
        if not resultados:
            st.caption("Nenhum procedimento encontrado.")
        else:
            procedimentos_por_label = {f"{nome} ({cod})": cod for cod, nome in resultados}
            procedimentos_escolhidos = st.multiselect(
                "Selecione o(s) procedimento(s)",
                options=list(procedimentos_por_label.keys()),
            )
            procedimentos_sel = [procedimentos_por_label[p] for p in procedimentos_escolhidos]

    colunas = st.multiselect(
        "Colunas de internacoes a incluir",
        options=INTERNACOES_COLUNAS,
        default=COLUNAS_PADRAO,
    )
    if not colunas:
        st.warning("Selecione ao menos uma coluna.")
        return

    formato = st.radio("Formato de exportação", ["CSV", "Parquet"], horizontal=True)

    select_cols_sql, from_where_sql, params = build_query(
        ano_ini, ano_fim, uf, municipios_sel, procedimentos_sel, colunas
    )
    # DISTINCT: selecionar mais de um procedimento pode multiplicar a mesma
    # internação (uma linha por procedimento batido no JOIN).
    select_sql = f"SELECT DISTINCT {select_cols_sql} {from_where_sql}"
    count_sql = f'SELECT COUNT(DISTINCT i."N_AIH") {from_where_sql}'

    st.subheader("Preview (100 primeiras linhas)")
    st.dataframe(con.execute(select_sql + " LIMIT 100", params).arrow())

    total = con.execute(count_sql, params).fetchone()[0]
    st.metric("Linhas no recorte filtrado", f"{total:,}".replace(",", "."))

    if total == 0:
        return

    if st.button("Gerar arquivo para download"):
        with st.spinner("Gerando arquivo..."):
            dados = export_bytes(con, select_sql, params, formato)
        ext = "csv" if formato == "CSV" else "parquet"
        st.download_button(
            f"Baixar recorte completo (.{ext})",
            data=dados,
            file_name=f"internacoes_{ano_ini}_{ano_fim}_{uf}.{ext}",
            mime="text/csv" if formato == "CSV" else "application/octet-stream",
        )


def render_dominio(con: duckdb.DuckDBPyConnection) -> None:
    st.header("Tabelas de Domínio")
    st.caption("Tabelas de referência (código → descrição) do modelo SIH/SUS.")

    escolhidas = st.multiselect(
        "Tabelas para baixar",
        options=list(TABELAS_DOMINIO.keys()),
        format_func=lambda t: f"{t} — {TABELAS_DOMINIO[t]}",
    )
    if not escolhidas:
        return

    formato = st.radio(
        "Formato de exportação", ["CSV", "Parquet"], horizontal=True, key="formato_dominio"
    )

    preview_tabela = escolhidas[0]
    st.subheader(f"Preview — {preview_tabela}")
    st.dataframe(con.execute(f'SELECT * FROM "{preview_tabela}" LIMIT 100').arrow())

    if st.button("Gerar ZIP para download"):
        ext = "csv" if formato == "CSV" else "parquet"
        with st.spinner("Gerando arquivo..."):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for tabela in escolhidas:
                    dados = export_bytes(con, f'SELECT * FROM "{tabela}"', [], formato)
                    zf.writestr(f"{tabela}.{ext}", dados)
        st.download_button(
            "Baixar ZIP",
            data=buffer.getvalue(),
            file_name="tabelas_dominio.zip",
            mime="application/zip",
        )


def main() -> None:
    st.set_page_config(page_title="SIH/SUS — Extração de Dados", layout="wide")
    st.title("SIH/SUS — Extração de Dados")

    if not os.path.exists(DB_PATH):
        st.error(f"Banco não encontrado em '{DB_PATH}'. Ajuste a constante DB_PATH no topo do arquivo.")
        return

    con = get_connection()
    aba_internacoes, aba_dominio = st.tabs(["Extrair Internações", "Tabelas de Domínio"])
    with aba_internacoes:
        render_extracao(con)
    with aba_dominio:
        render_dominio(con)


if __name__ == "__main__":
    main()
