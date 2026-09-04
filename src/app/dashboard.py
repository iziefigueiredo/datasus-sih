"""Painel Streamlit para extração de dados do SIH/DATASUS."""

import streamlit as st

from src.app.queries import (
    list_procedimentos, list_ufs, list_options,
    extrair_internacoes, extrair_socioeconomico, extrair_municipios,
)

st.set_page_config(page_title="SIH/DATASUS - Extração", layout="wide")
st.title("Extração de dados SIH/DATASUS")

dataset = st.sidebar.selectbox(
    "O que você quer extrair?",
    ["Internações (SIH)", "Socioeconômico", "Municípios"],
)
st.sidebar.subheader("Filtros")

if dataset == "Internações (SIH)":
    procs_df = list_procedimentos()
    sexo_df = list_options("SEXO")
    raca_df = list_options("RACA_COR")

    proc_nomes = st.sidebar.multiselect("Procedimento", procs_df["nome"])
    uf_sel = st.sidebar.multiselect("UF", list_ufs())
    ano_ini, ano_fim = st.sidebar.slider("Período", 2008, 2024, (2020, 2024))
    sexo_sel = st.sidebar.multiselect("Sexo", sexo_df["descricao"])
    raca_sel = st.sidebar.multiselect("Raça/cor", raca_df["descricao"])

    proc_cods = procs_df[procs_df["nome"].isin(proc_nomes)]["codigo"].tolist()
    sexo_cods = sexo_df[sexo_df["descricao"].isin(sexo_sel)]["codigo"].tolist()
    raca_cods = raca_df[raca_df["descricao"].isin(raca_sel)]["codigo"].tolist()

    pode_extrair = bool(proc_cods)
    if not pode_extrair:
        st.info("Selecione ao menos um procedimento para habilitar a extração.")

    if st.sidebar.button("Extrair", disabled=not pode_extrair):
        with st.spinner("Consultando o banco..."):
            st.session_state["resultado"] = extrair_internacoes(
                proc_cods, uf_sel, ano_ini, ano_fim, sexo_cods, raca_cods
            )

elif dataset == "Socioeconômico":
    uf_sel = st.sidebar.multiselect("UF", list_ufs())
    ano_ini, ano_fim = st.sidebar.slider("Período", 2008, 2024, (2020, 2024))
    if st.sidebar.button("Extrair"):
        with st.spinner("Consultando o banco..."):
            st.session_state["resultado"] = extrair_socioeconomico(uf_sel, ano_ini, ano_fim)

else:
    uf_sel = st.sidebar.multiselect("UF", list_ufs())
    if st.sidebar.button("Extrair"):
        with st.spinner("Consultando o banco..."):
            st.session_state["resultado"] = extrair_municipios(uf_sel)

if "resultado" in st.session_state:
    resultado = st.session_state["resultado"]
    st.write(f"{len(resultado):,} linhas extraídas".replace(",", "."))
    st.dataframe(resultado.head(1000))
    st.download_button(
        "Baixar CSV",
        resultado.to_csv(index=False).encode("utf-8-sig"),
        file_name="extracao_sih.csv",
        mime="text/csv",
    )
