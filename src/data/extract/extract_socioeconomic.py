"""
Extração de Indicadores Socioeconômicos Municipais
Localização: sihrd5/src/data/extract_socioeconomic.py

Consolida os 5 indicadores que alimentam TF_SOCIOECONOMICO:
    1. populacao       — IBGE SIDRA (API, tabela 6579)
    2. leitos          — CNES/LT (download automático via pysus se necessário)
    3. medicos         — CNES/PF (download automático via pysus se necessário)
    4. mort_infantil   — SIM + SINASC (download automático via pysus)
    5. pib             — IBGE SIDRA (API, tabela 5938)

Uso:
    python extract_socioeconomic.py                  # todos os indicadores
    python extract_socioeconomic.py --indicador populacao
    python extract_socioeconomic.py --indicador leitos medicos
    python extract_socioeconomic.py --uf RS
    python extract_socioeconomic.py --brasil
    python extract_socioeconomic.py --csv
"""

import argparse
import sys
import time
import requests
from pathlib import Path
import ipeadatapy as ip

import pandas as pd
from tqdm import tqdm

SRC_DIR = Path(__file__).parent.parent.parent  # src/
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

# ─────────────────────────────────────────────────────────────────────
# Constantes compartilhadas
# ─────────────────────────────────────────────────────────────────────

UF_PARA_CODIGO = {
    "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15",
    "AP": "16", "TO": "17", "MA": "21", "PI": "22", "CE": "23",
    "RN": "24", "PB": "25", "PE": "26", "AL": "27", "SE": "28",
    "BA": "29", "MG": "31", "ES": "32", "RJ": "33", "SP": "35",
    "PR": "41", "SC": "42", "RS": "43", "MS": "50", "MT": "51",
    "GO": "52", "DF": "53",
}

CODLEITO_PSIQUIATRICO  = "33"
# CBO-2002: família de médicos mudou de codificação ao longo do tempo:
#   2007-~2010: família 2231 (Médicos)
#   ~2010+:     família 225  (Médicos clínicos, cirurgiões, diagnóstico, etc.)
# Ambos os prefixos são válidos e devem ser aceitos em qualquer ano.
CBO2002_MEDICOS_PREFIXO = ("2231", "225")
CBO1994_MEDICOS_PREFIXO = ("22",)
ANO_MUDANCA_CBO = 2007
MES_MUDANCA_CBO = 8

SIDRA_POP_URL = "https://apisidra.ibge.gov.br/values/t/6579/n6/all/v/9324/p/{ano}"
SIDRA_PIB_URL = "https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/37/p/{ano}"

INDICADORES_DISPONIVEIS = ["populacao", "leitos", "medicos", "mort_infantil", "pib", "ipea"]

IPEA_SERIES = {
    "ANS_BNFPLSAUDEUF":      ("QT_BENEFICIARIOS_PLANO_SAUDE", "Int64"),
    "SIS_ESTABINTSUSUF":     ("QT_ESTAB_INTERNACAO_SUS",      "Int64"),
    "SIS_ESTABSAUDEUF":      ("QT_ESTAB_SAUDE",               "Int64"),
    "SIS_ESTABURGSUSUF":     ("QT_ESTAB_URGENCIA_SUS",        "Int64"),
    "SIS_NMENF1000HUF":      ("VL_ENFERMEIROS_1000",          "float64"),
    "SIS_NMTEC1000HUF":      ("VL_TECNICOS_SAUDE_1000",       "float64"),
    "SIS_NMLTCMPSUS1000HUF": ("VL_LEITOS_UTI_SUS_1000",       "float64"),
}

# ─────────────────────────────────────────────────────────────────────
# Helpers compartilhados
# ─────────────────────────────────────────────────────────────────────

def resolver_ufs(ufs_input):
    """Converte siglas ou códigos para lista de códigos IBGE 2 dígitos."""
    codigos = []
    for uf in ufs_input:
        uf = str(uf).upper().strip()
        if uf in UF_PARA_CODIGO:
            codigos.append(UF_PARA_CODIGO[uf])
        elif uf.isdigit() and len(uf) <= 2:
            codigos.append(uf.zfill(2))
        else:
            print(f"  AVISO: UF '{uf}' não reconhecida — ignorada.")
    return codigos


def carregar_municipios():
    """Carrega tabela de municípios com CO_MUNICIPIO_6D, CO_MUNICIPIO_7D, NO_MUNICIPIO, SG_UF."""
    path = Settings.get_support_file_path("municipios")
    df = pd.read_csv(path, encoding="utf-8", dtype=str)
    # Compatibilidade: aceita tanto formato antigo (codigo_6d...) quanto novo (CO_MUNICIPIO_6D...)
    rename_map = {
        "codigo_6d":    "CO_MUNICIPIO_6D",
        "codigo_ibge":  "CO_MUNICIPIO_7D",
        "nome":         "NO_MUNICIPIO",
        "estado":       "SG_UF",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["CO_MUNICIPIO_6D"] = df["CO_MUNICIPIO_6D"].str.zfill(6)
    df["CO_MUNICIPIO_7D"] = df["CO_MUNICIPIO_7D"].str.zfill(7)
    return df[["CO_MUNICIPIO_6D", "CO_MUNICIPIO_7D", "NO_MUNICIPIO", "SG_UF"]]


def _descobrir_parquet(raw_dir, prefixo, uf_sigla, ano, mes):
    """Tenta encontrar parquet DATASUS para UF/ano/mês.

    Nota: pysus pode criar o resultado como arquivo .parquet OU como
    diretório .parquet/ contendo partições dentro. Ambos são tratados.
    Retorna o diretório inteiro quando particionado — pandas lê todas
    as partições automaticamente via pd.read_parquet(dir).
    """
    nome = f"{prefixo}{uf_sigla.upper()}{str(ano)[-2:]}{str(mes).zfill(2)}.parquet"
    path = raw_dir / nome
    if path.is_file():
        return path
    if path.is_dir():
        # Retorna o diretório — pd.read_parquet(dir) lê todas as partições.
        # NÃO retornar inner[0] pois isso perde 80%+ dos dados.
        return path
    return None


def _salvar(df, nome_base, escopo_label, ano_inicio, ano_fim, csv):
    """Salva resultado em parquet ou CSV e imprime resumo."""
    Settings.create_directories()
    print(f"\n  Municípios: {df.iloc[:, 0].nunique():,} | Registros: {len(df):,}")
    print(df.head(3).to_string(index=False))

    if csv:
        out = Settings.PROCESSED_DIR / f"{nome_base}_{escopo_label}_{ano_inicio}_{ano_fim}.csv"
        df.to_csv(out, index=False, sep=";", decimal=",")
    else:
        out = Settings.PROCESSED_DIR / f"{nome_base}.parquet"
        df.to_parquet(out, index=False)
    print(f"  Salvo: {out}")


def _ler_parquet_seguro(path):
    """Lê parquet que pode ser arquivo ou diretório.

    O pysus em algumas versões cria um diretório chamado 'XXXX.parquet/'
    contendo múltiplas partições (ex: 5 arquivos de 30k rows cada).
    Esta função lê TODAS as partições via pd.read_parquet(dir).
    """
    if path.is_file():
        return pd.read_parquet(path)
    if path.is_dir():
        # pd.read_parquet(dir) com pyarrow lê todas as partições
        try:
            return pd.read_parquet(path)
        except Exception:
            # Fallback: concatenar partições manualmente
            inner = sorted(path.glob("*.parquet"))
            if inner:
                frames = [pd.read_parquet(f) for f in inner]
                return pd.concat(frames, ignore_index=True)
            return None
    return None


# ─────────────────────────────────────────────────────────────────────
# 1. POPULAÇÃO — IBGE SIDRA tabela 6579
# ─────────────────────────────────────────────────────────────────────

def _sidra_ano(ano, retries=3, espera=5):
    url = SIDRA_POP_URL.format(ano=ano)
    for tentativa in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            registros = []
            for row in resp.json()[1:]:
                cod = row.get("D1C", "").strip()
                val = row.get("V", "").strip()
                if not cod or not val or val in ("-", "...", ""):
                    continue
                try:
                    registros.append({
                        "CO_MUNICIPIO_7D": cod.zfill(7),
                        "NU_ANO": ano,
                        "VL_POPULACAO": int(val.replace(".", "").replace(",", "")),
                    })
                except ValueError:
                    continue
            return registros
        except requests.exceptions.RequestException as e:
            print(f"  Tentativa {tentativa}/{retries} falhou ({ano}): {e}")
            if tentativa < retries:
                time.sleep(espera)
    return []


def extrair_populacao(codigos_uf, anos, escopo_label, csv=False):
    """Extrai estimativas populacionais municipais (SIDRA t/6579)."""
    print("\n=== POPULAÇÃO (SIDRA t/6579) ===")
    anos_ok = [a for a in anos if a >= 2001]
    if len(anos_ok) < len(anos):
        print("  AVISO: SIDRA cobre a partir de 2001.")

    frames = []
    anos_sem_dados = []
    for ano in tqdm(anos_ok, desc="Consultando SIDRA"):
        registros = _sidra_ano(ano)
        if registros:
            df = pd.DataFrame(registros)
            if codigos_uf:
                df = df[df["CO_MUNICIPIO_7D"].str[:2].isin(codigos_uf)]
            if not df.empty:
                frames.append(df)
            else:
                anos_sem_dados.append(ano)
        else:
            anos_sem_dados.append(ano)
        time.sleep(0.5)

    if anos_sem_dados:
        print(f"  SIDRA: sem estimativa para {_formatar_faixas(anos_sem_dados)} "
              f"(anos censitários ou ainda não publicados)")

    if not frames:
        print("  ERRO: nenhum dado obtido da API SIDRA.")
        return

    resultado = pd.concat(frames, ignore_index=True)
    resultado["CO_MUNICIPIO_6D"] = resultado["CO_MUNICIPIO_7D"].str[:6]
    mun = carregar_municipios()
    resultado = (resultado
                 .merge(mun[["CO_MUNICIPIO_7D", "NO_MUNICIPIO", "SG_UF"]], on="CO_MUNICIPIO_7D", how="left")
                 [["CO_MUNICIPIO_6D", "CO_MUNICIPIO_7D", "NO_MUNICIPIO", "SG_UF", "NU_ANO", "VL_POPULACAO"]]
                 .sort_values(["CO_MUNICIPIO_6D", "NU_ANO"]).reset_index(drop=True))

    _salvar(resultado, "populacao", escopo_label, min(anos), max(anos), csv)


# ─────────────────────────────────────────────────────────────────────
# CNES — download automático se parquets não existirem
# ─────────────────────────────────────────────────────────────────────

def _garantir_cnes(grupo, raw_dir, ufs_siglas, anos):
    """
    Garante que os parquets CNES existam em raw_dir.
    Download paralelo via subprocess isolado (datasus_fetch_parallel).

    Após o download, verifica gaps (UFs/anos faltantes) e re-baixa
    automaticamente até MAX_REPAIR tentativas. Isso cobre falhas
    transitórias de rede/FTP que são comuns no DATASUS.
    """
    MAX_REPAIR = 3          # tentativas de reparo de gaps
    ESPERADO_POR_ANO = 12   # meses por UF/ano

    raw_dir.mkdir(parents=True, exist_ok=True)
    prefixo = "LT" if grupo == "LT" else "PF"

    def _contar_gaps():
        """Retorna lista de (uf, ano) faltantes."""
        existentes = {f.stem for f in raw_dir.glob("*.parquet")}
        gaps = []
        for uf in ufs_siglas:
            for ano in anos:
                yy = str(ano)[-2:]
                n_meses = sum(
                    1 for stem in existentes
                    if stem.startswith(f"{prefixo}{uf.upper()}{yy}")
                )
                if n_meses < ESPERADO_POR_ANO:
                    gaps.append((uf, ano))
        return gaps

    # ── Verificar se já temos tudo ──
    existentes = list(raw_dir.glob("*.parquet"))
    if existentes:
        gaps = _contar_gaps()
        if not gaps:
            print(f"  CNES/{grupo}: {len(existentes)} arquivos presentes — "
                  f"completo, pulando download.")
            return

        n_completos = len(ufs_siglas) * len(anos) - len(gaps)
        total_tasks = len(ufs_siglas) * len(anos)
        pct = n_completos / total_tasks * 100

        if pct < 50:
            print(f"  CNES/{grupo}: download anterior muito incompleto "
                  f"({pct:.0f}%) — limpando e re-baixando...")
            import shutil
            for f in existentes:
                if f.is_dir():
                    shutil.rmtree(f)
                else:
                    f.unlink()
            gaps = [(uf, ano) for uf in ufs_siglas for ano in anos]
        else:
            print(f"  CNES/{grupo}: {len(existentes)} arquivos presentes, "
                  f"{len(gaps)} UF/anos incompletos — re-baixando gaps...")
    else:
        print(f"  CNES/{grupo}: nenhum arquivo — baixando...")
        gaps = [(uf, ano) for uf in ufs_siglas for ano in anos]

    from data.extract.datasus_fetch_parallel import datasus_fetch_parallel

    # ── Download inicial ──
    ufs_gap = sorted(set(uf for uf, _ in gaps))
    anos_gap = sorted(set(ano for _, ano in gaps))

    stats = datasus_fetch_parallel(
        sistema="CNES",
        grupo=grupo,
        ufs=ufs_gap,
        anos=anos_gap,
        destino=raw_dir,
        max_workers=3,
    )

    # ── Retry automático de gaps ──
    for tentativa in range(1, MAX_REPAIR + 1):
        gaps = _contar_gaps()
        if not gaps:
            break

        gaps_por_uf = {}
        for uf, ano in gaps:
            gaps_por_uf.setdefault(uf, []).append(ano)

        n_gaps = len(gaps)
        ufs_com_gap = sorted(gaps_por_uf.keys())
        print(f"\n  CNES/{grupo}: reparo {tentativa}/{MAX_REPAIR} — "
              f"{n_gaps} UF/anos faltantes em {len(ufs_com_gap)} UFs: "
              f"{', '.join(ufs_com_gap)}")

        ufs_retry = sorted(set(uf for uf, _ in gaps))
        anos_retry = sorted(set(ano for _, ano in gaps))

        stats_retry = datasus_fetch_parallel(
            sistema="CNES",
            grupo=grupo,
            ufs=ufs_retry,
            anos=anos_retry,
            destino=raw_dir,
            max_workers=3,
            timeout=300,
        )

        stats["baixados"] += stats_retry["baixados"]
        stats["falhas"] += stats_retry["falhas"]

    # ── Diagnóstico final ──
    gaps_final = _contar_gaps()
    total_arqs = len(list(raw_dir.glob("*.parquet")))
    total_tasks = len(ufs_siglas) * len(anos)
    completos = total_tasks - len(gaps_final)

    print(f"\n  CNES/{grupo}: {total_arqs} arquivos | "
          f"{completos}/{total_tasks} UF/anos completos "
          f"({completos/total_tasks*100:.0f}%)")

    if gaps_final:
        gaps_por_uf = {}
        for uf, ano in gaps_final:
            gaps_por_uf.setdefault(uf, []).append(ano)
        for uf in sorted(gaps_por_uf):
            anos_falta = _formatar_faixas(sorted(gaps_por_uf[uf]))
            print(f"    ⚠️ {uf}: faltam anos {anos_falta}")
        print(f"  (gaps restantes podem ser indisponibilidade real do "
              f"servidor — re-rode etapa 3 pra tentar novamente)")


def _formatar_faixas(anos):
    """Formata lista de anos em faixas legíveis: [2008,2009,2010,2018] → '2008-2010, 2018'"""
    if not anos:
        return ""
    anos = sorted(anos)
    faixas = []
    inicio = fim = anos[0]
    for a in anos[1:]:
        if a == fim + 1:
            fim = a
        else:
            faixas.append(f"{inicio}-{fim}" if fim > inicio else str(inicio))
            inicio = fim = a
    faixas.append(f"{inicio}-{fim}" if fim > inicio else str(inicio))
    return ", ".join(faixas)


# ─────────────────────────────────────────────────────────────────────
# 2. LEITOS — CNES/LT
# ─────────────────────────────────────────────────────────────────────

def _leitos_ano(raw_dir, ufs_siglas, ano, excluir_psiq):
    frames = []
    for uf in ufs_siglas:
        arq = mes_ref = None
        for mes in range(12, 0, -1):
            arq = _descobrir_parquet(raw_dir, "LT", uf, ano, mes)
            if arq:
                mes_ref = mes
                break
        if arq is None:
            continue
        try:
            df = pd.read_parquet(arq, columns=["CODUFMUN", "QT_SUS", "CODLEITO"])
        except Exception:
            try:
                df = pd.read_parquet(arq, columns=["CODUFMUN", "QT_SUS"])
                df["CODLEITO"] = ""
            except Exception:
                # Fallback: diretório .parquet (comportamento pysus)
                try:
                    df = _ler_parquet_seguro(arq)
                    if df is None:
                        continue
                    if "CODLEITO" not in df.columns:
                        df["CODLEITO"] = ""
                    df = df[["CODUFMUN", "QT_SUS", "CODLEITO"]]
                except Exception as e:
                    print(f"  Erro {arq}: {e}")
                    continue
        df["CODUFMUN"] = df["CODUFMUN"].astype(str).str.strip().str.zfill(6)
        df["QT_SUS"]   = pd.to_numeric(df["QT_SUS"], errors="coerce").fillna(0)
        df["CODLEITO"] = df["CODLEITO"].astype(str).str.strip()
        if excluir_psiq:
            df = df[df["CODLEITO"] != CODLEITO_PSIQUIATRICO]
        agg = df.groupby("CODUFMUN", as_index=False)["QT_SUS"].sum()
        agg[["NU_ANO", "NU_MES_REF"]] = ano, mes_ref
        frames.append(agg)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def extrair_leitos(codigos_uf, ufs_siglas, anos, escopo_label, csv=False, incluir_psiq=False):
    """Extrai leitos SUS por município/ano (CNES/LT)."""
    print("\n=== LEITOS SUS (CNES/LT) ===")
    _garantir_cnes("LT", Settings.RAW_CNES_LT_DIR, ufs_siglas, anos)

    anos_com_dados = []
    anos_sem_dados = []
    frames = []
    for ano in tqdm(anos, desc="Anos"):
        df = _leitos_ano(Settings.RAW_CNES_LT_DIR, ufs_siglas, ano, not incluir_psiq)
        if not df.empty:
            frames.append(df)
            anos_com_dados.append(ano)
        else:
            anos_sem_dados.append(ano)

    if anos_sem_dados:
        print(f"  CNES/LT: sem dados para {_formatar_faixas(anos_sem_dados)}")

    if not frames:
        print("  ERRO: nenhum dado CNES/LT encontrado para nenhum ano.")
        return

    leitos = pd.concat(frames, ignore_index=True)
    if codigos_uf:
        leitos = leitos[leitos["CODUFMUN"].str[:2].isin(codigos_uf)]

    pop_path = Settings.PROCESSED_DIR / "populacao.parquet"
    if not pop_path.exists():
        print("  ERRO: populacao.parquet não encontrado. Execute --indicador populacao primeiro.")
        return

    pop = pd.read_parquet(pop_path, columns=["CO_MUNICIPIO_6D", "NU_ANO", "VL_POPULACAO"])
    leitos = leitos.rename(columns={"CODUFMUN": "CO_MUNICIPIO_6D", "QT_SUS": "QT_LEITOS_SUS"})
    res = leitos.merge(pop, on=["CO_MUNICIPIO_6D", "NU_ANO"], how="left")
    res["VL_LEITOS_SUS_1000"] = res.apply(
        lambda r: round(r["QT_LEITOS_SUS"] / r["VL_POPULACAO"] * 1000, 4)
        if pd.notna(r["VL_POPULACAO"]) and r["VL_POPULACAO"] > 0 else None, axis=1
    )
    mun = carregar_municipios()
    res = (res.merge(mun[["CO_MUNICIPIO_6D", "NO_MUNICIPIO", "SG_UF"]].drop_duplicates("CO_MUNICIPIO_6D"),
                     on="CO_MUNICIPIO_6D", how="left")
           [["CO_MUNICIPIO_6D", "NO_MUNICIPIO", "SG_UF", "NU_ANO", "NU_MES_REF",
             "QT_LEITOS_SUS", "VL_POPULACAO", "VL_LEITOS_SUS_1000"]]
           .sort_values(["CO_MUNICIPIO_6D", "NU_ANO"]).reset_index(drop=True))

    _salvar(res, "leitos", escopo_label, min(anos), max(anos), csv)


# ─────────────────────────────────────────────────────────────────────
# 3. MÉDICOS — CNES/PF
# ─────────────────────────────────────────────────────────────────────

def _medicos_ano(raw_dir, ufs_siglas, ano):
    frames = []
    for uf in ufs_siglas:
        arq = mes_ref = None
        for mes in range(12, 0, -1):
            arq = _descobrir_parquet(raw_dir, "PF", uf, ano, mes)
            if arq:
                mes_ref = mes
                break
        if arq is None:
            continue
        prefixo = CBO2002_MEDICOS_PREFIXO if (ano, mes_ref) >= (ANO_MUDANCA_CBO, MES_MUDANCA_CBO) else CBO1994_MEDICOS_PREFIXO
        # CNES/PF mudou nome da coluna: arquivos antigos usam "CBO",
        # versões mais recentes podem usar "CBO_OCUPACAO".
        # Tenta CBO_OCUPACAO primeiro; se falhar, tenta CBO.
        try:
            try:
                df = pd.read_parquet(arq, columns=["CODUFMUN", "CBO_OCUPACAO", "CPF_PROF"])
            except (KeyError, Exception):
                df = pd.read_parquet(arq, columns=["CODUFMUN", "CBO", "CPF_PROF"])
                df = df.rename(columns={"CBO": "CBO_OCUPACAO"})
        except Exception:
            # Fallback: ler via _ler_parquet_seguro (diretórios .parquet)
            try:
                df_full = _ler_parquet_seguro(arq)
                if df_full is None:
                    continue
                col_cbo = "CBO_OCUPACAO" if "CBO_OCUPACAO" in df_full.columns else "CBO"
                df = df_full[["CODUFMUN", col_cbo, "CPF_PROF"]].copy()
                df = df.rename(columns={col_cbo: "CBO_OCUPACAO"})
            except Exception as e:
                print(f"  Erro {arq}: {e}")
                continue
        df["CODUFMUN"]     = df["CODUFMUN"].astype(str).str.strip().str.zfill(6)
        df["CBO_OCUPACAO"] = df["CBO_OCUPACAO"].astype(str).str.strip()
        df["CPF_PROF"]     = df["CPF_PROF"].astype(str).str.strip()
        dedup = (df[df["CBO_OCUPACAO"].apply(lambda x: x.startswith(prefixo))]
                 .dropna(subset=["CPF_PROF"])
                 .query("CPF_PROF != '' and CPF_PROF != 'nan'")
                 .drop_duplicates(subset=["CODUFMUN", "CPF_PROF"]))
        if dedup.empty:
            continue
        agg = dedup.groupby("CODUFMUN", as_index=False).size().rename(columns={"size": "QT_MEDICOS"})
        agg[["NU_ANO", "NU_MES_REF"]] = ano, mes_ref
        frames.append(agg)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def extrair_medicos(codigos_uf, ufs_siglas, anos, escopo_label, csv=False):
    """Extrai médicos únicos por município/ano (CNES/PF)."""
    print("\n=== MÉDICOS (CNES/PF) ===")
    _garantir_cnes("PF", Settings.RAW_CNES_PF_DIR, ufs_siglas, anos)

    anos_com_dados = []
    anos_sem_dados = []
    frames = []
    for ano in tqdm(anos, desc="Anos"):
        df = _medicos_ano(Settings.RAW_CNES_PF_DIR, ufs_siglas, ano)
        if not df.empty:
            frames.append(df)
            anos_com_dados.append(ano)
        else:
            anos_sem_dados.append(ano)

    if anos_sem_dados:
        print(f"  CNES/PF: sem dados para {_formatar_faixas(anos_sem_dados)}")

    if not frames:
        print("  ERRO: nenhum dado CNES/PF encontrado para nenhum ano.")
        return

    medicos = pd.concat(frames, ignore_index=True)
    if codigos_uf:
        medicos = medicos[medicos["CODUFMUN"].str[:2].isin(codigos_uf)]

    pop_path = Settings.PROCESSED_DIR / "populacao.parquet"
    if not pop_path.exists():
        print("  ERRO: populacao.parquet não encontrado. Execute --indicador populacao primeiro.")
        return

    pop = pd.read_parquet(pop_path, columns=["CO_MUNICIPIO_6D", "NU_ANO", "VL_POPULACAO"])
    medicos = medicos.rename(columns={"CODUFMUN": "CO_MUNICIPIO_6D"})
    res = medicos.merge(pop, on=["CO_MUNICIPIO_6D", "NU_ANO"], how="left")
    res["VL_MEDICOS_1000"] = res.apply(
        lambda r: round(r["QT_MEDICOS"] / r["VL_POPULACAO"] * 1000, 4)
        if pd.notna(r["VL_POPULACAO"]) and r["VL_POPULACAO"] > 0 else None, axis=1
    )
    mun = carregar_municipios()
    res = (res.merge(mun[["CO_MUNICIPIO_6D", "NO_MUNICIPIO", "SG_UF"]].drop_duplicates("CO_MUNICIPIO_6D"),
                     on="CO_MUNICIPIO_6D", how="left")
           [["CO_MUNICIPIO_6D", "NO_MUNICIPIO", "SG_UF", "NU_ANO", "NU_MES_REF",
             "QT_MEDICOS", "VL_POPULACAO", "VL_MEDICOS_1000"]]
           .sort_values(["CO_MUNICIPIO_6D", "NU_ANO"]).reset_index(drop=True))

    _salvar(res, "medicos", escopo_label, min(anos), max(anos), csv)


# ─────────────────────────────────────────────────────────────────────
# 4. MORTALIDADE INFANTIL — SIM + SINASC
# ─────────────────────────────────────────────────────────────────────

def _eh_menor_1_ano(idade_str):
    if pd.isna(idade_str) or str(idade_str).strip() == "":
        return False
    return str(idade_str)[0] in ("0", "1", "2", "3")


def _baixar_sim(ufs_siglas, anos):
    import socket, shutil
    from pathlib import Path
    from pysus.online_data.SIM import SIM
    print("  Baixando SIM (óbitos)...")

    # Limpa cache do pysus para forçar re-download.
    # Motivo: pysus cacheia em ~/.pysus/ e, quando o arquivo já existe,
    # sim.download() pula silenciosamente sem retornar pro processamento.
    # Também limpa diretórios .parquet/ que o pysus cria 
    pysus_cache = Path.home() / ".pysus"
    if pysus_cache.exists():
        for f in pysus_cache.glob("DO*"):
            if f.is_dir():
                shutil.rmtree(f, ignore_errors=True)
            else:
                f.unlink(missing_ok=True)

    sim = SIM().load()
    raw_dir = Settings.RAW_SIM_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(120)
    try:
        for uf in tqdm(ufs_siglas, desc="SIM"):
            try:
                arquivos = sim.get_files("CID10", uf=uf, year=anos)
                if not arquivos:
                    print(f"  SIM: nenhum arquivo para {uf}")
                    continue
                print(f"  SIM/{uf}: {len(arquivos)} arquivos encontrados")

                # Fase 1: Download de todos os arquivos
                # Ignora retorno do pysus — ParquetSet não é iterável

                for arq in arquivos:
                    try:
                        sim.download([arq], local_dir=raw_dir)
                    except Exception as e:
                        print(f"    Erro download {arq}: {e}")

                # Fase 2: Processar parquets do disco
                # Mais robusto que tentar usar o retorno de sim.download()
                for pq_path in sorted(raw_dir.glob(f"DO{uf.upper()}*.parquet")):
                    try:
                        df = _ler_parquet_seguro(pq_path)
                        if df is None or df.empty:
                            continue
                        if "IDADE" not in df.columns or "CODMUNRES" not in df.columns:
                            continue
                        df_inf = df[df["IDADE"].apply(_eh_menor_1_ano)].copy()
                        if df_inf.empty:
                            continue
                        df_inf["CO_MUNICIPIO_6D"] = df_inf["CODMUNRES"].astype(str).str.zfill(7).str[:6]
                        df_inf["NU_ANO"] = pd.to_datetime(df_inf["DTOBITO"], format="%d%m%Y", errors="coerce").dt.year
                        cod_uf = UF_PARA_CODIGO.get(uf.upper(), uf.zfill(2))
                        df_inf = df_inf[df_inf["CO_MUNICIPIO_6D"].str[:2] == cod_uf]
                        frames.append(df_inf.groupby(["CO_MUNICIPIO_6D", "NU_ANO"]).size().reset_index(name="QT_OBITOS_INFANTIS"))
                    except Exception as e:
                        print(f"    Erro processando {pq_path.name}: {e}")
            except Exception as e:
                print(f"  Erro UF {uf}: {e}")
    finally:
        socket.setdefaulttimeout(old_timeout)
    if not frames:
        return pd.DataFrame(columns=["CO_MUNICIPIO_6D", "NU_ANO", "QT_OBITOS_INFANTIS"])
    return pd.concat(frames).groupby(["CO_MUNICIPIO_6D", "NU_ANO"])["QT_OBITOS_INFANTIS"].sum().reset_index()


def _baixar_sinasc(ufs_siglas, anos):
    import socket, shutil
    from pathlib import Path
    from pysus.online_data.SINASC import SINASC
    print("  Baixando SINASC (nascidos vivos)...")

    # Limpa cache do pysus p/ forçar re-download.
    # Mesmo motivo do SIM: sinasc.download() pula arquivos cacheados
    # sem retornar, causando perda de anos inteiros.
    pysus_cache = Path.home() / ".pysus"
    if pysus_cache.exists():
        for f in pysus_cache.glob("DN*"):
            if f.is_dir():
                shutil.rmtree(f, ignore_errors=True)
            else:
                f.unlink(missing_ok=True)

    sinasc = SINASC().load()
    raw_dir = Settings.RAW_SINASC_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(120)
    try:
        for uf in tqdm(ufs_siglas, desc="SINASC"):
            try:
                arquivos = sinasc.get_files("DN", uf=uf, year=anos)
                if not arquivos:
                    print(f"  SINASC: nenhum arquivo para {uf}")
                    continue
                print(f"  SINASC/{uf}: {len(arquivos)} arquivos encontrados")

                # Fase 1: Download de todos os arquivos
                for arq in arquivos:
                    try:
                        sinasc.download([arq], local_dir=raw_dir)
                    except Exception as e:
                        print(f"    Erro download {arq}: {e}")

                # Fase 2: Processar parquets do disco
                for pq_path in sorted(raw_dir.glob(f"DN{uf.upper()}*.parquet")):
                    try:
                        df = _ler_parquet_seguro(pq_path)
                        if df is None or df.empty:
                            continue
                        if "CODMUNRES" not in df.columns:
                            continue
                        df["CO_MUNICIPIO_6D"] = df["CODMUNRES"].astype(str).str.zfill(7).str[:6]
                        df["NU_ANO"] = pd.to_datetime(df["DTNASC"], format="%d%m%Y", errors="coerce").dt.year
                        cod_uf = UF_PARA_CODIGO.get(uf.upper(), uf.zfill(2))
                        df = df[df["CO_MUNICIPIO_6D"].str[:2] == cod_uf]
                        frames.append(df.groupby(["CO_MUNICIPIO_6D", "NU_ANO"]).size().reset_index(name="QT_NASCIDOS_VIVOS"))
                    except Exception as e:
                        print(f"    Erro processando {pq_path.name}: {e}")
            except Exception as e:
                print(f"  Erro UF {uf}: {e}")
    finally:
        socket.setdefaulttimeout(old_timeout)
    if not frames:
        return pd.DataFrame(columns=["CO_MUNICIPIO_6D", "NU_ANO", "QT_NASCIDOS_VIVOS"])
    return pd.concat(frames).groupby(["CO_MUNICIPIO_6D", "NU_ANO"])["QT_NASCIDOS_VIVOS"].sum().reset_index()


def extrair_mort_infantil(codigos_uf, ufs_siglas, anos, escopo_label, csv=False):
    """Extrai taxa de mortalidade infantil (SIM + SINASC)."""
    print("\n=== MORTALIDADE INFANTIL (SIM + SINASC) ===")

    df_obitos = _baixar_sim(ufs_siglas, anos)
    df_nasc   = _baixar_sinasc(ufs_siglas, anos)

    mun = carregar_municipios()
    if codigos_uf:
        mun = mun[mun["CO_MUNICIPIO_6D"].str[:2].isin(codigos_uf)]

    grade = pd.MultiIndex.from_product(
        [mun["CO_MUNICIPIO_6D"].unique(), anos],
        names=["CO_MUNICIPIO_6D", "NU_ANO"]
    ).to_frame(index=False)

    res = (grade
           .merge(df_obitos, on=["CO_MUNICIPIO_6D", "NU_ANO"], how="left")
           .merge(df_nasc,   on=["CO_MUNICIPIO_6D", "NU_ANO"], how="left"))

    res["QT_OBITOS_INFANTIS"] = res["QT_OBITOS_INFANTIS"].fillna(0).astype(int)
    res["QT_NASCIDOS_VIVOS"]  = res["QT_NASCIDOS_VIVOS"].fillna(0).astype(int)
    res["VL_MORT_INFANTIL"]   = res.apply(
        lambda r: round(r["QT_OBITOS_INFANTIS"] / r["QT_NASCIDOS_VIVOS"] * 1000, 2)
        if r["QT_NASCIDOS_VIVOS"] > 0 else None, axis=1
    )
    res = (res.merge(mun[["CO_MUNICIPIO_6D", "NO_MUNICIPIO", "SG_UF"]], on="CO_MUNICIPIO_6D", how="left")
           [["CO_MUNICIPIO_6D", "NO_MUNICIPIO", "SG_UF", "NU_ANO",
             "QT_OBITOS_INFANTIS", "QT_NASCIDOS_VIVOS", "VL_MORT_INFANTIL"]]
           .sort_values(["CO_MUNICIPIO_6D", "NU_ANO"]).reset_index(drop=True))

    _salvar(res, "mort_infantil", escopo_label, min(anos), max(anos), csv)


# ─────────────────────────────────────────────────────────────────────
# 5. PIB PER CAPITA — IBGE SIDRA tabela 5938
# ─────────────────────────────────────────────────────────────────────

def _sidra_pib_ano(ano, retries=3, espera=5):
    """Consulta PIB per capita municipal na SIDRA (tabela 5938, variável 37)."""
    url = SIDRA_PIB_URL.format(ano=ano)
    for tentativa in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            registros = []
            for row in resp.json()[1:]:
                cod = row.get("D1C", "").strip()
                val = row.get("V", "").strip()
                if not cod or not val or val in ("-", "...", ""):
                    continue
                try:
                    registros.append({
                        "CO_MUNICIPIO_7D": cod.zfill(7),
                        "NU_ANO": ano,
                        "VL_PIB_PERCAPITA": float(val.replace(",", ".")),
                    })
                except ValueError:
                    continue
            return registros
        except requests.exceptions.RequestException as e:
            print(f"  Tentativa {tentativa}/{retries} falhou ({ano}): {e}")
            if tentativa < retries:
                time.sleep(espera)
    return []


def extrair_pib(codigos_uf, anos, escopo_label, csv=False):
    """Extrai PIB per capita municipal (SIDRA tabela 5938, variável 37)."""
    print("\n=== PIB PER CAPITA (SIDRA t/5938) ===")

    # SIDRA PIB municipal: dados de 2002 em diante
    anos_ok = [a for a in anos if a >= 2002]
    if len(anos_ok) < len(anos):
        print("  AVISO: SIDRA PIB municipal cobre a partir de 2002.")

    frames = []
    for ano in tqdm(anos_ok, desc="Consultando SIDRA PIB"):
        registros = _sidra_pib_ano(ano)
        if registros:
            df = pd.DataFrame(registros)
            if codigos_uf:
                df = df[df["CO_MUNICIPIO_7D"].str[:2].isin(codigos_uf)]
            if not df.empty:
                frames.append(df)
        time.sleep(0.5)

    if not frames:
        print("  ERRO: nenhum dado obtido da API SIDRA.")
        return

    resultado = pd.concat(frames, ignore_index=True)
    resultado["CO_MUNICIPIO_6D"] = resultado["CO_MUNICIPIO_7D"].str[:6]
    if resultado.duplicated(["CO_MUNICIPIO_6D", "NU_ANO"]).any():
        resultado = resultado.drop_duplicates(["CO_MUNICIPIO_6D", "NU_ANO"], keep="last")

    mun = carregar_municipios()
    resultado = (resultado
                 .merge(mun[["CO_MUNICIPIO_7D", "NO_MUNICIPIO", "SG_UF"]], on="CO_MUNICIPIO_7D", how="left")
                 [["CO_MUNICIPIO_6D", "NO_MUNICIPIO", "SG_UF", "NU_ANO", "VL_PIB_PERCAPITA"]]
                 .sort_values(["CO_MUNICIPIO_6D", "NU_ANO"]).reset_index(drop=True))

    _salvar(resultado, "pib_percapita", escopo_label, min(anos), max(anos), csv)

# ─────────────────────────────────────────────────────────────────────
# 6. INDICADORES IPEA — IPEADATA (7 séries de saúde municipal)
# ─────────────────────────────────────────────────────────────────────

def extrair_ipea(codigos_uf, anos, escopo_label, csv=False):
    """Extrai 7 indicadores de saúde municipais via IPEADATA."""
    try:
        import ipeadatapy as ip
    except ImportError:
        print("  ERRO: ipeadatapy não instalado. Execute: pip install ipeadatapy")
        return

    print("\n=== INDICADORES IPEA (IPEADATA) ===")
    ano_min, ano_max = min(anos), max(anos)

    frames = {}
    for codigo, (coluna, dtype) in IPEA_SERIES.items():
        print(f"  {codigo} → {coluna}...", end=" ", flush=True)
        df_serie = None
        for tentativa in range(1, 4):
            try:
                df_serie = ip.timeseries(codigo)
                break
            except Exception as e:
                if tentativa < 3:
                    time.sleep(5 * tentativa)
                else:
                    print(f"ERRO ({e}) — pulando.")
        if df_serie is None:
            continue

        val_col = next((c for c in df_serie.columns if c.startswith("VALUE")), None)
        if val_col is None:
            print("sem coluna VALUE — pulando.")
            continue

        # Em ipeadatapy, CODE = TERCODIGO (código IBGE do território)
        df_serie = df_serie.rename(columns={"CODE": "CO_MUN_RAW"})
        df_serie["CO_MUN_RAW"] = df_serie["CO_MUN_RAW"].astype(str).str.strip()
        df_serie["CO_MUNICIPIO_6D"] = df_serie["CO_MUN_RAW"].str[:6].str.zfill(6)

        df_serie = df_serie[df_serie["YEAR"].between(ano_min, ano_max)].copy()
        if codigos_uf:
            df_serie = df_serie[df_serie["CO_MUNICIPIO_6D"].str[:2].isin(codigos_uf)]

        df_serie = (df_serie[["CO_MUNICIPIO_6D", "YEAR", val_col]]
                    .rename(columns={"YEAR": "NU_ANO", val_col: coluna}))
     
        df_serie[coluna] = pd.to_numeric(df_serie[coluna], errors="coerce")
        if dtype == "Int64":
            df_serie[coluna] = df_serie[coluna].round(0).astype("Int64")
        elif dtype == "float64":
            df_serie[coluna] = df_serie[coluna] / 10

        registros_por_ano = df_serie.groupby("NU_ANO").size().median()
        print(f"{len(df_serie):,} registros | {registros_por_ano:.0f}/ano")

        if registros_por_ano < 100:
            print(f"  AVISO: {codigo} com {registros_por_ano:.0f} obs/ano — granularidade pode não ser municipal.")

        frames[codigo] = df_serie
        time.sleep(1)

    if not frames:
        print("  ERRO: nenhuma série IPEA obtida.")
        return

    mun = carregar_municipios()
    if codigos_uf:
        mun = mun[mun["CO_MUNICIPIO_6D"].str[:2].isin(codigos_uf)]

    grade = pd.MultiIndex.from_product(
        [mun["CO_MUNICIPIO_6D"].unique(), anos],
        names=["CO_MUNICIPIO_6D", "NU_ANO"]
    ).to_frame(index=False)

    resultado = grade
    for codigo, df_s in frames.items():
        coluna = IPEA_SERIES[codigo][0]
        df_agg = (df_s.groupby(["CO_MUNICIPIO_6D", "NU_ANO"], as_index=False)[coluna]
                  .first())
        resultado = resultado.merge(df_agg, on=["CO_MUNICIPIO_6D", "NU_ANO"], how="left")

    resultado = resultado.sort_values(["CO_MUNICIPIO_6D", "NU_ANO"]).reset_index(drop=True)
    _salvar(resultado, "ipea_saude", escopo_label, ano_min, ano_max, csv)

# ─────────────────────────────────────────────────────────────────────
# 7. merge dos 5 parquets em socioeconomico.parquet
# ─────────────────────────────────────────────────────────────────────

def consolidar_socioeconomico(escopo_label, anos, csv=False):
    """
    Merge dos 5 parquets intermediários em socioeconomico.parquet final.
    Colunas alinhadas ao schema: VL_POPULACAO → QT_POPULACAO.
    Parquets ausentes geram NULLs nas colunas correspondentes.
    """
    print("\n=== CONSOLIDANDO TF_SOCIOECONOMICO ===")
    processed = Settings.PROCESSED_DIR

    pop_path = processed / "populacao.parquet"
    if not pop_path.exists():
        print("  ERRO: populacao.parquet não encontrado — necessário como grade base.")
        return

    # Grade base município × ano com QT_POPULACAO (alinhado ao schema)
    base = pd.read_parquet(pop_path, columns=["CO_MUNICIPIO_6D", "NU_ANO", "VL_POPULACAO"])
    base = base.rename(columns={"VL_POPULACAO": "QT_POPULACAO"})
    base["QT_POPULACAO"] = base["QT_POPULACAO"].astype("Int64")

    # Merge sequencial dos outros indicadores
    merges = [
        ("pib_percapita.parquet",  ["CO_MUNICIPIO_6D", "NU_ANO", "VL_PIB_PERCAPITA"]),
        ("mort_infantil.parquet",  ["CO_MUNICIPIO_6D", "NU_ANO", "QT_OBITOS_INFANTIS",
                                    "QT_NASCIDOS_VIVOS", "VL_MORT_INFANTIL"]),
        ("leitos.parquet",         ["CO_MUNICIPIO_6D", "NU_ANO", "QT_LEITOS_SUS",
                                    "VL_LEITOS_SUS_1000"]),
        ("medicos.parquet",        ["CO_MUNICIPIO_6D", "NU_ANO", "QT_MEDICOS",
                                    "VL_MEDICOS_1000"]),
        
        ("ipea_saude.parquet",     ["CO_MUNICIPIO_6D", "NU_ANO",
                                    "QT_BENEFICIARIOS_PLANO_SAUDE", "QT_ESTAB_INTERNACAO_SUS",
                                    "QT_ESTAB_SAUDE", "QT_ESTAB_URGENCIA_SUS",
                                    "VL_ENFERMEIROS_1000", "VL_TECNICOS_SAUDE_1000",
                                    "VL_LEITOS_UTI_SUS_1000"]),
    ]

    resultado = base
    for fname, cols in merges:
        path = processed / fname
        if path.exists():
            df = pd.read_parquet(path, columns=cols)
            resultado = resultado.merge(df, on=["CO_MUNICIPIO_6D", "NU_ANO"], how="left")
            print(f"  ✓ {fname}")
        else:
            print(f"  ⚠️  {fname} não encontrado — colunas serão NULL")

    resultado = resultado.sort_values(["CO_MUNICIPIO_6D", "NU_ANO"]).reset_index(drop=True)

    _salvar(resultado, "socioeconomico", escopo_label, min(anos), max(anos), csv)


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Extração de indicadores socioeconômicos para TF_SOCIOECONOMICO."
    )
    parser.add_argument(
        "--indicador", nargs="+", metavar="IND",
        choices=INDICADORES_DISPONIVEIS,
        default=INDICADORES_DISPONIVEIS,
        help=f"Indicadores a extrair. Padrão: todos. Opções: {INDICADORES_DISPONIVEIS}",
    )
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--uf", nargs="+", metavar="UF")
    scope.add_argument("--brasil", action="store_true")
    parser.add_argument("--ano-inicio", type=int, default=None)
    parser.add_argument("--ano-fim",    type=int, default=None)
    parser.add_argument("--csv", action="store_true")
    parser.add_argument("--incluir-psiquiatrico", action="store_true",
                        help="Inclui leitos psiquiátricos (CODLEITO=33). Padrão: excluídos.")
    return parser.parse_args()


def main():
    inicio = time.time()
    args   = parse_args()

    # Escopo geográfico
    if args.brasil:
        ufs_siglas   = list(UF_PARA_CODIGO.keys())
        codigos_uf   = None
        escopo_label = "brasil"
    elif args.uf:
        ufs_siglas   = [u.upper() for u in args.uf]
        codigos_uf   = resolver_ufs(ufs_siglas)
        escopo_label = "_".join(ufs_siglas)
    else:
        # Download CNES/SIM/SINASC apenas das UFs alvo 
        # mas sem filtrar a saída — população e PIB cobrem todo o Brasil.
        ufs_siglas   = [u.upper() for u in Settings.UF_DEFAULT]
        codigos_uf   = None
        escopo_label = "brasil"

    # Período
    ano_inicio  = args.ano_inicio or Settings.ANOS_INICIO
    ano_fim     = args.ano_fim    or Settings.ANOS_FIM
    anos        = list(range(ano_inicio, ano_fim + 1))
    indicadores = args.indicador

    print("=" * 60)
    print("EXTRAÇÃO INDICADORES SOCIOECONÔMICOS")
    print("=" * 60)
    print(f"  Escopo     : {ufs_siglas}")
    print(f"  Período    : {ano_inicio}–{ano_fim}")
    print(f"  Indicadores: {indicadores}")

    if "populacao"     in indicadores:
        extrair_populacao(codigos_uf, anos, escopo_label, args.csv)
    if "leitos"        in indicadores:
        extrair_leitos(codigos_uf, ufs_siglas, anos, escopo_label, args.csv, args.incluir_psiquiatrico)
    if "medicos"       in indicadores:
        extrair_medicos(codigos_uf, ufs_siglas, anos, escopo_label, args.csv)
    if "mort_infantil" in indicadores:
        extrair_mort_infantil(codigos_uf, ufs_siglas, anos, escopo_label, args.csv)
    if "pib"           in indicadores:
        extrair_pib(codigos_uf, anos, escopo_label, args.csv)
    if "ipea"          in indicadores:
        extrair_ipea(codigos_uf, anos, escopo_label, args.csv)

    # Consolidação final: merge dos parquets intermediários em socioeconomico.parquet
    consolidar_socioeconomico(escopo_label, anos, args.csv)

    print(f"\n{'='*60}")
    print(f"CONCLUÍDO em {(time.time() - inicio) / 60:.1f} min")


if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging("socioeconomico")
    main()