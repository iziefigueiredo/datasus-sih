"""
Download CNES — Leitos (LT) e Profissional (PF)
Localização: sihrd5/src/data/download_cnes.py
Função: EXTRACT - Baixa arquivos CNES (.dbc) e converte para .parquet

Grupos:
    LT — Leitos       → data/raw/cnes_lt/
    PF — Profissional → data/raw/cnes_pf/

Frequência: mensal (um arquivo por UF/ano/mês)
Cobertura : LT a partir de Out/2005, PF a partir de Ago/2005
"""

import sys
import time
from pathlib import Path
from tqdm import tqdm
import logging
import psutil
import pyarrow.parquet as pq
from pysus.online_data.CNES import CNES

SRC_DIR = Path(__file__).parent.parent.parent  # src/
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logger = logging.getLogger(__name__)


def verificar_existentes(pasta):
    if not pasta.exists():
        return 0, set()
    nomes = {f.stem for f in pasta.glob("*.parquet") if f.stat().st_size > 0}
    return len(nomes), nomes


def filtrar_novos(arquivos, nomes_existentes):
    novos = []
    for arq in arquivos:
        nome = Path(str(arq)).stem
        if nome.endswith('.dbc'):
            nome = nome[:-4]
        if nome not in nomes_existentes:
            novos.append(arq)
    return novos


def log_desempenho(pasta, label):
    process = psutil.Process()
    mem_mb = process.memory_info().rss / (1024 * 1024)
    arquivos = list(pasta.glob("*.parquet"))
    total_registros = 0
    tamanho_bytes = 0
    for arq in arquivos:
        try:
            meta = pq.ParquetFile(arq).metadata
            total_registros += meta.num_rows
            tamanho_bytes += arq.stat().st_size
        except Exception:
            continue
    tamanho_gb = tamanho_bytes / (1024 ** 3)
    logger.info(f"[{label}] Arquivos: {len(arquivos)} | Registros: {total_registros:,} | "
                f"Tamanho: {tamanho_gb:.2f} GB | Memória: {mem_mb:.2f} MB")
    print(f"  Arquivos  : {len(arquivos)}")
    print(f"  Registros : {total_registros:,}")
    print(f"  Tamanho   : {tamanho_gb:.2f} GB")
    print(f"  Memória   : {mem_mb:.2f} MB")


def download_grupo(cnes, grupo, label, raw_dir, ufs, anos, meses, auto=False):
    """Lógica genérica de download para qualquer grupo CNES."""
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nVerificando arquivos já baixados em {raw_dir}...")
    qtd_existentes, nomes_existentes = verificar_existentes(raw_dir)
    print(f"Já baixados: {qtd_existentes} arquivos")

    print(f"Buscando arquivos CNES/{grupo}...")
    encontrados = []
    for uf in tqdm(ufs, desc=f"Buscando {label}"):
        try:
            arquivos = cnes.get_files(grupo, uf=uf, year=anos, month=meses)
            encontrados.extend(arquivos)
        except Exception as e:
            print(f"  Erro buscando {uf}: {e}")

    a_baixar = filtrar_novos(encontrados, nomes_existentes)
    print(f"  Total: {len(encontrados)} | Baixados: {qtd_existentes} | Restam: {len(a_baixar)}")

    if not a_baixar:
        print(f"  Todos os arquivos {label} já baixados!")
        return 0

    print(f"\nPrimeiros arquivos:")
    for i, arq in enumerate(a_baixar[:3]):
        print(f"  {i+1}. {arq}")
    if len(a_baixar) > 3:
        print(f"  ... +{len(a_baixar) - 3} arquivos")

    if not auto:
        if input(f"\nBaixar {len(a_baixar)} arquivos {label}? (s/N): ").lower() != 's':
            print("Cancelado.")
            return 0

    print(f"\nBaixando {len(a_baixar)} arquivos {label}...")
    baixados = 0
    lote_size = 18

    if len(a_baixar) > 20:
        for i in range(0, len(a_baixar), lote_size):
            lote = a_baixar[i:i + lote_size]
            lote_num = i // lote_size + 1
            total_lotes = (len(a_baixar) - 1) // lote_size + 1
            print(f"Lote {lote_num}/{total_lotes} ({len(lote)} arquivos)...")
            try:
                for arq in tqdm(lote, desc=f"  Lote {lote_num}", leave=False):
                    cnes.download(arq, local_dir=raw_dir)
                    baixados += 1
                progresso = baixados / len(a_baixar) * 100
                print(f"  {baixados}/{len(a_baixar)} ({progresso:.0f}%)")
            except Exception as e:
                print(f"  Erro lote {lote_num}: {e}")
                continue
    else:
        for arq in tqdm(a_baixar, desc=f"Baixando {label}"):
            try:
                cnes.download(arq, local_dir=raw_dir)
                baixados += 1
            except Exception as e:
                print(f"  Erro {arq}: {e}")

    print(f"\nCONCLUÍDO {label}! {baixados} arquivos baixados.")
    return baixados


def main():
    inicio = time.time()

    print("=== DOWNLOAD CNES (LT + PF) ===")
    Settings.create_directories()
    # mkdir logs removido - usa logging centralizado

    ufs  = Settings.UF_DEFAULT
    anos = Settings.get_years_range()
    meses = Settings.MESES

    print(f"Config: UF={ufs}, Anos={min(anos)}-{max(anos)}")

    cnes = CNES().load()

    # --- LT — Leitos ---
    print("\n" + "="*40)
    print("LEITOS (LT)")
    print("="*40)
    n_lt = download_grupo(
        cnes=cnes, grupo="LT", label="LT",
        raw_dir=Settings.RAW_CNES_LT_DIR,
        ufs=ufs, anos=anos, meses=meses,
    )

    # --- PF — Profissional ---
    print("\n" + "="*40)
    print("PROFISSIONAL (PF)")
    print("="*40)
    n_pf = download_grupo(
        cnes=cnes, grupo="PF", label="PF",
        raw_dir=Settings.RAW_CNES_PF_DIR,
        ufs=ufs, anos=anos, meses=meses,
    )

    # --- Resumo final ---
    fim = time.time()
    duracao_min = (fim - inicio) / 60

    print("\n" + "="*40)
    print("RESUMO FINAL")
    print("="*40)
    print("\nLT (Leitos):")
    log_desempenho(Settings.RAW_CNES_LT_DIR, "LT")
    print("\nPF (Profissional):")
    log_desempenho(Settings.RAW_CNES_PF_DIR, "PF")

    logger.info(f"Tempo total: {duracao_min:.2f} min | LT: {n_lt} novos | PF: {n_pf} novos")
    print(f"\nTempo total: {duracao_min:.2f} minutos")


if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging("download_cnes")
    main()