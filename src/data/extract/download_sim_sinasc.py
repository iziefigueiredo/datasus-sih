"""
Download SIM + SINASC
Localização: sihrd5/src/data/download_sim_sinasc.py
Função: EXTRACT - Baixa dados SIM (.dbc) e SINASC (.dbc) e converte para .parquet

Fontes:
    SIM    — /dissemin/publicos/SIM/CID10/DORES/
    SINASC — /dissemin/publicos/SINASC/NOV/DORES/
"""

import sys
import time
from pathlib import Path
from tqdm import tqdm
import logging
import psutil
import pyarrow.parquet as pq
from pysus.online_data.SIM import SIM
from pysus.online_data.SINASC import SINASC

SRC_DIR = Path(__file__).parent.parent.parent  # src/
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logger = logging.getLogger(__name__)


def verificar_existentes(pasta):
    """Conta parquets já baixados na pasta."""
    if not pasta.exists():
        return 0, set()
    nomes = {
        f.stem for f in pasta.glob("*.parquet")
        if f.stat().st_size > 0
    }
    return len(nomes), nomes


def filtrar_novos(arquivos, nomes_existentes):
    """Remove arquivos já baixados."""
    novos = []
    for arq in arquivos:
        nome = Path(str(arq)).stem
        if nome.endswith('.dbc'):
            nome = nome[:-4]
        if nome not in nomes_existentes:
            novos.append(arq)
    return novos


def log_desempenho(pasta, label):
    """Loga contagem, registros e tamanho dos parquets na pasta."""
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


def download_base(db, grupo, label, raw_dir, ufs, anos, auto=False):
    """
    Lógica genérica de download para SIM ou SINASC.
    db     — instância SIM().load() ou SINASC().load()
    grupo  — 'CID10' (SIM) ou 'DN' (SINASC)
    label  — 'SIM' ou 'SINASC' (para logs)
    raw_dir — pasta de destino
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nVerificando arquivos já baixados em {raw_dir}...")
    qtd_existentes, nomes_existentes = verificar_existentes(raw_dir)
    print(f"Já baixados: {qtd_existentes} arquivos")

    print(f"Buscando arquivos {label}...")
    encontrados = []
    for uf in tqdm(ufs, desc=f"Buscando {label}"):
        try:
            arquivos = db.get_files(grupo, uf=uf, year=anos)
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
                # SIM: download um por vez | SINASC: aceita lista
                if label == 'SIM':
                    for arq in tqdm(lote, desc=f"  Lote {lote_num}", leave=False):
                        db.download(arq, local_dir=raw_dir)
                        baixados += 1
                else:
                    parquets = db.download(lote, local_dir=raw_dir)
                    baixados += len(parquets)

                progresso = baixados / len(a_baixar) * 100
                print(f"  {baixados}/{len(a_baixar)} ({progresso:.0f}%)")
            except Exception as e:
                print(f"  Erro lote {lote_num}: {e}")
                continue
    else:
        if label == 'SIM':
            for arq in tqdm(a_baixar, desc=f"Baixando {label}"):
                try:
                    db.download(arq, local_dir=raw_dir)
                    baixados += 1
                except Exception as e:
                    print(f"  Erro {arq}: {e}")
        else:
            parquets = db.download(a_baixar, local_dir=raw_dir)
            baixados = len(parquets)

    print(f"\nCONCLUÍDO {label}! {baixados} arquivos baixados.")
    return baixados


def main():
    inicio = time.time()
    print("=== DOWNLOAD SIM + SINASC ===")
    Settings.create_directories()

    ufs  = Settings.UF_DEFAULT
    anos = Settings.get_years_range()

    print(f"Config: UF={ufs}, Anos={min(anos)}-{max(anos)}")

    # --- SIM ---
    print("\n" + "="*40)
    print("SISTEMA DE INFORMAÇÃO SOBRE MORTALIDADE (SIM)")
    print("="*40)
    sim = SIM().load()
    n_sim = download_base(
        db=sim,
        grupo="CID10",
        label="SIM",
        raw_dir=Settings.RAW_SIM_DIR,
        ufs=ufs,
        anos=anos,
    )

    # --- SINASC ---
    print("\n" + "="*40)
    print("SISTEMA DE INFORMAÇÕES SOBRE NASCIDOS VIVOS (SINASC)")
    print("="*40)
    sinasc = SINASC().load()
    n_sinasc = download_base(
        db=sinasc,
        grupo="DN",
        label="SINASC",
        raw_dir=Settings.RAW_SINASC_DIR,
        ufs=ufs,
        anos=anos,
    )

    # --- Log final ---
    fim = time.time()
    duracao_min = (fim - inicio) / 60

    print("\n" + "="*40)
    print("RESUMO FINAL")
    print("="*40)
    print("\nSIM:")
    log_desempenho(Settings.RAW_SIM_DIR, "SIM")
    print("\nSINASC:")
    log_desempenho(Settings.RAW_SINASC_DIR, "SINASC")

    logger.info(f"Tempo total: {duracao_min:.2f} min | SIM: {n_sim} novos | SINASC: {n_sinasc} novos")
    print(f"\nTempo total: {duracao_min:.2f} minutos")


if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging("download_sim_sinasc")
    main()