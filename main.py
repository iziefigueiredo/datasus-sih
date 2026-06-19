"""
Pipeline principal - Menu de Etapas
Orquestracao de Extracao, Transformacao e Carga (ETL/ELT)

Arquitetura:
    Etapas 1-3: Extract — download de dados de fontes externas
    Etapa 4:    Load+Transform — carga ELT incremental por UF no DuckDB
"""
import sys
import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

from config.logging_config import setup_logging

from data.extract.download_sih           import main as download_data_main
from data.extract.download_docs          import main as download_docs_main
from data.extract.download_domain_tables   import main as download_tabelas_main
from data.extract.extract_socioeconomic  import main as socioeconomico_main
from data.pipeline_load                  import main as pipeline_carga_main

STEP_NAMES = {
    "1": ("download_sih",    "Etapa 1: Download dos microdados"),
    "2": ("download_docs",   "Etapa 2: Download documentacao + tabelas de dominio"),
    "3": ("socioeconomico",  "Etapa 3: Download de indicadores socioeconomicos"),
    "4": ("carga_elt",       "Etapa 4: Carga ELT no DuckDB"),
}

logger = logging.getLogger(__name__)


def _etapa2():
    """Etapa 2: Download documentacao + tabelas de dominio DATASUS."""
    download_docs_main()
    download_tabelas_main()


STEP_FUNCTIONS = {
    "1": download_data_main,
    "2": _etapa2,
    "3": socioeconomico_main,
    "4": pipeline_carga_main,
}


def main():
    setup_logging("main_menu")
    logger.info("Pipeline SIH/SUS iniciado.")

    while True:
        print("=" * 60)
        print("PIPELINE SIH/SUS")
        print("=" * 60)
        print("1 - Download dos microdados")
        print("2 - Download documentacao + tabelas de dominio")
        print("3 - Extracao de indicadores socioeconomicos")
        print("4 - Pipeline TLT (auditoria)")
        print("0 - Sair")
        print("=" * 60)

        escolha = input("Selecione a etapa: ").strip()

        if escolha == "0":
            logger.info("Pipeline encerrado pelo usuario.")
            break

        if escolha not in STEP_FUNCTIONS:
            logger.warning(f"Opcao invalida: \'{escolha}\'")
            print("Opcao invalida. Tente novamente.")
            continue

        log_key, step_label = STEP_NAMES[escolha]
        log_path = setup_logging(log_key)

        try:
            logger.info(f"Iniciando {step_label}...")
            STEP_FUNCTIONS[escolha]()
            logger.info(f"{step_label} concluida com sucesso.")
        except Exception as e:
            logger.error(f"Erro em {step_label}: {e}", exc_info=True)
            print(f"\nErro durante {step_label}. Verifique: {log_path}")


if __name__ == "__main__":
    main()