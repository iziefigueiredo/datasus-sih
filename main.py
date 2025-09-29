"""
Pipeline principal - Apenas Etapa 6 (Carga no banco)
"""

import sys
import logging
from pathlib import Path

# --- INÍCIO DO BLOCO DE CONFIGURAÇÃO ---

BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "reports" / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

STEP_LOG_FILES = {
    "load": "6_load.log",
    "main": "0_main_menu.log"
}

def setup_logging(step_name: str):
    log_filename = STEP_LOG_FILES.get(step_name, "pipeline_geral.log")
    log_filepath = LOGS_DIR / log_filename

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    
    file_handler = logging.FileHandler(log_filepath, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

# --- FIM DO BLOCO DE CONFIGURAÇÃO ---

SRC_DIR = BASE_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

# Só importa a etapa 6
from database.load import run_db_load_pipeline as load_main

def main():
    while True:
        setup_logging("main")
        
        print("=" * 60)
        print("PIPELINE SIH/SUS - MENU DE ETAPAS")
        print("=" * 60)
        print("6 - Carga no banco PostgreSQL")
        print("0 - Sair")
        print("=" * 60)

        escolha = input("Digite o número da etapa que deseja executar: ").strip()

        try:
            if escolha == "6":
                setup_logging("load")
                logging.info("Iniciando Etapa 6: Carga no Banco")
                load_main()
                logging.info("Etapa 6: Carga no Banco concluída.")
            elif escolha == "0":
                logging.info("Encerrando pipeline.")
                break
            else:
                logging.warning(f"Opção inválida selecionada: {escolha}")
        except Exception as e:
            logging.error("=" * 60)
            logging.error("ERRO NA ETAPA!")
            logging.error(f"Detalhes do erro: {e}", exc_info=True)
            logging.error("=" * 60)


if __name__ == "__main__":
    main()