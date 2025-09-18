"""
Pipeline principal - Menu de Etapas 1 a 5
Download, Unificação, Pré-processamento, Divisão e Carga no banco
"""

import sys
import logging
from pathlib import Path

# --- INÍCIO DO BLOCO DE CONFIGURAÇÃO ---

# Define o diretório base do projeto de forma robusta
# Path(__file__).resolve() garante que o caminho seja absoluto, evitando erros
BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "reports" / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True) # Garante que a pasta reports/logs/ exista

# Mapeia o nome de cada etapa para o nome do seu arquivo de log
STEP_LOG_FILES = {
    "download": "1_download.log",
    "unify": "2_unify.log",
    "preprocess": "3_preprocess.log",
    "aggregate": "4_aggregate.log",
    "split": "5_split.log",
    "load": "6_load.log",
    "main": "0_main_menu.log" # Um log para o próprio menu
}

def setup_logging(step_name: str):
    """
    Configura o sistema de logging para uma etapa específica.
    Remove handlers antigos e adiciona novos para o arquivo de log correto.
    """
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


# Certifica que o diretório 'src' está no PATH
SRC_DIR = BASE_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

# Importa a função principal de cada etapa
from data.download import main as download_main
from data.unify import main as unify_main
from data.preprocess import main as preprocess_main
from data.aggregate import main as aggregate_main
from data.split import main as split_main
from database.load import run_db_load_pipeline as load_main


def main():
    while True:
        setup_logging("main")
        
        print("=" * 60)
        print("PIPELINE SIH/SUS - MENU DE ETAPAS")
        print("=" * 60)
        print("1 - Download dos dados do DATASUS")
        print("2 - Unificação dos arquivos")
        print("3 - Pré-processamento")
        print("4 - Agregação e contração")
        print("5 - Divisão em tabelas")
        print("6 - Carga no banco PostgreSQL")
        print("0 - Sair")
        print("=" * 60)

        escolha = input("Digite o número da etapa que deseja executar: ").strip()

        try:
            if escolha == "1":
                setup_logging("download") 
                logging.info("Iniciando Etapa 1: Download")
                download_main()
                logging.info("Etapa 1: Download concluída.")
            elif escolha == "2":
                setup_logging("unify")
                logging.info("Iniciando Etapa 2: Unificação")
                unify_main()
                logging.info("Etapa 2: Unificação concluída.")
            elif escolha == "3":
                setup_logging("preprocess")
                logging.info("Iniciando Etapa 3: Pré-processamento")
                preprocess_main()
                logging.info("Etapa 3: Pré-processamento concluído.")
            elif escolha == "4":
                setup_logging("aggregate")
                logging.info("Iniciando Etapa 4: Agregação e contração")
                aggregate_main()
                logging.info("Etapa 4: Agregação e contração concluída.")
            elif escolha == "5":
                setup_logging("split")
                logging.info("Iniciando Etapa 5: Divisão")
                split_main()
                logging.info("Etapa 5: Divisão concluída.")
            elif escolha == "6":
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