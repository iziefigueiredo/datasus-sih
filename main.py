"""
Pipeline principal - Menu de Etapas 1 a 5
Download, Unificação, Pré-processamento, Divisão e Carga no banco
"""

import sys
from pathlib import Path

SRC_DIR = Path(__file__).parent / "src"
sys.path.insert(0, str(SRC_DIR))

from config.settings import Settings
from data.split import TableSplitter
from database.load import run_db_load_pipeline


def etapa_1():
    from data.download import main as download_dados
    download_dados()

def etapa_2():
    from data.unify import main as unificar_dados
    unificar_dados()

def etapa_3():
    from data.preprocess import main as preprocessar_dados
    preprocessar_dados()

def etapa_4():
    splitter = TableSplitter()
    splitter.run()

def etapa_5():
    run_db_load_pipeline()


def main():
    while True:
        print("=" * 60)
        print("PIPELINE SIH/SUS - MENU DE ETAPAS")
        print("=" * 60)
        print("1 - Download dos dados do DATASUS")
        print("2 - Unificação dos arquivos")
        print("3 - Pré-processamento")
        print("4 - Divisão em tabelas")
        print("5 - Carga no banco PostgreSQL")
        print("0 - Sair")
        print("=" * 60)

        escolha = input("Digite o número da etapa que deseja executar: ").strip()

        try:
            if escolha == "1":
                etapa_1()
            elif escolha == "2":
                etapa_2()
            elif escolha == "3":
                etapa_3()
            elif escolha == "4":
                etapa_4()
            elif escolha == "5":
                etapa_5()
            elif escolha == "0":
                print("Encerrando pipeline.")
                break
            else:
                print("Opção inválida.")
        except Exception as e:
            print("=" * 60)
            print("ERRO NA ETAPA!")
            print("=" * 60)
            print(f"Erro: {e}")


if __name__ == "__main__":
    main()
