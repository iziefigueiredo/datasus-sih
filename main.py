"""
Pipeline principal - Menu de Etapas 1 a 5
Download, Unificação, Pré-processamento, Divisão e Carga no banco
"""

import sys
from pathlib import Path

# Certifica que o diretório 'src' está no PATH
SRC_DIR = Path(__file__).parent / "src"
sys.path.insert(0, str(SRC_DIR))

# Importa a função principal de cada etapa.
# Não há necessidade de criar funções intermediárias como etapa_1().
from data.download import main as download_main
from data.unify import main as unify_main
from data.preprocess import main as preprocess_main
from data.split import main as split_main
from database.load import run_db_load_pipeline as load_main


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
                download_main()
            elif escolha == "2":
                unify_main()
            elif escolha == "3":
                preprocess_main()
            elif escolha == "4":
                split_main()
            elif escolha == "5":
                load_main()
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