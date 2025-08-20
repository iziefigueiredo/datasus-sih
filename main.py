"""
Pipeline principal - Etapas 3, 4, 4.5 e 5
Pré-processamento, divisão, agregações e carga no banco de dados
"""

import sys
from pathlib import Path

SRC_DIR = Path(__file__).parent / "src"
sys.path.insert(0, str(SRC_DIR))


from config.settings import Settings
from data.split import TableSplitter
from database.load import run_db_load_pipeline

def main():
    print("=" * 60)
    print("PIPELINE SIH/SUS - ETAPAS 3, 4, 4.5 e 5")
    print("=" * 60)
    print()

    # Criação de diretórios
    print("Criando estrutura de diretórios...")
    Settings.criar_diretorios()
    print("Diretórios prontos!\n")

    try:
        """"
        # Etapa 1: Download
        print("ETAPA 1: Download dos dados do DATASUS")
        print("-" * 50)
        from data.download import main as download_dados
        download_dados()
        print("Download concluído!")
        print()

        # Etapa 2: Unificação
        print("ETAPA 2: Unificação dos arquivos")
        print("-" * 50)
        from data.unify import main as unificar_dados
        unificar_dados()
        print("Unificação concluída!")
        print()
        """
        # Etapa 3: Pré-processamento
        print("ETAPA 3: Pré-processamento dos dados")
        print("-" * 50)
        from data.preprocess import main as preprocessar_dados
        preprocessar_dados()
        print("Pré-processamento concluído!")
        print()
        
        print("="*60)
        print("PIPELINE CONCLUÍDO COM SUCESSO!")
        print("="*60)
        print("Dataset final: data/parquet_unified/sih_rs_tratado.parquet")
        print("Pronto para análises de gestão hospitalar!")
        print("="*60)
        
        
        # Etapa 4 - Divisão
        print("ETAPA 4 - DIVISÃO DOS DADOS EM TABELAS")
        print("-" * 50)
        splitter = TableSplitter()
        splitter.run()
        print("Divisão concluída!\n")

        # Etapa 5 - Carga
        print("ETAPA 5 - CARGA DOS DADOS NO POSTGRESQL")
        print("-" * 50)
        run_db_load_pipeline()
        print("Carga no banco de dados concluída!\n")

        print("=" * 60)
        print("PIPELINE CONCLUÍDO COM SUCESSO!")
        print("=" * 60)
        print("Dados disponíveis em: data/parquet_unified/")
        print("Banco de dados pronto para uso.")
        print("=" * 60)

    except Exception as e:
        print("=" * 60)
        print("ERRO NO PIPELINE!")
        print("=" * 60)
        print(f"Erro: {e}")
        raise

if __name__ == "__main__":
    main()