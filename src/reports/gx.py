import polars as pl
import sys
from pathlib import Path
import logging

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def verificar_etnia():
    """Verifica a contagem de registros de etnia válidos e inválidos."""
    try:
        caminho_arquivo = Settings.INTERIM_DIR / Settings.PARQUET_TREATED_FILENAME
        
        if not caminho_arquivo.exists():
            logger.error(f"Arquivo de entrada não encontrado: {caminho_arquivo}")
            return

        logger.info("Analisando dados de ETNIA...")

        # Lê o arquivo
        df = pl.read_parquet(caminho_arquivo)
        
        # Define os valores inválidos
        valores_invalidos = ["0", "00", "000"]
        
        # Filtra os dados que deveriam ser removidos
        df_removidos = df.filter(
            pl.col("ETNIA").is_null() | 
            pl.col("ETNIA").is_in(valores_invalidos)
        )
        
        # Filtra os dados que deveriam ser mantidos
        df_mantidos = df.filter(
            pl.col("ETNIA").is_not_null() & 
            ~pl.col("ETNIA").is_in(valores_invalidos)
        )

        # Conta os registros
        total_registros = len(df)
        registros_removidos = len(df_removidos)
        registros_mantidos = len(df_mantidos)
        
        # Exibe os resultados
        logger.info("=" * 40)
        logger.info(f"Total de registros na tabela principal: {total_registros:,}")
        logger.info(f"Registros COM ETNIA inválida/nula: {registros_removidos:,}")
        logger.info(f"Registros COM ETNIA válida: {registros_mantidos:,}")
        
        if total_registros > 0:
            porcentagem_mantida = (registros_mantidos / total_registros) * 100
            logger.info(f"Porcentagem de registros mantidos: {porcentagem_mantida:.2f}%")
        
        logger.info("=" * 40)

    except Exception as e:
        logger.error(f"Ocorreu um erro ao executar o diagnóstico: {e}")

if __name__ == "__main__":
    verificar_etnia()