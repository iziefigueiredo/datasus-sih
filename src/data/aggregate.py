import polars as pl
import sys
from pathlib import Path
import logging
import time
import tempfile
import gc

# Garante que o script pode importar de src/
SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class SIHContractor:
    """Contrai os dados do SIH/SUS agregando por N_AIH"""

    def __init__(self, arquivo_entrada=None, arquivo_saida=None):
        self.entrada = arquivo_entrada or Settings.INTERIM_DIR / "sih_rs_tratado.parquet"
        self.saida = arquivo_saida or Settings.INTERIM_DIR / "sih_rs_contraido.parquet"
        Settings.criar_diretorios()
        logger.info(f"Processamento de agregação iniciado. Entrada: {self.entrada}")

    def contrair(self) -> int:
        """Executa a contração dos dados"""
        inicio = time.time()
        
        try:
            logger.info("Lendo arquivo de entrada...")
            # Usa scan_parquet para ler de forma lazy e otimizar a memória
            df_lazy = pl.scan_parquet(self.entrada)
            
            # Lógica para as agregações
            colunas_soma = ['VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI']
            colunas_media = ['UTI_MES_TO', 'UTI_INT_TO', 'DIAR_ACOM', 'IDADE']
            
            aggregations = []
            for col in df_lazy.columns:
                if col == 'N_AIH':
                    continue
                if col in colunas_soma:
                    aggregations.append(pl.col(col).sum().alias(col))
                elif col in colunas_media:
                    aggregations.append(pl.col(col).mean().alias(col))
                # Mantém a primeira ocorrência para as outras colunas
                else:
                    aggregations.append(pl.col(col).first().alias(col))

            logger.info("Executando agregação...")
            df_contraido = df_lazy.group_by("N_AIH").agg(aggregations).collect()

            registros_finais = len(df_contraido)
            
            logger.info(f"Salvando arquivo final: {self.saida}")
            df_contraido.write_parquet(self.saida, compression="snappy")
            
            tempo_total = time.time() - inicio
            tamanho_final_mb = self.saida.stat().st_size / (1024 * 1024)
            
            logger.info("="*60)
            logger.info("AGREGAÇÃO CONCLUÍDA!")
            logger.info("="*60)
            logger.info(f"Registros contraídos: {registros_finais:,}")
            logger.info(f"Tamanho final: {tamanho_final_mb:.1f} MB")
            logger.info(f"Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
            
            return registros_finais

        except Exception as e:
            logger.error(f"Erro na contração: {e}")
            raise

def main():
    """Execução principal"""
    try:
        contractor = SIHContractor()
        resultado = contractor.contrair()
        print(f"\nSUCESSO! {resultado:,} registros contraídos.")
    except Exception as e:
        print(f"\nERRO: {e}")
        raise


if __name__ == "__main__":
    main()