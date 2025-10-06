import polars as pl
import sys
from pathlib import Path
import logging
import time
import gc
import tempfile
from datetime import datetime

# Garante que o script pode importar de src/
SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class SIHPreprocessor:
    """Pré-processamento SIH/SUS com processamento em chunks"""
    
    def __init__(self, arquivo_entrada=None, arquivo_saida=None, chunk_size=100_000):
        self.entrada = arquivo_entrada or Settings.INTERIM_DIR / "sih_rs.parquet"
        self.saida = arquivo_saida or Settings.INTERIM_DIR / "sih_rs_variavel_tipo.parquet"
        self.chunk_size = chunk_size
        self.temp_dir = Path(tempfile.mkdtemp(prefix="sih_processing_"))
        Settings.criar_diretorios()
        logger.info(f"Diretório temporário: {self.temp_dir}")
    
    def tratar_chunk_completo(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica todos os tratamentos a um chunk"""
        
        
        # Converte campos de valor de texto para float, tratando vírgulas
        campos_valores = ['VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI']
        for col in campos_valores:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .cast(pl.Float64, strict=False)
                    .alias(col)
                )

        if 'VAL_UTI' not in df.columns:
            df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias('VAL_UTI'))

      

        # Trata campos de data
        campos_datas = ['DT_INTER', 'DT_SAIDA', 'NASC']
        for col in campos_datas:
            if col in df.columns:
                df = df.with_columns([
                    pl.col(col).cast(pl.String).str.strptime(pl.Date, format="%Y%m%d", strict=False)
                ])

          
     
 
   
        # Tratamento da coluna NACIONAL
        if 'NACIONAL' in df.columns:
            df = df.with_columns(
                pl.col("NACIONAL")
                .cast(pl.Int16, strict=False) # Primeiro, tenta converter para Int16
               
            )
            
           
        # Padroniza outras colunas
        if 'NUM_FILHOS' in df.columns:
            df = df.with_columns([
                pl.col("NUM_FILHOS").cast(pl.Int8, strict=False).fill_null(0).clip(0, None)
            ])
      
        if 'ETNIA' in df.columns:
            df = df.with_columns([
                pl.col("ETNIA").cast(pl.Int8, strict=False).fill_null(0).clip(0, None)
            ])
        
              

        # Garante que N_AIH não seja nulo para evitar problemas posteriores
        df = df.filter(pl.col("N_AIH").is_not_null())
        
        return df

    def processar_salvar_chunk(self) -> list:
        """Processa chunks e salva arquivos temporários"""
        logger.info("=== FASE 1: Processamento em Chunks ===")
        
        df_lazy = pl.scan_parquet(self.entrada)
        total_rows = df_lazy.select(pl.len()).collect().item()
        logger.info(f"Total de registros: {total_rows:,}")
        
        arquivos_temp = []
        chunk_num = 0
        
        for start in range(0, total_rows, self.chunk_size):
            chunk_num += 1
            end = min(start + self.chunk_size, total_rows)
            
            if chunk_num % 10 == 0 or chunk_num == 1:
                progresso = (end / total_rows) * 100
                logger.info(f"Processando chunk {chunk_num} ({progresso:.1f}%)...")
            
            chunk = df_lazy.slice(start, self.chunk_size).collect()
            chunk_tratado = self.tratar_chunk_completo(chunk)
            
            arquivo_temp = self.temp_dir / f"chunk_{chunk_num:03d}.parquet"
            chunk_tratado.write_parquet(arquivo_temp, compression="snappy")
            arquivos_temp.append(arquivo_temp)
            
            del chunk, chunk_tratado
            if chunk_num % 5 == 0:
                gc.collect()
        
        logger.info(f"{len(arquivos_temp)} chunks processados e salvos")
        return arquivos_temp
    
    def limpar_temp(self):
        """Remove arquivos temporários"""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
            logger.info(f"Diretório temporário removido: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"Erro ao remover temp: {e}")
    
    def processar(self) -> int:
        """Processamento principal"""
        logger.info("=== PRÉ-PROCESSAMENTO SIH/SUS ===")
        logger.info(f"Entrada: {self.entrada}")
        logger.info(f"Saída: {self.saida}")
        logger.info(f"Chunk size: {self.chunk_size:,}")
        
        inicio = time.time()
        
        try:
            if self.saida.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup = self.saida.with_suffix(f'.backup_{timestamp}.parquet')
                self.saida.rename(backup)
                logger.info(f"Backup criado: {backup.name}")
            
            arquivos_temp = self.processar_salvar_chunk()
            
            logger.info("Unificando e salvando arquivo final...")
            
            # Lê os arquivos temporários de forma lazy para evitar estouro de memória
            df_lazy = pl.scan_parquet(arquivos_temp)
            df_final = df_lazy.collect()
            
            registros_finais = len(df_final)
            logger.info(f"Registros finais: {registros_finais:,}")

            df_final.write_parquet(self.saida, compression="snappy", use_pyarrow=True)
            
            tempo_total = time.time() - inicio
            tamanho_final_mb = self.saida.stat().st_size / (1024 * 1024)
            
            logger.info("="*60)
            logger.info("PROCESSAMENTO CONCLUÍDO!")
            logger.info("="*60)
            logger.info(f"Registros processados: {registros_finais:,}")
            logger.info(f"Tamanho final: {tamanho_final_mb:.1f} MB")
            logger.info(f"Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
            
            return registros_finais
            
        except Exception as e:
            logger.error(f"Erro: {e}")
            raise
        finally:
            self.limpar_temp()
            gc.collect()


def main():
    """Execução principal"""
    try:
        preprocessor = SIHPreprocessor(chunk_size=100_000)
        resultado = preprocessor.processar()
        print(f"\nSUCESSO! {resultado:,} registros processados.")
        
    except Exception as e:
        print(f"\nERRO: {e}")
        raise


if __name__ == "__main__":
    main()
