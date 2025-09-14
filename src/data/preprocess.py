"""
Script para pré-processamento de dados SIH/SUS
"""

import polars as pl
import sys
from pathlib import Path
import logging
import time
import gc
import tempfile
from datetime import datetime
import re
import shutil

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class SIHPreprocessor:
    """Pré-processamento SIH/SUS com processamento em chunks"""
    
    def __init__(self, arquivo_entrada=None, arquivo_saida=None, chunk_size=100_000):
        self.entrada = arquivo_entrada or Settings.INTERIM_DIR / Settings.PARQUET_UNIFIED_FILENAME
        self.saida = arquivo_saida or Settings.INTERIM_DIR / Settings.PARQUET_TREATED_FILENAME
        self.chunk_size = chunk_size
        self.temp_dir = Path(tempfile.mkdtemp(prefix="sih_processing_"))
        Settings.criar_diretorios()
        logger.info(f"Diretório temporário: {self.temp_dir}")
    
    def tratar_chunk_completo(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica todos os tratamentos a um chunk"""
        
        if 'NUM_FILHOS' in df.columns:
            df = df.with_columns([
                pl.col("NUM_FILHOS").cast(pl.Int32, strict=False).fill_null(0).clip(0, None)
            ])
        
        if 'INSTRU' in df.columns:
            df = df.with_columns([
                pl.col("INSTRU").cast(pl.Utf8).str.zfill(2).str.replace_all("nan", "0").fill_null("00")
            ])
        
        if 'IDADE' in df.columns:
            df = df.with_columns([
                pl.col("IDADE").cast(pl.Float64, strict=False).fill_null(0.0)
            ])
            
            if 'COD_IDADE' in df.columns:
                df = df.with_columns([
                    pl.col("COD_IDADE").cast(pl.Int32, strict=False).fill_null(4)
                ])
                
                df = df.with_columns([
                    pl.when(pl.col("COD_IDADE") == 1)
                    .then(0.0)
                    .when(pl.col("COD_IDADE") == 2)  
                    .then((pl.col("IDADE") / 365.0).round(2))
                    .when(pl.col("COD_IDADE") == 3)
                    .then((pl.col("IDADE") / 12.0).round(2))
                    .otherwise(pl.col("IDADE"))
                    .clip(0, 120)
                    .alias("IDADE")
                ])
                
                df = df.drop("COD_IDADE")
        
        if 'SEXO' in df.columns:
            df = df.with_columns([
                pl.col("SEXO").cast(pl.Int32, strict=False).fill_null(0).clip(0, 3)
            ])
        
        campos_datas = ['DT_INTER', 'DT_SAIDA', 'NASC']
        for col in campos_datas:
            if col in df.columns:
                df = df.with_columns([
                    pl.col(col).cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d", strict=False)
                ])
        
        campos_valores = ['VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI']
        for col in campos_valores:
            if col in df.columns:
                df = df.with_columns([
                    pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0).clip(0, None)
                ])
        
        campos_inteiros = ['QT_DIARIAS', 'DIAS_PERM', 'UTI_MES_TO', 'UTI_INT_TO', 'DIAR_ACOM']
        for col in campos_inteiros:
            if col in df.columns:
                df = df.with_columns([
                    pl.col(col).cast(pl.Int32, strict=False).fill_null(0).clip(0, None)
                ])
        
        campos_cid = ['DIAG_PRINC', 'DIAG_SECUN', 'CID_NOTIF', 'CID_ASSO', 'CID_MORTE']

        for col in campos_cid:

            if col in df.columns:

                def tratar_cid(valor):

                    if valor is None:
                        return "0"
            
                    valor_str = str(valor).strip().upper()
            
                    if valor_str in ["", "0", "00", "000", "0000", "00000", "000000"]:
                        return "0"
                    return valor_str

                df = df.with_columns([
                        pl.col(col).map_elements(tratar_cid, skip_nulls=False, return_dtype=pl.String).alias(col)
                    ])
        
        # Tratamento da coluna CGC_HOSP (CNPJ do hospital)
        if 'CGC_HOSP' in df.columns:
            df = df.with_columns(
                pl.col("CGC_HOSP")
                .cast(pl.Utf8, strict=False)
                .str.replace_all(r'\D', '')
                .str.zfill(14)
                .alias("CGC_HOSP")
            )
        
        # Tratamento da coluna NAT_JUR (Natureza Jurídica)
        if 'NAT_JUR' in df.columns:
            df = df.with_columns(
                pl.col("NAT_JUR")
                .cast(pl.Utf8, strict=False)
                .str.replace_all(r'\D', '')
                .alias("NAT_JUR")
            )
        
        df = df.filter(pl.col("N_AIH").is_not_null())
        
        return df
    
    def processar_e_salvar_chunks(self) -> list:
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
    
    def agregar_chunks_finais(self, arquivos_temp: list) -> pl.DataFrame:
        """
        Lê todos os chunks temporários e aplica uma única agregação final,
        incluindo a lógica para selecionar o PROC_REA de maior valor.
        """
        logger.info("=== FASE 2: Agregação Final ===")
        
        # Usa scan_parquet para ler todos os arquivos sem carregar tudo na memória de uma vez
        df_lazy = pl.scan_parquet(arquivos_temp)
        
        # Define as colunas para cada tipo de agregação
        colunas_soma = ['VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI', 'DIAS_PERM']
        colunas_media = ['UTI_MES_TO', 'UTI_INT_TO', 'DIAR_ACOM', 'IDADE']
        colunas_moda = ['CGC_HOSP', 'NAT_JUR']
        
        # Constrói a lista de expressões de agregação dinamicamente
        aggregations = []
        
        # Agregações de SOMA
        for col in colunas_soma:
            if col in df_lazy.columns:
                aggregations.append(pl.col(col).sum().alias(col))
                
        # Agregações de MÉDIA
        for col in colunas_media:
            if col in df_lazy.columns:
                aggregations.append(pl.col(col).mean().round(1).alias(col))

        # Agregações de MODA
        for col in colunas_moda:
            if col in df_lazy.columns:
                aggregations.append(pl.col(col).mode().first().alias(col))
                
        # LÓGICA ESPECIAL PARA PROC_REA: Pega o PROC_REA correspondente ao maior VAL_TOT
        if 'PROC_REA' in df_lazy.columns and 'VAL_TOT' in df_lazy.columns:
            logger.info("Aplicando lógica para PROC_REA de maior valor...")
            # Ordena por VAL_TOT decrescente dentro de cada grupo e pega o primeiro PROC_REA
            aggregations.append(
                pl.col('PROC_REA').sort_by(pl.col('VAL_TOT'), descending=True).first().alias('PROC_REA')
            )
        
        # Agregações de FIRST (para todas as outras colunas)
        colunas_ja_agregadas = set(colunas_soma + colunas_media + colunas_moda + ['N_AIH', 'PROC_REA'])
        outras_colunas = [col for col in df_lazy.columns if col not in colunas_ja_agregadas]
        
        for col in outras_colunas:
            aggregations.append(pl.col(col).first().alias(col))
            
        # Executa a agregação
        logger.info("Executando a agregação final para todas as colunas...")
        df_final = df_lazy.group_by('N_AIH').agg(aggregations).collect()
        
        logger.info(f"Agregação concluída. Total de AIHs únicas: {len(df_final):,}")
        
        return df_final
    
    def limpar_temp(self):
        """Remove arquivos temporários"""
        try:
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
            
            # FASE 1: Limpa e padroniza os dados em chunks
            arquivos_temp = self.processar_e_salvar_chunks()
            
            # FASE 2: Agrega todos os chunks de uma vez
            df_final = self.agregar_chunks_finais(arquivos_temp)
            
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
            logger.error(f"Erro no processamento: {e}")
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