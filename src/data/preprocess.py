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

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class SIHPreprocessor:
    """Pré-processamento SIH/SUS com processamento em chunks"""
    
    def __init__(self, arquivo_entrada=None, arquivo_saida=None, chunk_size=100_000):
        self.entrada = arquivo_entrada or Settings.INTERIM_DIR / "sih_rs.parquet"
        self.saida = arquivo_saida or Settings.INTERIM_DIR / "sih_rs_tratado.parquet"
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
    
    def contrair_por_lotes(self, arquivos_temp: list) -> Path:
        """Contrai dados processando poucos arquivos por vez"""
        logger.info("=== FASE 2: Contração por Lotes ===")
        
        colunas_soma = ['VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI',  'DIAS_PERM']
        colunas_media = ['UTI_MES_TO', 'UTI_INT_TO', 'DIAR_ACOM', 'IDADE']
        
        lote_size = 5
        arquivos_contraidos = []
        
        for i in range(0, len(arquivos_temp), lote_size):
            lote = arquivos_temp[i:i+lote_size]
            lote_num = (i // lote_size) + 1
            
            logger.info(f"Contraindo lote {lote_num} ({len(lote)} arquivos)...")
            
            dfs = []
            for arquivo in lote:
                df = pl.read_parquet(arquivo)
                dfs.append(df)
            
            df_lote = pl.concat(dfs, how="vertical_relaxed")
            del dfs
            gc.collect()
            
            aggregations = []
            
            for col in df_lote.columns:
                if col == 'N_AIH':
                    continue
                elif col in colunas_soma:
                    aggregations.append(pl.col(col).sum().alias(col))
                elif col in colunas_media:
                    aggregations.append(pl.col(col).mean().round(1).alias(col))
                else:
                    aggregations.append(pl.col(col).first().alias(col))
            
            df_contraido = df_lote.group_by('N_AIH').agg(aggregations)
            
            arquivo_contraido = self.temp_dir / f"contraido_{lote_num:03d}.parquet"
            df_contraido.write_parquet(arquivo_contraido, compression="snappy")
            arquivos_contraidos.append(arquivo_contraido)
            
            del df_lote, df_contraido
            gc.collect()
        
        logger.info(f"{len(arquivos_contraidos)} lotes contraídos")
        
        if len(arquivos_contraidos) > 1:
            logger.info("=== FASE 3: Contração Final ===")
            return self.contracao_final(arquivos_contraidos)
        else:
            return arquivos_contraidos[0]
    
    def contracao_final(self, arquivos_contraidos: list) -> Path:
        """Contração final de todos os lotes"""
        
        colunas_soma = ['VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI', 'DIAS_PERM']
        colunas_media = ['UTI_MES_TO', 'UTI_INT_TO', 'DIAR_ACOM', 'IDADE']
        
        grupos_size = 3
        arquivos_finais = []
        
        for i in range(0, len(arquivos_contraidos), grupos_size):
            grupo = arquivos_contraidos[i:i+grupos_size]
            grupo_num = (i // grupos_size) + 1
            
            logger.info(f"Contração final - grupo {grupo_num}...")
            
            dfs = [pl.read_parquet(arquivo) for arquivo in grupo]
            df_grupo = pl.concat(dfs, how="vertical_relaxed")
            del dfs
            gc.collect()
            
            aggregations = []
            for col in df_grupo.columns:
                if col == 'N_AIH':
                    continue
                elif col in colunas_soma:
                    aggregations.append(pl.col(col).sum().alias(col))
                elif col in colunas_media:
                    aggregations.append(pl.col(col).mean().round(1).alias(col))
                else:
                    aggregations.append(pl.col(col).first().alias(col))
            
            df_final_grupo = df_grupo.group_by('N_AIH').agg(aggregations)
            
            arquivo_final = self.temp_dir / f"final_{grupo_num:03d}.parquet"
            df_final_grupo.write_parquet(arquivo_final, compression="snappy")
            arquivos_finais.append(arquivo_final)
            
            del df_grupo, df_final_grupo
            gc.collect()
        
        if len(arquivos_finais) > 1:
            logger.info("Concatenação final...")
            dfs_finais = [pl.read_parquet(arquivo) for arquivo in arquivos_finais]
            df_final = pl.concat(dfs_finais, how="vertical_relaxed")
            del dfs_finais
            gc.collect()
            
            aihs_unicas = df_final['N_AIH'].n_unique()
            if len(df_final) > aihs_unicas:
                logger.info("Contração final necessária...")
                
                aggregations = []
                for col in df_final.columns:
                    if col == 'N_AIH':
                        continue
                    elif col in colunas_soma:
                        aggregations.append(pl.col(col).sum().alias(col))
                    elif col in colunas_media:
                        aggregations.append(pl.col(col).mean().round(1).alias(col))
                    else:
                        aggregations.append(pl.col(col).first().alias(col))
                
                df_final = df_final.group_by('N_AIH').agg(aggregations)
            
            arquivo_resultado = self.temp_dir / "resultado_final.parquet"
            df_final.write_parquet(arquivo_resultado, compression="snappy")
            return arquivo_resultado
        else:
            return arquivos_finais[0]
    
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
            
            arquivos_temp = self.processar_e_salvar_chunks()
            arquivo_final = self.contrair_por_lotes(arquivos_temp)
            
            logger.info("Salvando arquivo final...")
            df_resultado = pl.read_parquet(arquivo_final)
            
            registros_finais = len(df_resultado)
            logger.info(f"Registros finais: {registros_finais:,}")
            
            df_resultado.write_parquet(self.saida, compression="snappy", use_pyarrow=True)
            
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