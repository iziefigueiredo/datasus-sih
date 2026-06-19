import polars as pl
import sys
from pathlib import Path
import logging
import time
import gc
import tempfile
from datetime import datetime

SRC_DIR = Path(__file__).parent.parent.parent  # src/
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class SIHPreprocessor:
    """Pré-processamento SIH/SUS com processamento em chunks"""
    
    def __init__(self, arquivo_entrada=None, arquivo_saida=None, chunk_size=100_000):
        self.entrada = arquivo_entrada or Settings.INTERIM_DIR / Settings.PARQUET_UNIFICADO_FILENAME
        self.saida = arquivo_saida or Settings.INTERIM_DIR / Settings.PARQUET_TRATADO_FILENAME
        self.chunk_size = chunk_size
        self.temp_dir = Path(tempfile.mkdtemp(prefix="sih_processing_"))
        Settings.create_directories()
        logger.info(f"Diretório temporário: {self.temp_dir}")
    
    def tratar_chunk_completo(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica todos os tratamentos a um chunk"""

        # === Conversão de campos booleanos ===
        campos_bool = ["MORTE", "IND_VDRL", "GESTRISCO"]
        for col in campos_bool:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .str.strip_chars()
                    .eq("1")
                    .alias(col)
                )
                
        # === Conversão padronizada de tipos numéricos ===
        cols_int64 = []  # N_AIH e PROC_REA tratados abaixo como Int64; CEP mantido String; CNES via Int32

       
        # Colunas com intervalos pequenos → Int16 
        cols_int16 = [
            "IDADE", "DIAS_PERM", "NACIONAL", "DIAR_ACOM",
        ]  #

        # Colunas com intervalos pequenos → Int8 
        cols_int8 = [
            "SEXO", "NUM_FILHOS", "INSTRU", "CONTRACEP1", "CONTRACEP2",
            "ESPEC", "UTI_INT_TO", "MARCA_UTI", "VINCPREV", "CAR_INT", "COD_IDADE", "COMPLEX", "RACA_COR", "ETNIA",
        ]  

       
        # Conversão dos grupos
        for col in cols_int64:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .str.strip_chars()
                    .cast(pl.Int64, strict=False)
                )

        

        for col in cols_int16:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .str.strip_chars()
                    .cast(pl.Int16, strict=False)
                )

        for col in cols_int8:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .str.strip_chars()
                    .cast(pl.Int8, strict=False)
                )
        
        # Converte campos de valor de texto para float, tratando vírgulas
        campos_valores = ['VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI']
        for col in campos_valores:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .str.replace_all(",", ".")
                    .str.replace_all(" ", "")
                    .str.replace_all("-", "")
                    .cast(pl.Float64, strict=False)
                    .alias(col)
                )

       
        
        # Trata campos de data
        campos_datas = ['DT_INTER', 'DT_SAIDA', 'NASC']
        for col in campos_datas:
            if col in df.columns:
                df = df.with_columns([
                    pl.col(col).cast(pl.String).str.strptime(pl.Date, format="%Y%m%d", strict=False)
                ])

        
        # Padronização dos códigos de município para 6 dígitos/ Mapeamento de valores não encontrado
        campos_municipio = ['MUNIC_RES', 'MUNIC_MOV']

        for col in campos_municipio:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .str.strip_chars()
                    .str.slice(0, 6)
                    .str.pad_start(length=6, fill_char='0')
                    .cast(pl.Int32, strict=False)
                    .clip(lower_bound=0)
                    .alias(col)
                )
        # === Normalização de IDADE por COD_IDADE ===
        # COD_IDADE: 2=dias, 3=meses, 4=anos, 5=mais de 100 anos
        if "IDADE" in df.columns and "COD_IDADE" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("COD_IDADE") == 5)
                .then(pl.col("IDADE") + pl.lit(100, dtype=pl.Int16))
                .when(pl.col("COD_IDADE") == 2)
                .then((pl.col("IDADE").cast(pl.Float32) / 365.25).round(0).cast(pl.Int16))
                .when(pl.col("COD_IDADE") == 3)
                .then((pl.col("IDADE").cast(pl.Float32) / 12).round(0).cast(pl.Int16))
                .otherwise(pl.col("IDADE"))
                .cast(pl.Int16)
                .alias("IDADE")
            )
        # NACIONAL: Int16  — zeros à esquerda são padding de char(3), não semânticos.

        # === Tratamento de RACA_COR e ETNIA ===
        # RACA_COR: Int8 — domínio válido 1-5; 99 = Sem informação → 0
        # ATENÇÃO: .clip() foi removido propositalmente.
        # .clip(0, 5) convertia 99 → 5 (Indígena) silenciosamente, inflando
        # a categoria indígena com todos os registros sem informação (~19-30%
        # dos dados brutos). Correção: valores fora de [1-5] → 0 (sentinela).
        #if "RACA_COR" in df.columns:
        #    df = df.with_columns(
        #        pl.col("RACA_COR")
        #        .cast(pl.Int8, strict=False)
        #        .alias("RACA_COR")
        #    )

        #if "ETNIA" in df.columns:
        #    df = df.with_columns(
        #        pl.col("ETNIA")
        #        .cast(pl.Int16, strict=False)
        #        .alias("ETNIA")
        #)

        # PROC_REA: string de 10 dígitos, zeros à esquerda obrigatórios. Zeros à esquerda são semânticos
        if "PROC_REA" in df.columns:
            df = df.with_columns(
                pl.col("PROC_REA")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .str.replace_all(r"[^0-9]", "")
                .str.pad_start(10, "0")
                .alias("PROC_REA")
            )
            
        # CEP: com zeros à esquerda 
        if "CEP" in df.columns:
            df = df.with_columns(
                pl.col("CEP")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .str.replace_all(r"[^0-9]", "")
                .str.pad_start(8, "0")
                .alias("CEP")
            )
        # CNES: com zeros à esquerda (7 dígitos)
        if "CNES" in df.columns:
            df = df.with_columns(
                pl.col("CNES")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .str.replace_all(r"[^0-9]", "")
                .str.pad_start(7, "0")
                .alias("CNES")
            )

        # N_AIH: Int64 — 13 dígitos, nenhuma UF começa com 0
        if "N_AIH" in df.columns:
            df = df.with_columns(
                pl.col("N_AIH")
                .cast(pl.Int64, strict=False)
                .alias("N_AIH")
            )
        # CBOR: alfanumérico CBO — String 
        if "CBOR" in df.columns:
            df = df.with_columns(
                pl.col("CBOR")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .alias("CBOR")
            )
        # CAR_INT: Int8 — domínio 1-6 pós Portaria 719/2007 . Zero à esquerda
        # no CSV ('01') é padding de char(2), não semântico. Tratado via cols_int8.
        # COMPLEX: C(02) String 
        if "COMPLEX" in df.columns:
            df = df.with_columns(
                pl.col("COMPLEX")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .str.pad_start(2, "0")
                .alias("COMPLEX")
            )
        # NAT_JUR: C(04) String — código CONCLA
        if "NAT_JUR" in df.columns:
            df = df.with_columns(
                pl.col("NAT_JUR")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .alias("NAT_JUR")
            )
        # CGC_HOSP: C(14) String — CNPJ do hospital, zeros à esquerda obrigatórios
        if "CGC_HOSP" in df.columns:
            df = df.with_columns(
                pl.col("CGC_HOSP")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .str.replace_all(r"[^0-9]", "")
                .str.pad_start(14, "0")
                .alias("CGC_HOSP")
            )
        
        # INSC_PN: String nullable
        if "INSC_PN" in df.columns:
            df = df.with_columns(
                pl.col("INSC_PN")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .pipe(lambda s:
                    pl.when(s.is_in([""]) | s.is_null())
                    .then(pl.lit(None))
                    .otherwise(s)
                )
                .alias("INSC_PN")
            )

    
        
        # Tratamento de campos CID
        campos_cid = ['DIAG_PRINC', 'DIAG_SECUN', 'CID_NOTIF', 'CID_MORTE',
              *[f"DIAGSEC{i}" for i in range(1, 10)]]
        for col in campos_cid:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .str.strip_chars()
                    .str.to_uppercase()
                    .alias(col)
                )

        # Garante que N_AIH não seja nulo
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
        
        logger.info(f"  {len(arquivos_temp)} chunks processados")
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
                
                Settings.BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
                backup = Settings.BACKUPS_DIR / f"{self.saida.stem}_backup_{timestamp}.parquet"

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
            
            logger.info(f"\n{'='*50}")
            logger.info(f"PRÉ-PROCESSAMENTO CONCLUÍDO")
            logger.info(f"  Saída:      {self.saida}")
            logger.info(f"  Registros:  {registros_finais:,}")
            logger.info(f"  Tamanho:    {tamanho_final_mb:.1f} MB")
            logger.info(f"  Tempo:      {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
            logger.info(f"{'='*50}")
            
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
    from config.logging_config import setup_logging
    setup_logging("preprocess")
    main()