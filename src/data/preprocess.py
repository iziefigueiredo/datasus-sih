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
        self.saida = arquivo_saida or Settings.INTERIM_DIR / "sih_rs_tratado.parquet"
        self.chunk_size = chunk_size
        self.temp_dir = Path(tempfile.mkdtemp(prefix="sih_processing_"))
        Settings.criar_diretorios()
        logger.info(f"Diretório temporário: {self.temp_dir}")
    
    def tratar_chunk_completo(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica todos os tratamentos a um chunk"""
                
        # === Conversão padronizada de tipos numéricos ===
        cols_int64 = [
            "N_AIH", "CNES", "CEP", "PROC_REA"
        ]

        # Colunas de contagem ou valores médios → Int32
        cols_int32 = [
            "DIAR_ACOM", "UTI_MES_TO", "UTI_INT_TO", "codigo_6d", "NAT_JUR", "CBOR"
        ]

        # Colunas com intervalos pequenos → Int16 
        cols_int16 = [
            "IDADE", "DIAS_PERM", "NACIONAL", "GESTAO", "NATUREZA"
        ]

        # Colunas com intervalos pequenos → Int8 
        cols_int8 = [
            "SEXO", "NUM_FILHOS", "ETNIA", "RACA_COR", "INSTRU", "COMPLEX"
        ]

       

        # Conversão dos grupos, respeitando a existência das colunas
        for col in cols_int64:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False).fill_null(0))

        for col in cols_int32:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Int32, strict=False).fill_null(0))

        for col in cols_int16:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Int16, strict=False).fill_null(0))

        for col in cols_int8:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Int8, strict=False).fill_null(0))

        
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
                    .fill_null(0.0)
                    .clip(lower_bound=0.0)
                    .alias(col)
                )

        if 'VAL_UTI' not in df.columns:
            df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias('VAL_UTI'))

        # ETAPA 2: Recalcula e substitui 'VAL_TOT' pela soma dos componentes.
        # Esta é a nova "fonte da verdade" para o valor total.
        df = df.with_columns(
            (
                pl.col("VAL_SH") + 
                pl.col("VAL_SP") + 
                pl.col("VAL_UTI")
            ).alias("VAL_TOT")
        )

        # Trata campos de data
        campos_datas = ['DT_INTER', 'DT_SAIDA', 'NASC']
        for col in campos_datas:
            if col in df.columns:
                df = df.with_columns([
                    pl.col(col).cast(pl.String).str.strptime(pl.Date, format="%Y%m%d", strict=False)
                ])

        # Calcula a idade de forma precisa
        if 'DT_INTER' in df.columns and 'NASC' in df.columns:
            df = df.with_columns([
                (pl.col("DT_INTER").dt.year() - pl.col("NASC").dt.year() -
                 pl.when(
                     (pl.col("DT_INTER").dt.month() < pl.col("NASC").dt.month()) |
                     ((pl.col("DT_INTER").dt.month() == pl.col("NASC").dt.month()) &
                      (pl.col("DT_INTER").dt.day() < pl.col("NASC").dt.day()))
                 )
                 .then(1)
                 .otherwise(0)
                )
                .clip(0, 150)
                .cast(pl.Int16)
                .alias("IDADE")
            ])
            
        # Calcula DIAS_PERM a partir das datas
        if 'DT_INTER' in df.columns and 'DT_SAIDA' in df.columns:
            df = df.with_columns(
                (pl.col("DT_SAIDA") - pl.col("DT_INTER")).dt.total_days().alias("DIAS_PERM")
            )
            df = df.with_columns(
                pl.col("DIAS_PERM").cast(pl.Int16, strict=False).fill_null(0)
            )

        # Padronização dos códigos de município para 6 dígitos/ Mapeamento de valores não encontrado
        # === Tratamento dos códigos de município  ===
        campos_municipio = ['MUNIC_RES', 'MUNIC_MOV']

        # Padronização dos códigos de município para 6 dígitos/ Mapeamento de valores não encontrado
        for col in campos_municipio:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)
                    .str.strip_chars()
                    .fill_null("000000")
                    
                    # --- LÓGICA DE GENERALIZAÇÃO PARA O DISTRITO FEDERAL ---
                    .pipe(lambda s:
                        # SE o código começar com '53' (prefixo do DF)
                        pl.when(s.str.starts_with("53"))
                        .then(pl.lit("530010"))  # ENTÃO, substitui pelo código unificado de Brasília
                        .otherwise(s)           # SENÃO, mantém o código original
                    )
                    
                    # Continua com a padronização geral para 6 dígitos
                    .str.slice(0, 6)
                    .str.pad_start(length=6, fill_char='0')

                    .cast(pl.Int64, strict=False)  # Tenta converter o string limpo para inteiro de 64 bits
                    .fill_null(0)                  # Preenche 0 (código ignorado) caso o cast falhe
                    .clip(lower_bound=0)           # Garante que não há valores negativos (correção do clip)
                    
                    .alias(col)
                )


        # Tratamento da coluna NACIONAL
        if 'NACIONAL' in df.columns:
            df = df.with_columns(
                # 1. Usa a lógica WHEN/THEN/OTHERWISE como a expressão principal
                pl.when(pl.col("NACIONAL") == 0)
                .then(pl.lit(10)) # Substitui 0 por 10
                .otherwise(pl.col("NACIONAL"))
                
                # 2. Aplica o limite no resultado da condição

                .clip(lower_bound=0, upper_bound=350) 
                
                .alias("NACIONAL") # Nomeia a coluna final
            )

        # === Tratamento de RACA_COR e ETNIA ===
        if "RACA_COR" in df.columns and "ETNIA" in df.columns:
            df = df.with_columns([
                pl.col("RACA_COR")
                .cast(pl.Int8, strict=False)
                .fill_null(0)
                .clip(lower_bound=0, upper_bound=99)
                .alias("RACA_COR"),

                pl.col("ETNIA")
                .cast(pl.Int16, strict=False)  # precisa ser Int16 pois vai até 264
                .fill_null(0)
                .clip(lower_bound=0, upper_bound=264)
                .alias("ETNIA")
            ])

            # Se RACA_COR != 5 e ETNIA for válida (≠ 0)
            df = df.with_columns(
                pl.when((pl.col("RACA_COR") != 5) & (pl.col("ETNIA") != 0))
                .then(pl.lit(5))
                .otherwise(pl.col("RACA_COR"))
                .alias("RACA_COR")
            )


        # Padronização do código de procedimento (PROC_REA)
        if "PROC_REA" in df.columns:
            df = df.with_columns(
                pl.col("PROC_REA")
                .cast(pl.Int64, strict=False)
                .fill_null(0)
                .clip(lower_bound=0)
                .alias("PROC_REA")
            )

    
        
        # Tratamento de campos CID
        campos_cid = ['DIAG_PRINC', 'DIAG_SECUN', 'CID_NOTIF', 'CID_ASSO', 'CID_MORTE']
        for col in campos_cid:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String, strict=False)  # Garante que é texto
                    .str.strip_chars()              # Remove espaços, transformando '  ' em ''
                    .str.to_uppercase()             # Converte para maiúsculas
                    
                    # --- LÓGICA DE PADRONIZAÇÃO APRIMORADA ---
                    .pipe(lambda s: 
                        # SE a string for vazia, nula, OU contiver apenas zeros (ex: '0', '00', '0000')
                        pl.when(
                            s.is_in([""]) | 
                            s.is_null() | 
                            s.str.contains(r"^0+$") # Regex: ^ (início), 0+ (um ou mais zeros), $ (fim)
                        )
                        .then(pl.lit("0"))  # ENTÃO, substitui por um único '0'
                        .otherwise(s)       # SENÃO, mantém o valor original
                    )
                    .alias(col)
                )

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
