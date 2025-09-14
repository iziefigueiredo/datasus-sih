import polars as pl
import sys
from pathlib import Path
import logging
import gc
import pandas as pd

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))

from config.settings import Settings
from database.schema import TABLE_SCHEMAS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class TableSplitter:
    def __init__(self):
        self.input_parquet_path = Settings.PARQUET_INTERIM_DIR / Settings.PARQUET_TREATED_FILENAME
        self.output_dir = Settings.PARQUET_PROCESSED_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.internacoes_cols = [
            "CNES", "N_AIH", "ESPEC", "IDENT", "DT_INTER", "DT_SAIDA", 
            "PROC_REA", "VAL_SH", "VAL_SP", "VAL_TOT", "COBRANCA", "DIAS_PERM",
            "COMPLEX", "CID_NOTIF", "MUNIC_MOV", "DIAG_PRINC", "DIAG_SECUN", "CID_ASSO",
            "NASC", "SEXO", "IDADE", "NACIONAL", "NUM_FILHOS", "RACA_COR", "MUNIC_RES", "CEP"
        ]

        self.uti_detalhes_cols = [
            "N_AIH", "UTI_MES_TO", "MARCA_UTI", "UTI_INT_TO"
        ]

        self.condicoes_especificas_cols = [
            "N_AIH", "IND_VDRL"
        ]

        self.hospital_cols = [
            "CNES", "NATUREZA", "GESTAO", "NAT_JUR"
        ]

        self.obstetricos_cols = [
            "N_AIH", "INSC_PN"
        ]

        self.instrucao_cols = [
            "N_AIH", "INSTRU"
        ]

        self.mortes_cols = [
            "N_AIH", "CID_MORTE"
        ]

    # As funções abaixo, como split_condicoes_especificas, também foram ajustadas
    # para usar as variáveis de nome de arquivo definidas em Settings, quando disponíveis.
    # Isso torna seu código mais consistente.

    def split_internacoes(self):
        table_name = "internacoes"
        output_file = self.output_dir / Settings.INTERNACOES_FILENAME
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            df = pl.read_parquet(self.input_parquet_path, columns=self.internacoes_cols)
            df = df.select(self.internacoes_cols)
            df.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise

    def split_uti_detalhes(self):
        table_name = "uti_detalhes"
        output_file = self.output_dir / Settings.UTI_DETALHES_FILENAME
        schema_info = TABLE_SCHEMAS.get(table_name)
        if not schema_info:
            logger.error(f"Esquema para a tabela '{table_name}' não encontrado. Ignorando.")
            return
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            cols = [col for col in schema_info["columns"].keys() if col != "id"]
            if "N_AIH" not in cols:
                cols.insert(0, "N_AIH")
            df = pl.read_parquet(self.input_parquet_path, columns=cols)
            df = df.with_columns([
                pl.col("UTI_MES_TO").cast(pl.Int32, strict=False),
                pl.col("UTI_INT_TO").cast(pl.Int32, strict=False),
                pl.col("VAL_UTI").cast(pl.Float64, strict=False) if "VAL_UTI" in df.columns else pl.lit(0.0).alias("VAL_UTI"),
            ])
            df = df.filter(pl.col("VAL_UTI") > 0).unique(subset=["N_AIH"], keep="first")
            df = df.select(cols)
            df.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise

    def split_condicoes_especificas(self):
        table_name = "condicoes_especificas"
        output_file = self.output_dir / Settings.CONDICOES_ESPECIFICAS_FILENAME
        schema_info = TABLE_SCHEMAS.get(table_name)
        if not schema_info:
            logger.error(f"Esquema para a tabela '{table_name}' não encontrado. Ignorando.")
            return
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            cols = [col for col in schema_info["columns"].keys() if col != "id"]
            if "N_AIH" not in cols:
                cols.insert(0, "N_AIH")
            df = pl.read_parquet(self.input_parquet_path, columns=cols)
            df = df.filter(pl.col("IND_VDRL") == "1").unique(subset=["N_AIH"], keep="first")
            df = df.select(cols)
            df.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise

    def split_hospital(self):
        table_name = "hospital"
        output_file = self.output_dir / Settings.HOSPITAL_FILENAME 
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            df = pl.read_parquet(self.input_parquet_path, columns=self.hospital_cols)
            df_pd = df.to_pandas()
            df_grouped = df_pd.groupby("CNES").agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None).reset_index()
            df_result = pl.from_pandas(df_grouped)
            df_result.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df_result):,} registros salvos.")
            del df, df_pd, df_grouped, df_result
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise

    def split_obstetricos(self):
        table_name = "obstetricos"
        output_file = self.output_dir / Settings.OBSTETRICOS_FILENAME 
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            df = pd.read_parquet(self.input_parquet_path, columns=["N_AIH", "INSC_PN"])
            df["N_AIH"] = df["N_AIH"].astype(str).str.strip()
            df["INSC_PN"] = df["INSC_PN"].astype(str).str.strip()

            df = df[
                df["INSC_PN"].notna() &
                (df["INSC_PN"] != "") &
                (df["INSC_PN"] != "0") &
                (~df["INSC_PN"].str.fullmatch(r"0+"))
            ]

            df.to_parquet(output_file, index=False)
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise

    def split_instrucao(self):
        table_name = "instrucao"
        output_file = self.output_dir / Settings.INSTRUCAO_FILENAME
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")

        try:
            df = pl.read_parquet(self.input_parquet_path, columns=["N_AIH", "INSTRU"]).with_columns(
                pl.col("N_AIH").cast(pl.String, strict=False),
                pl.col("INSTRU").cast(pl.String, strict=False)
            )

            total = df.height
            nao_nulos = df.filter(pl.col("INSTRU").is_not_null()).height
            validos = df.filter(pl.col("INSTRU").is_not_null() & (pl.col("INSTRU") != "00")).height
            logger.info(f"instrucao: total={total:,} | INSTRU!=NULL={nao_nulos:,} | INSTRU!='00'={validos:,}")

            df = df.filter(pl.col("INSTRU").is_not_null() & (pl.col("INSTRU") != "00"))

            df = df.unique(subset=["N_AIH"], keep="first").select(["N_AIH", "INSTRU"])

            df.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos em {output_file.name}.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise
    
    def split_mortes(self):
        table_name = "mortes"
        output_file = self.output_dir / Settings.MORTES_FILENAME 
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            df = pl.read_parquet(self.input_parquet_path, columns=["N_AIH", "MORTE", "CID_MORTE"])

            df = df.filter(pl.col("MORTE") == "1")

            df = df.select(["N_AIH", "CID_MORTE"]).unique(subset=["N_AIH"], keep="first")

            df.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise

    def split_infehosp(self):
        table_name = "infehosp"
        output_file = self.output_dir / Settings.INFEHOSP_FILENAME 
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            df = pl.read_parquet(self.input_parquet_path, columns=["N_AIH", "INFEHOSP"]).with_columns(
                pl.col("N_AIH").cast(pl.String, strict=False),
                pl.col("INFEHOSP").cast(pl.Int32, strict=False).fill_null(0)
            )

            df = df.filter(pl.col("INFEHOSP") == 1)

            df = df.unique(subset=["N_AIH"], keep="first").select(["N_AIH", "INFEHOSP"])

            df.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise


    def split_vincprev(self):
        table_name = "vincprev"
        # AQUI FOI AJUSTADO PARA USAR A VARIÁVEL DO SETTINGS
        output_file = self.output_dir / Settings.VINCPREV_FILENAME 
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            df = pl.read_parquet(self.input_parquet_path, columns=["N_AIH", "VINCPREV"]).with_columns(
                pl.col("N_AIH").cast(pl.String, strict=False),
                pl.col("VINCPREV").cast(pl.String, strict=False).str.strip_chars().str.to_uppercase()
            )

            df = df.filter(
                pl.col("VINCPREV").is_not_null()
                & (pl.col("VINCPREV") != "")
                & (~pl.col("VINCPREV").str.contains(r"^0+$"))
            )

            df = df.unique(subset=["N_AIH"], keep="first").select(["N_AIH", "VINCPREV"])

            df.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise

    def split_cbor(self):
        table_name = "cbor"
        # AQUI FOI AJUSTADO PARA USAR A VARIÁVEL DO SETTINGS
        output_file = self.output_dir / Settings.CBOR_FILENAME
        logger.info(f"Iniciando divisão para a tabela '{table_name}'...")
        try:
            df = pl.read_parquet(self.input_parquet_path, columns=["N_AIH", "CBOR"]).with_columns(
                pl.col("N_AIH").cast(pl.String, strict=False),
                pl.col("CBOR").cast(pl.String, strict=False).str.strip_chars().str.to_uppercase()
            )

            df = df.filter(
                pl.col("CBOR").is_not_null()
                & (pl.col("CBOR") != "")
                & (~pl.col("CBOR").str.contains(r"^0+$"))
            )

            df = df.unique(subset=["N_AIH"], keep="first").select(["N_AIH", "CBOR"])

            df.write_parquet(output_file, compression="snappy")
            logger.info(f"Divisão para '{table_name}' concluída. {len(df):,} registros salvos.")
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Erro durante a divisão para '{table_name}': {e}")
            raise

    def run(self):
        logger.info("=== INICIANDO DIVISÃO DE ARQUIVOS PARA CARGA NO BANCO ===")
        self.split_internacoes()
        self.split_uti_detalhes()
        self.split_condicoes_especificas()
        self.split_hospital()
        self.split_obstetricos()
        self.split_instrucao()
        self.split_mortes()
        self.split_infehosp()
        self.split_vincprev()
        self.split_cbor()
        logger.info("=== DIVISÃO DE ARQUIVOS CONCLUÍDA ===")

def main():

    splitter = TableSplitter()
    splitter.run()

if __name__ == "__main__":
    main()