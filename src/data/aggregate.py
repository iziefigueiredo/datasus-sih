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
        """
        Executa a contração dos dados, agregando por N_AIH e recalculando
        campos derivados para garantir consistência.
        """
        inicio = time.time()
        
        try:
            logger.info("Lendo arquivo de entrada de forma lazy...")
            df_lazy = pl.scan_parquet(self.entrada)
            
            # Usar collect_schema() para obter nomes de colunas de forma eficiente
            schema_cols = df_lazy.collect_schema().names()

            # --- PASSO 1: Definir a estratégia de agregação ---
            # Colunas a serem somadas
            colunas_soma = ['VAL_SH', 'VAL_SP', 'VAL_UTI'] # VAL_TOT será recalculado
            
            # Colunas a serem agregadas com a média
            colunas_media = ['UTI_MES_TO', 'UTI_INT_TO', 'DIAR_ACOM']
            
            # Colunas que serão recalculadas DEPOIS da agregação
            colunas_recalculadas = ['IDADE', 'DIAS_PERM', 'VAL_TOT']

            aggregations = []
            for col in schema_cols:
                # Ignora a chave de agrupamento e as colunas que serão recalculadas
                if col == 'N_AIH' or col in colunas_recalculadas:
                    continue
                
                if col in colunas_soma:
                    aggregations.append(pl.col(col).sum().alias(col))
                elif col in colunas_media:
                    aggregations.append(pl.col(col).mean().alias(col))
                else:
                    # Para todas as outras colunas (DT_INTER, NASC, etc.), pega o primeiro valor
                    aggregations.append(pl.col(col).first().alias(col))

            # --- PASSO 2: Executar a agregação principal ---
            logger.info("Executando agregação principal...")
            df_contraido_lazy = df_lazy.group_by("N_AIH").agg(aggregations)

            # --- PASSO 3: Recalcular colunas derivadas para garantir consistência ---
            logger.info("Recalculando VAL_TOT, DIAS_PERM e IDADE para garantir consistência...")
            df_final_lazy = df_contraido_lazy.with_columns(
                # Recalcula VAL_TOT
                (pl.col("VAL_SH") + pl.col("VAL_SP") + pl.col("VAL_UTI")).alias("VAL_TOT"),
                
                # Recalcula DIAS_PERM
                (pl.col("DT_SAIDA") - pl.col("DT_INTER")).dt.total_days().cast(pl.Int16, strict=False).fill_null(0).alias("DIAS_PERM"),
                
                # Recalcula IDADE
                (
                    pl.col("DT_INTER").dt.year() - pl.col("NASC").dt.year() -
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
            )

            # --- PASSO 4: Coletar e salvar o resultado final ---
            logger.info("Coletando resultados e salvando arquivo final...")
            df_final = df_final_lazy.collect()
            
            registros_finais = len(df_final)
            
            logger.info(f"Salvando arquivo final: {self.saida}")
            df_final.write_parquet(self.saida, compression="snappy")
            
            # --- Relatório Final ---
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
            logger.error(f"Erro na contração: {e}", exc_info=True) # Adicionado exc_info=True para mais detalhes
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