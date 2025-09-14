"""
Script para unificar arquivos parquet individuais em um único arquivo
Localização: projeto_sih/src/data/unify.py
Função: TRANSFORM - Une múltiplos arquivos .parquet em 1 arquivo único
"""
import polars as pl
import gc
import time
import sys
from pathlib import Path
from typing import List, Optional
import logging
from glob import glob

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SIHUnifier:
    """
    Classe para unificar múltiplos arquivos parquet em um único arquivo
    """
    
    def __init__(
        self,
        pasta_entrada: Optional[Path] = None,
        arquivo_saida: Optional[Path] = None,
        lote_size: int = 50
    ):
        self.pasta_entrada = pasta_entrada or Settings.RAW_DIR
        self.arquivo_saida = arquivo_saida or Settings.INTERIM_DIR/ "sih_rs.parquet"
        self.lote_size = lote_size
        
        self.colunas_desejadas = [
            'ESPEC', 'N_AIH', 'IDENT', 'CEP', 'MUNIC_RES', 'NASC', 'SEXO', 'DT_INTER', 'DT_SAIDA',
            'UTI_MES_TO', 'MARCA_UTI', 'UTI_INT_TO', 'DIAR_ACOM', 'QT_DIARIAS', 'PROC_REA', 
            'VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI', 'NATUREZA', 'CNES', 'NAT_JUR', 'GESTAO', 'IND_VDRL', 
            'IDADE', 'DIAG_PRINC', 'DIAG_SECUN', 'COBRANCA', 'MORTE', 'MUNIC_MOV', 'DIAS_PERM', 'NACIONAL', 
            'NUM_FILHOS', 'INSTRU', 'CID_NOTIF', 'CONTRACEP1', 'CONTRACEP2', 'GESTRICO', 'INSC_PN', 'CBOR',
            'CNAER', 'VINCPREV', 'INFEHOSP', 'CID_ASSO', 'CID_MORTE', 'COMPLEX', 'RACA_COR', 'ETNIA',
            'DIAGSEC1', 'DIAGSEC2', 'DIAGSEC3', 'DIAGSEC4', 'DIAGSEC5', 'DIAGSEC6', 'DIAGSEC7', 'DIAGSEC8', 'DIAGSEC9', 
            'COD_IDADE', 'CGC_HOSP'
        ]
        
        Settings.criar_diretorios()
    
    def buscar_arquivos_parquet(self) -> List[Path]:
        """Busca todos os arquivos .parquet na pasta de entrada"""
        logger.info(f"Buscando arquivos parquet em: {self.pasta_entrada}")
        
        padroes = [
            str(self.pasta_entrada / "**/*.parquet"),
            str(self.pasta_entrada / "*.parquet")
        ]
        
        arquivos = []
        for padrao in padroes:
            arquivos.extend(glob(padrao, recursive=True))
        
        arquivos_path = list(set(Path(arquivo) for arquivo in arquivos))
        arquivos_path.sort()
        
        logger.info(f"Encontrados {len(arquivos_path)} arquivos parquet")
        return arquivos_path
    
    def unificar_com_polars_relaxed(self) -> int:
        """Unifica arquivos usando polars com vertical_relaxed"""
        logger.info("Usando polars com vertical_relaxed")
        
        arquivos = self.buscar_arquivos_parquet()
        
        if not arquivos:
            raise FileNotFoundError("Nenhum arquivo parquet encontrado!")
        
        logger.info(f"Processando {len(arquivos)} arquivos...")
        
        lazy_frames = []

        arquivos_processados = 0

        arquivos_com_erro = 0
        
        for i, arquivo in enumerate(arquivos):
            
            try:
                df_lazy = pl.scan_parquet(arquivo)
                schema = df_lazy.collect_schema()

                colunas_presentes = list(schema.keys())
                colunas_faltando = [col for col in self.colunas_desejadas if col not in colunas_presentes]

                for col in colunas_faltando:
                    df_lazy = df_lazy.with_columns(pl.lit(None).alias(col))

                df_lazy = df_lazy.select(self.colunas_desejadas)
                lazy_frames.append(df_lazy)
                arquivos_processados += 1

            except Exception as e:
                arquivos_com_erro += 1
                continue
        

        
        logger.info(f"Resumo: {arquivos_processados}/{len(arquivos)} arquivos processados")
        if arquivos_com_erro > 0:
            logger.warning(f"Arquivos com erro: {arquivos_com_erro}")
        
        if not lazy_frames:
            raise RuntimeError("Nenhum arquivo foi processado com sucesso!")
        
        logger.info(f"Concatenando {len(lazy_frames)} DataFrames com vertical_relaxed...")
        df_unificado = pl.concat(lazy_frames, how="vertical_relaxed")
        
        self.arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info("Salvando arquivo unificado...")
        df_unificado.sink_parquet(
            self.arquivo_saida,
            compression="snappy",
            maintain_order=False
        )
        
        registros_totais = pl.scan_parquet(self.arquivo_saida).select(pl.len()).collect().item()
        
        return registros_totais
    
    def unificar(self, usar_lazy: bool = True) -> int:
        """Método principal para unificar arquivos"""
        logger.info("=== UNIFICAÇÃO DE ARQUIVOS SIH/SUS ===")
        logger.info(f"Pasta entrada: {self.pasta_entrada}")
        logger.info(f"Arquivo saída: {self.arquivo_saida}")
        
        inicio = time.time()
        
        try:
            registros_totais = self.unificar_com_polars_relaxed()
            
            tempo_total = time.time() - inicio
            tamanho_mb = self.arquivo_saida.stat().st_size / (1024 * 1024)
            
            logger.info("="*50)
            logger.info("UNIFICAÇÃO CONCLUÍDA!")
            logger.info("="*50)
            logger.info(f"Arquivo criado: {self.arquivo_saida.name}")
            logger.info(f"Total de registros: {registros_totais:,}")
            logger.info(f"Tamanho: {tamanho_mb:.1f} MB")
            logger.info(f"Tempo: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
            
            return registros_totais
            
        except Exception as e:
            logger.error(f"Erro durante unificação: {e}")
            raise
        finally:
            gc.collect()


def main():
    """Função principal para execução standalone"""
    try:
        unifier = SIHUnifier(
            pasta_entrada=Settings.RAW_DIR,
            arquivo_saida=Settings.INTERIM_DIR / "sih_rs.parquet",
            lote_size=50
        )
        
        registros = unifier.unificar(usar_lazy=True)
        
        logger.info(f"\nSUCESSO! {registros:,} registros unificados com POLARS!")
        
    except Exception as e:
        logger.error(f"\nERRO: {e}")
        raise


if __name__ == "__main__":
    main()