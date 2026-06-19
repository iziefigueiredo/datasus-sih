"""
load.py
-------
Classe DuckDBLoader — carga standalone de parquets processados no DuckDB.

Lê os parquets de data/processed/ e insere nas tabelas do banco sihrd5.duckdb,
seguindo a ordem e os tipos definidos em schema.py.

Uso standalone:
    python src/database/load.py

No pipeline semi-ELT (pipeline_load.py), a carga das dimensões e da tabela
fato é feita diretamente, sem passar por esta classe.
"""

import sys
import time
import logging
from pathlib import Path

import polars as pl
import duckdb

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))

from config.settings import Settings
from database.schema import TABLE_SCHEMAS, LOAD_ORDER

logger = logging.getLogger(__name__)


class DuckDBLoader:
    def __init__(self, db_path: Path, processed_dir: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path))
        self.processed_dir = processed_dir
        self.tables = LOAD_ORDER

    def _polars_to_duckdb(self, tipo) -> str:
        mapping = {
            pl.Int8: "TINYINT",    pl.Int16: "SMALLINT",  pl.Int32: "INTEGER",
            pl.Int64: "BIGINT",    pl.UInt64: "UBIGINT",
            pl.Float32: "FLOAT",   pl.Float64: "DOUBLE",
            pl.Boolean: "BOOLEAN", pl.Date: "DATE",
            pl.Datetime: "TIMESTAMP",
            pl.String: "VARCHAR",  pl.Utf8: "VARCHAR",    pl.Categorical: "VARCHAR",
        }
        return mapping.get(tipo, "VARCHAR")

    def create_tables(self):
        """Cria todas as tabelas definidas em TABLE_SCHEMAS."""
        n = 0
        for nome in self.tables:
            schema = TABLE_SCHEMAS.get(nome)
            if not schema:
                continue

            colunas_sql = []
            for col_name, col_type in schema["columns"].items():
                colunas_sql.append(f'"{col_name}" {self._polars_to_duckdb(col_type)}')

            pk_cols = schema.get("primary_key", [])
            if pk_cols:
                pk_str = ", ".join([f'"{c}"' for c in pk_cols])
                colunas_sql.append(f"PRIMARY KEY ({pk_str})")

            # FKs documentadas em schema.py mas não enforced no DuckDB —
            # tabelas de domínio do DATASUS são incompletas por design.
            sql = f'CREATE TABLE IF NOT EXISTS "{nome}" ({", ".join(colunas_sql)});'
            self.con.execute(sql)
            n += 1

        logger.info(f"  Schema: {n} tabelas criadas")

    def process_table(self, table_name: str):
        """Lê parquet e insere na tabela correspondente."""
        file_path = self.processed_dir / f"{table_name}.parquet"
        if not file_path.exists():
            logger.warning(f"  ERRO {table_name} — parquet não encontrado")
            return

        df = pl.read_parquet(file_path)
        if len(df) == 0:
            logger.warning(f"  ERRO {table_name} — 0 registros")
            return

        colunas_destino = self.con.table(table_name).columns
        df_load = df.select([c for c in colunas_destino if c in df.columns])

        t0 = time.time()
        self.con.execute(f'INSERT INTO "{table_name}" BY NAME SELECT * FROM df_load')
        logger.info(f"  {table_name:<25s} {len(df_load):>10,} linhas  {time.time()-t0:>5.1f}s")

    def run(self):
        logger.info("=== CARGA NO DUCKDB ===")
        logger.info(f"  Origem:  {self.processed_dir}")
        self.create_tables()
        logger.info(f"\n  {'TABELA':<25s} {'LINHAS':>10s}  {'TEMPO':>5s}")
        logger.info(f"  {'-'*44}")
        for table in self.tables:
            self.process_table(table)

    def get_metrics(self) -> int:
        total = 0
        for t in self.tables:
            total += self.con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        return total


def main():
    from config.logging_config import setup_logging
    setup_logging("load")

    db_path = Settings.DB_PATH
    processed_dir = Settings.PROCESSED_DIR

    if db_path.exists():
        db_path.unlink()

    loader = DuckDBLoader(db_path=db_path, processed_dir=processed_dir)
    t0 = time.time()

    try:
        loader.run()
        tempo = time.time() - t0
        total = loader.get_metrics()
        tamanho_mb = db_path.stat().st_size / (1024 * 1024)
        logger.info(f"\n{'='*50}")
        logger.info(f"CARGA CONCLUÍDA")
        logger.info(f"  Banco:   {db_path}")
        logger.info(f"  Linhas:  {total:,}")
        logger.info(f"  Disco:   {tamanho_mb:.1f} MB")
        logger.info(f"  Tempo:   {tempo:.1f}s ({tempo/60:.1f} min)")
        logger.info(f"{'='*50}")
    except Exception as e:
        logger.critical(f"Falha na carga: {e}", exc_info=True)
        raise
    finally:
        loader.con.close()


if __name__ == "__main__":
    main()