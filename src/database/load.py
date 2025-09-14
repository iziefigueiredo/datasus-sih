import polars as pl
import pandas as pd
import sys
import io
import logging
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))

from config.settings import Settings
from database.schema import TABLE_SCHEMAS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


class PostgreSQLLoader:
    def __init__(self, db_url, processed_dir, chunk_size=50000):
        self.engine = create_engine(db_url)
        self.conn = self.engine.raw_connection()
        self.cursor = self.conn.cursor()
        self.chunk_size = chunk_size
        self.processed_dir = processed_dir
        self.tables = [
            "internacoes",
            "uti_detalhes",
            "condicoes_especificas",
            "hospital",
            "obstetricos",
            "cid10",
            "municipios",
            "procedimentos",
            "instrucao",
            "mortes",
            "infehosp",
            "vincprev",
            "cbor",
            "dado_ibge"  
        ]

    def criar_uniques(self):
        logger.info("\n--- Criando UNIQUE constraints a partir do schema ---")
        for table_name, info in TABLE_SCHEMAS.items():
            for col in info.get("uniques", []):
                uq_name = f"uq_{table_name}_{col}"
                if self.constraint_existe(uq_name):
                    logger.info(f"UNIQUE '{uq_name}' já existe. Pulando.")
                    continue
                try:
                    self.cursor.execute(
                        f'ALTER TABLE "{table_name}" '
                        f'ADD CONSTRAINT {uq_name} UNIQUE ("{col}");'
                    )
                    self.conn.commit()
                    logger.info(f"UNIQUE criada: {uq_name}")
                except Exception as e:
                    self.conn.rollback()
                    logger.error(f"Erro ao criar UNIQUE {uq_name}: {e}")

    def run(self):
        logger.info("=== INICIANDO CARGA NO POSTGRESQL ===")
        self.converter_csv_parquet()
        self.criar_tabelas()
        for table in self.tables:
            self.process_table(table)
        self.criar_uniques()    
        self.criar_constraints()
        self.conn.close()
        logger.info("=== CARGA CONCLUÍDA COM SUCESSO ===")

    def criar_tabelas(self, table_name=None):
        if table_name:
            nomes = [table_name]
        else:
            logger.info("\n--- Criando todas as tabelas no PostgreSQL ---")
            nomes = list(TABLE_SCHEMAS.keys())

        for nome in nomes:
            schema = TABLE_SCHEMAS.get(nome, {})
            colunas_sql = []

            for col, tipo in schema["columns"].items():
                tipo_pg = self.polars_to_postgres_type(tipo)
                colunas_sql.append(f'"{col}" {tipo_pg}')

            pk = schema.get("primary_key")
            if pk:
                pk_str = ", ".join([f'"{col}"' for col in pk])
                colunas_sql.append(f"PRIMARY KEY ({pk_str})")

            sql = f'CREATE TABLE IF NOT EXISTS "{nome}" (\n  {", ".join(colunas_sql)}\n);'

            try:
                self.cursor.execute(sql)
                self.conn.commit()
                logger.info(f"Tabela criada ou já existente: {nome}")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Erro ao criar tabela {nome}: {e}")

    def polars_to_postgres_type(self, tipo):
        if tipo == pl.Int64 or tipo == pl.Int32:
            return "BIGINT" if tipo == pl.Int64 else "INTEGER"
        elif tipo == pl.Float64:
            return "DOUBLE PRECISION"
        elif tipo == pl.Utf8:
            return "TEXT"
        elif tipo == pl.Boolean:
            return "BOOLEAN"
        elif tipo == pl.Date or tipo == pl.Datetime:
            return "TIMESTAMP"
        else:
            return "TEXT"

    def process_table(self, table_name):
        # AQUI O CAMINHO FOI AJUSTADO PARA USAR O DIRETÓRIO DE PROCESSADOS
        # COM O QUAL O OBJETO FOI INSTANCIADO
        if table_name in ["cid10", "municipios", "procedimentos", "dado_ibge"]:
            file_path = self.processed_dir / f"{table_name}.parquet"
        else:
            file_path = self.processed_dir / f"{table_name}.parquet"

        if not file_path.exists():
            logger.warning(f"Arquivo {file_path} não encontrado. Pulando '{table_name}'.")
            return

        df = pl.read_parquet(file_path)
        if df.is_empty():
            logger.info(f"{table_name}: arquivo vazio. Pulando carga.")
            return

        self.criar_tabelas(table_name)
        self.truncar_tabela(table_name)

        schema = TABLE_SCHEMAS.get(table_name, {})
        logger.info(f"{table_name}: Iniciando carga de {len(df):,} linhas...")

        colunas_db = self.get_colunas_db(table_name)

        faltando = [c for c in colunas_db if c not in df.columns]
        if faltando:
            raise ValueError(f"Tabela {table_name}: colunas ausentes no arquivo: {faltando}")
        extras = [c for c in df.columns if c not in colunas_db]
        if extras:
            logger.info(f"{table_name}: colunas extras no arquivo (serão ignoradas no SELECT): {extras}")

        df = self.converter_tipos(df, schema)

        self.carregar_em_chunks(df, table_name, colunas_db)

    def truncar_tabela(self, table_name):
        try:
            self.cursor.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE;')
            self.conn.commit()
            logger.info(f"{table_name}: truncada com sucesso.")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erro ao truncar {table_name}: {e}")

    def get_colunas_db(self, table_name):
        self.cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{table_name.lower()}' AND column_name != 'id'
            ORDER BY ordinal_position;
        """)
        return [row[0] for row in self.cursor.fetchall()]

    def converter_tipos(self, df, schema):
        for col in df.columns:
            if col not in schema.get("columns", {}):
                continue
            tipo = schema["columns"][col]
            try:
                df = df.with_columns(pl.col(col).cast(tipo, strict=False))
            except Exception as e:
                logger.warning(f"Falha ao converter coluna '{col}' para {tipo}: {e}")
        return df

    def carregar_em_chunks(self, df, table_name, colunas_db):
        for i in range(0, len(df), self.chunk_size):
            chunk = df.slice(i, self.chunk_size).select(colunas_db)
            buffer = io.BytesIO()
            csv_str = chunk.write_csv(None, include_header=False)
            buffer.write(csv_str.encode("utf-8"))
            buffer.seek(0)

            try:
                self.cursor.copy_from(buffer, table_name, sep=",", null="", columns=colunas_db)
                self.conn.commit()
                logger.info(f"{table_name}: Chunk {i // self.chunk_size + 1} carregado.")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Erro ao carregar chunk {i // self.chunk_size + 1} de {table_name}: {e}")
                raise

    def constraint_existe(self, nome):
        self.cursor.execute("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = %s;
        """, (nome,))
        return self.cursor.fetchone() is not None

    def criar_constraints(self):
        logger.info("\n--- Criando chaves primárias e estrangeiras ---")
        comandos = [
            ("fk_uti_internacoes", "ALTER TABLE uti_detalhes ADD CONSTRAINT fk_uti_internacoes FOREIGN KEY (\"N_AIH\") REFERENCES internacoes (\"N_AIH\");"),
            ("fk_cond_internacoes", "ALTER TABLE condicoes_especificas ADD CONSTRAINT fk_cond_internacoes FOREIGN KEY (\"N_AIH\") REFERENCES internacoes (\"N_AIH\");"),
            ("fk_internacoes_hospital", "ALTER TABLE internacoes ADD CONSTRAINT fk_internacoes_hospital FOREIGN KEY (\"CNES\") REFERENCES hospital (\"CNES\");"),
            ("fk_obs_internacoes", "ALTER TABLE obstetricos ADD CONSTRAINT fk_obs_internacoes FOREIGN KEY (\"N_AIH\") REFERENCES internacoes (\"N_AIH\");"),
            ("fk_diag_princ", "ALTER TABLE internacoes ADD CONSTRAINT fk_diag_princ FOREIGN KEY (\"DIAG_PRINC\") REFERENCES cid10 (\"CID\");"),
            ("fk_diag_secun", "ALTER TABLE internacoes ADD CONSTRAINT fk_diag_secun FOREIGN KEY (\"DIAG_SECUN\") REFERENCES cid10 (\"CID\");"),
            ("fk_cid_notif", "ALTER TABLE internacoes ADD CONSTRAINT fk_cid_notif FOREIGN KEY (\"CID_NOTIF\") REFERENCES cid10 (\"CID\");"),
            ("fk_cid_asso", "ALTER TABLE internacoes ADD CONSTRAINT fk_cid_asso FOREIGN KEY (\"CID_ASSO\") REFERENCES cid10 (\"CID\");"),
            ("fk_instrucao_internacoes", "ALTER TABLE instrucao ADD CONSTRAINT fk_instrucao_internacoes FOREIGN KEY (\"N_AIH\") REFERENCES internacoes (\"N_AIH\");"),
            ("fk_mortes_internacoes", "ALTER TABLE mortes ADD CONSTRAINT fk_mortes_internacoes FOREIGN KEY (\"N_AIH\") REFERENCES internacoes (\"N_AIH\");"),
            ("fk_infehosp_internacoes", "ALTER TABLE infehosp ADD CONSTRAINT fk_infehosp_internacoes FOREIGN KEY (\"N_AIH\") REFERENCES internacoes (\"N_AIH\");"),
            ("fk_vincprev_internacoes", "ALTER TABLE vincprev ADD CONSTRAINT fk_vincprev_internacoes FOREIGN KEY (\"N_AIH\") REFERENCES internacoes (\"N_AIH\");"),
            ("fk_cbor_internacoes", "ALTER TABLE cbor ADD CONSTRAINT fk_cbor_internacoes FOREIGN KEY (\"N_AIH\") REFERENCES internacoes (\"N_AIH\");"),
            ("fk_dado_ibge_municipios", "ALTER TABLE dado_ibge ADD CONSTRAINT fk_dado_ibge_municipios FOREIGN KEY (\"codigo_municipio_completo\") REFERENCES municipios (\"codigo_ibge\");"),
        ]


        for nome, comando in comandos:
            if self.constraint_existe(nome):
                logger.info(f"Constraint '{nome}' já existe. Pulando.")
                continue
            try:
                self.cursor.execute(comando)
                self.conn.commit()
                logger.info(f"Constraint criada: {nome}")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Erro ao criar constraint {nome}: {e}")

   

def run_db_load_pipeline():
    db_config = Settings.DB_CONFIG
    db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    # AQUI O CAMINHO FOI AJUSTADO PARA A PASTA DE PROCESSADOS
    processed_dir = Settings.PROCESSED_DIR

    # AQUI O CONSTRUTOR FOI AJUSTADO PARA RECEBER O NOVO NOME DA VARIÁVEL
    loader = PostgreSQLLoader(db_url=db_url, processed_dir=processed_dir)
    loader.run()


if __name__ == "__main__":
    run_db_load_pipeline()