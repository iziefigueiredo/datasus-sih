import polars as pl
import pandas as pd
import sys
import io
import logging
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine
import time
from sqlalchemy import text


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
            # Tabelas de dimensão primeiro (sem dependências ou com dependências já listadas)
            "cid10",
            "municipios",
            "procedimentos",
            "hospital",
            "dado_ibge",
            "internacoes", # Tabela de fatos principal
            "uti_detalhes",
            "condicoes_especificas",
            "obstetricos",
            "instrucao",
            "mortes",
            "infehosp",
            "vincprev",
            "cbor",
            "contraceptivos",
            "etnia",
            "notificacoes",
            "pernoite",
            "diagnosticos",
            "atendimentos"
        ]

    def get_database_size_info(self):
        with self.engine.connect() as conn:
            # tamanho do banco em MB
            size_query = "SELECT pg_database_size(current_database()) / 1024 / 1024;"
            tamanho_db_mb = conn.execute(text(size_query)).scalar()

            # estimativa de linhas em todas as tabelas
            rows_query = """
                SELECT SUM(reltuples)::bigint
                FROM pg_class
                WHERE relkind = 'r';
            """
            total_linhas = conn.execute(text(rows_query)).scalar()

        return tamanho_db_mb, total_linhas

    def criar_uniques(self):
        logger.info("\n--- Criando UNIQUE constraints a partir do schema ---")
        for table_name, info in TABLE_SCHEMAS.items():
            for col_group in info.get("uniques", []):
                # Garante que col_group seja sempre uma lista
                cols = col_group if isinstance(col_group, list) else [col_group]
                uq_name = f"uq_{table_name}_{'_'.join(cols)}"

                if self.constraint_existe(uq_name):
                    logger.info(f"UNIQUE '{uq_name}' já existe. Pulando.")
                    continue
                try:
                    cols_str = ", ".join([f'"{c}"' for c in cols])
                    self.cursor.execute(
                        f'ALTER TABLE "{table_name}" '
                        f'ADD CONSTRAINT {uq_name} UNIQUE ({cols_str});'
                    )
                    self.conn.commit()
                    logger.info(f"UNIQUE criada: {uq_name}")
                except Exception as e:
                    self.conn.rollback()
                    logger.error(f"Erro ao criar UNIQUE {uq_name}: {e}")

    def run(self):
        logger.info("=== INICIANDO CARGA NO POSTGRESQL ===")
        
        self.criar_tabelas()
        for table in self.tables:
            self.process_table(table)
        self.criar_uniques()
        self.criar_constraints()
        #self.conn.close()
        logger.info("=== CARGA CONCLUÍDA COM SUCESSO ===")

    def polars_to_postgres_type(self, tipo, col_name=None, pk_cols=None):
        """
        Converte um tipo Polars para um tipo PostgreSQL.
        A lógica foi aprimorada para identificar chaves primárias e usar BIGSERIAL.
        """
        # Se a coluna for uma chave primária e seu tipo for nosso "sinalizador",
        # converta para BIGSERIAL.
        if col_name in (pk_cols or []) and tipo == pl.UInt64:
            return "BIGSERIAL"

        # O resto da lógica de conversão continua a mesma.
        if tipo == pl.Int64 or tipo == pl.Int32:
            return "BIGINT" if tipo == pl.Int64 else "INTEGER"
        elif tipo == pl.Float64:
            return "DOUBLE PRECISION"
        elif tipo == pl.String or tipo == pl.Utf8: # Usar pl.String é mais moderno
            return "TEXT"
        elif tipo == pl.Boolean:
            return "BOOLEAN"
        elif tipo == pl.Date or tipo == pl.Datetime:
            return "TIMESTAMP"
        else:
            return "TEXT"

    def criar_tabelas(self, table_name=None):
        if table_name:
            nomes = [table_name]
        else:
            logger.info("\n--- Criando todas as tabelas no PostgreSQL ---")
            # Usa a ordem definida na classe para garantir a criação correta das dependências
            nomes = self.tables

        for nome in nomes:
            schema = TABLE_SCHEMAS.get(nome, {})
            if not schema:
                logger.warning(f"Esquema para a tabela '{nome}' não encontrado. Pulando.")
                continue
            
            colunas_sql = []
            
            # Pega a lista de chaves primárias antes do loop
            pk_cols = schema.get("primary_key", [])

            # Itera sobre as colunas para montar o SQL
            for col_name, col_type in schema["columns"].items():
                # Passa as informações extras para a função de conversão
                postgres_type = self.polars_to_postgres_type(col_type, col_name=col_name, pk_cols=pk_cols)
                colunas_sql.append(f'"{col_name}" {postgres_type}')

            # Adiciona a constraint de chave primária no final, se existir
            if pk_cols:
                pk_str = ", ".join([f'"{col}"' for col in pk_cols])
                colunas_sql.append(f"PRIMARY KEY ({pk_str})")

            # Monta e executa o comando SQL final
            sql = f'CREATE TABLE IF NOT EXISTS "{nome}" (\n  {",\n  ".join(colunas_sql)}\n);'

            try:
                self.cursor.execute(sql)
                self.conn.commit()
                logger.info(f"Tabela criada ou já existente: {nome}")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Erro ao criar tabela {nome}: {e}")

    def process_table(self, table_name):
        
        file_path = self.processed_dir / f"{table_name}.parquet"

        if not file_path.exists():
            logger.warning(f"Arquivo {file_path} não encontrado. Pulando '{table_name}'.")
            return

        df = pl.read_parquet(file_path)
        if df.is_empty():
            logger.info(f"{table_name}: arquivo vazio. Pulando carga.")
            return

        # A criação da tabela já foi feita no início do 'run'
        self.truncar_tabela(table_name)

        schema = TABLE_SCHEMAS.get(table_name, {})
        logger.info(f"{table_name}: Iniciando carga de {len(df):,} linhas...")

        colunas_db = self.get_colunas_db(table_name)
        
        # Filtra colunas do DataFrame para corresponder às colunas do DB
        # Ignora colunas que são auto-incrementadas (como id_atendimento)
        colunas_df_para_carregar = [c for c in df.columns if c in colunas_db]

        faltando = [c for c in colunas_df_para_carregar if c not in df.columns]
        if faltando:
            raise ValueError(f"Tabela {table_name}: colunas ausentes no arquivo: {faltando}")
        
        extras = [c for c in df.columns if c not in colunas_df_para_carregar]
        if extras:
            logger.info(f"{table_name}: colunas extras no arquivo (serão ignoradas): {extras}")

        df = self.converter_tipos(df, schema)

        self.carregar_em_chunks(df, table_name, colunas_df_para_carregar)

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
            WHERE table_name = '{table_name.lower()}'
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

    def carregar_em_chunks(self, df, table_name, colunas_df):
        if not colunas_df:
            logger.warning(f"Tabela {table_name}: Nenhuma coluna para carregar. Pulando.")
            return
            
        for i in range(0, len(df), self.chunk_size):
            chunk = df.slice(i, self.chunk_size).select(colunas_df)
            buffer = io.BytesIO()
            csv_str = chunk.write_csv(None, include_header=False)
            buffer.write(csv_str.encode("utf-8"))
            buffer.seek(0)

            try:
                self.cursor.copy_from(buffer, table_name, sep=",", null="", columns=colunas_df)
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
        logger.info("\n--- Criando chaves estrangeiras a partir do schema ---")
        # Itera na ordem correta para garantir que as tabelas referenciadas existam
        for table_name in self.tables:
            schema_info = TABLE_SCHEMAS.get(table_name, {})
            for fk in schema_info.get("foreign_keys", []):
                fk_name = f"fk_{table_name}_{fk['column']}"
                
                if self.constraint_existe(fk_name):
                    logger.info(f"Constraint '{fk_name}' já existe. Pulando.")
                    continue
                
                try:
                    comando = (
                        f"ALTER TABLE \"{table_name}\" "
                        f"ADD CONSTRAINT \"{fk_name}\" "
                        f"FOREIGN KEY (\"{fk['column']}\") "
                        f"REFERENCES \"{fk['references_table']}\" (\"{fk['references_column']}\") "
                        "ON DELETE CASCADE;"
                    )
                    self.cursor.execute(comando)
                    self.conn.commit()
                    logger.info(f"Constraint criada: {fk_name}")
                except Exception as e:
                    self.conn.rollback()
                    logger.error(f"Erro ao criar constraint {fk_name}: {e}")


def run_db_load_pipeline():
    """
    Orquestra a execução da carga no banco de dados e registra o tempo total.
    """
    inicio = time.time()
    
    loader = None # Inicializa a variável para que esteja acessível no 'finally'
    
    try:
        # Configura as variáveis de conexão e diretórios
        db_config = Settings.DB_CONFIG
        db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        processed_dir = Settings.PROCESSED_DIR

        # Cria a instância do loader
        loader = PostgreSQLLoader(db_url=db_url, processed_dir=processed_dir)
        
        # Executa a carga. (Lembre-se de remover 'self.conn.close()' de dentro do método run())
        loader.run() 

        tempo_total = time.time() - inicio
        
        # Com a conexão principal ainda aberta, busca as métricas finais do banco.
        tamanho_db_mb, total_linhas = loader.get_database_size_info()

        # Exibe o resumo final do sucesso da operação
        logger.info("="*50)
        logger.info("CARGA NO BANCO DE DADOS CONCLUÍDA!")
        logger.info("="*50)
        logger.info(f"Total de linhas carregadas em todas as tabelas: {total_linhas:,}")
        logger.info(f"Tamanho final do banco de dados: {tamanho_db_mb:.1f} MB")
        logger.info(f"Tempo total da etapa de carga: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
        # --- FIM DO BLOCO DE LOG ---

    except Exception as e:
        # Se qualquer erro ocorrer durante o 'try', ele será capturado e logado aqui.
        logger.critical(f"A etapa de carga no banco de dados falhou: {e}", exc_info=True)
        raise # Re-levanta a exceção para parar a execução do programa principal.

    finally:
        # Este bloco é executado SEMPRE, tenha ocorrido um erro ou não.
        # É o lugar mais seguro para garantir que a conexão com o banco seja fechada.
        if loader and loader.conn and not loader.conn.closed:
            try:
                loader.conn.close()
                logger.info("Conexão com o banco de dados fechada com sucesso.")
            except Exception as e:
                # Loga um erro se até mesmo o fechamento da conexão falhar.
                logger.error(f"Erro ao tentar fechar a conexão com o banco de dados: {e}")

if __name__ == "__main__":
    run_db_load_pipeline()
