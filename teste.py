import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import sys

# --- 1. CONFIGURAÇÕES DO BANCO DE DADOS ---
DB_NAME = "sih_rs"
DB_USER = "postgres"
DB_PASSWORD = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"

# NOME DA TABELA FORNECIDO
TABLE_NAME = "atendimentos" 
COLUMN_NAME = "PROC_REA"

# --- 2. CONEXÃO E CONSULTA ---
try:
    # Conecta ao banco de dados
    conn = psycopg2.connect(
        dbname=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD, 
        host=DB_HOST, 
        port=DB_PORT
    )
    
    # Consulta SQL
    sql_query = f'SELECT "{COLUMN_NAME}" FROM {TABLE_NAME} WHERE "{COLUMN_NAME}" IS NOT NULL;'
    print(f"Executando a consulta: {sql_query}")

    # Carrega os dados diretamente no DataFrame
    df = pd.read_sql(sql_query, conn)
    
    print(f"Total de registros lidos: {len(df)}")

except psycopg2.Error as e:
    print(f"Erro ao conectar ou consultar o banco de dados: {e}")
    print("Verifique se o banco de dados e a tabela 'atendimentos' existem.")
    sys.exit(1)
finally:
    # Fecha a conexão
    if 'conn' in locals() and conn:
        conn.close()

# --- 3. CÁLCULO E PRÉ-PROCESSAMENTO (TOP 10) ---

# Calcula a frequência dos valores e ordena
frequencia = df[COLUMN_NAME].value_counts().sort_values(ascending=False)

# Limita o gráfico aos 10 valores mais frequentes (para clareza visual)
TOP_N = 10
if len(frequencia) > TOP_N:
    frequencia_plot = frequencia.head(TOP_N)
    print(f"\nGerando gráfico apenas com os Top {TOP_N} valores mais frequentes.")
else:
    frequencia_plot = frequencia
    print(f"\nGerando gráfico com todos os {len(frequencia)} valores únicos.")


# --- 4. GERAÇÃO DO GRÁFICO ---
plt.figure(figsize=(10, 6))
frequencia_plot.plot(kind='bar', color='skyblue')

# Configurações do gráfico
plt.title(f'Frequência dos Top {TOP_N} Códigos {COLUMN_NAME} na Tabela "{TABLE_NAME}"')
plt.xlabel(f'Código do Procedimento ({COLUMN_NAME})')
plt.ylabel('Frequência')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()

# Salva o gráfico
output_file = 'frequencia_proc_rea.png'
plt.savefig(output_file)
print(f"\nGráfico salvo com sucesso como: {output_file}")
