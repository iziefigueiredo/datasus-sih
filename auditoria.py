# %%
import duckdb

# Path
db_path = "sihrd6.duckdb"
con = duckdb.connect(db_path, read_only=True)

# 
tabelas = con.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_name
""").fetchall()
tabelas = [t[0] for t in tabelas]
print(f"{len(tabelas)} tabelas encontradas")

# 
alertas = []

for tabela in tabelas:
    n_linhas = con.execute(f'SELECT COUNT(*) FROM "{tabela}"').fetchone()[0]

    if n_linhas == 0:
        alertas.append((tabela, "TABELA VAZIA", None, None))
        continue

    colunas = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '{tabela}'
    """).fetchall()
    colunas = [c[0] for c in colunas]

    for col in colunas:
        n_nulos = con.execute(
            f'SELECT COUNT(*) FROM "{tabela}" WHERE "{col}" IS NULL'
        ).fetchone()[0]
        pct_nulo = 100.0 * n_nulos / n_linhas

        if pct_nulo == 100.0:
            alertas.append((tabela, "COLUNA 100% NULA", col, pct_nulo))
        elif pct_nulo > 50.0:
            alertas.append((tabela, "COLUNA >50% NULA", col, pct_nulo))

# 
print(f"\n{'TABELA':<25s} {'PROBLEMA':<20s} {'COLUNA':<25s} {'% NULO'}")
print("-" * 85)
for tabela, problema, coluna, pct in alertas:
    pct_str = f"{pct:.1f}%" if pct is not None else "-"
    print(f"{tabela:<25s} {problema:<20s} {str(coluna):<25s} {pct_str}")

if not alertas:
    print("Nenhum problema encontrado.")