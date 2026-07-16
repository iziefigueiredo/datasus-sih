# %%
import duckdb
import polars as pl

# %%
# ajuste os caminhos conforme seu ambiente
db_path = "./sihrd6.duckdb"
tcnes_path = "./data/support/tcnes.csv"
cadhosp_path = "./data/support/cadhosp.csv"

# %%
con = duckdb.connect(db_path, read_only=True)
sem_nome = con.execute(
    'SELECT CNES FROM hospital WHERE NO_HOSPITAL IS NULL'
).pl()
print(sem_nome)

# %%
cnes_faltantes = sem_nome["CNES"].to_list()

# %%
tcnes = pl.read_csv(tcnes_path, schema_overrides={"CNES": pl.String})
cadhosp = pl.read_csv(cadhosp_path, schema_overrides={"CGC_HOSP": pl.String})

# %%
for cnes_val in cnes_faltantes:
    cnes = str(cnes_val).strip()
    no_tcnes = tcnes.filter(pl.col("CNES") == cnes).height > 0
    # tenta também com zero à esquerda até 7 dígitos, caso dtype tenha perdido zeros
    no_tcnes_pad = tcnes.filter(pl.col("CNES") == cnes.zfill(7)).height > 0
    print(f"CNES {cnes} | no TCNESBR (exato): {no_tcnes} | no TCNESBR (com zfill7): {no_tcnes_pad}")