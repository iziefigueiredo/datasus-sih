# %%
import pandas as pd

# %%
destino = "./dados_sih"

rd = pd.read_parquet(f"{destino}/RDAC0901.parquet")
sp = pd.read_parquet(f"{destino}/SPAC0901.parquet")

# %%
naih_rd = set(rd["N_AIH"])
naih_sp = set(sp["SP_NAIH"])

# %%
so_rd = naih_rd - naih_sp
so_sp = naih_sp - naih_rd
em_ambos = naih_rd & naih_sp

# %%
print(f"registros RD: {len(rd)}")
print(f"registros SP: {len(sp)}")
print(f"NAIH únicas RD: {len(naih_rd)}")
print(f"NAIH únicas SP: {len(naih_sp)}")
print(f"NAIH em ambos: {len(em_ambos)}")
print(f"só em RD: {len(so_rd)}")
print(f"só em SP: {len(so_sp)}")