# %%
from dbfread import DBF
import pandas as pd
import zipfile
import os

# %%
caminho_zip = "./dados_sih/TAB_SIH.zip"
pasta_extraida = "./dados_sih/TAB_SIH"

with zipfile.ZipFile(caminho_zip) as z:
    z.extractall(pasta_extraida)

pasta_dbf = f"{pasta_extraida}/DBF"

# %%
cadhosp = pd.DataFrame(iter(DBF(f"{pasta_dbf}/CADHOSP.DBF", encoding="latin1")))
tcnes = pd.DataFrame(iter(DBF(f"{pasta_dbf}/TCNESBR.dbf", encoding="latin1")))

# %%
print("CADHOSP:", cadhosp.shape, list(cadhosp.columns))
print(cadhosp.head())

# %%
print("TCNESBR:", tcnes.shape, list(tcnes.columns))
print(tcnes.head())

# %%
# checar duplicidade de chave em cada fonte
print("CNPJ duplicado em CADHOSP:", cadhosp["CGC_HOSP"].duplicated().sum())
print("CNES duplicado em TCNESBR:", tcnes["CNES"].duplicated().sum())

# %%
# nome válido = não nulo e não string vazia/só espaço
nomes_cadhosp = cadhosp["RAZAO"].astype(str).str.strip()
nomes_tcnes = tcnes["NOMEFANT"].astype(str).str.strip()

qtd_cadhosp = (nomes_cadhosp != "").sum()
qtd_tcnes = (nomes_tcnes != "").sum()

# %%
print(f"CADHOSP: {qtd_cadhosp} nomes válidos de {len(cadhosp)} registros")
print(f"TCNESBR: {qtd_tcnes} nomes válidos de {len(tcnes)} registros")