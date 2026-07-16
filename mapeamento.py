# %%
import ftplib
import os
import tempfile
import pandas as pd
from pyreaddbc import dbc2dbf
from dbfread import DBF

# %%
host = "ftp.datasus.gov.br"
pasta = "/dissemin/publicos/SIHSUS/200801_/Dados"
destino = "./dados_sih"

os.makedirs(destino, exist_ok=True)

# %%
def baixar_dbc(arquivo, pasta, destino, host):
    caminho_local = os.path.join(destino, arquivo)
    with ftplib.FTP(host) as ftp:
        ftp.login()  # anônimo
        ftp.cwd(pasta)
        with open(caminho_local, "wb") as f:
            ftp.retrbinary(f"RETR {arquivo}", f.write)
    print(f"baixado: {arquivo}")
    return caminho_local

# %%
def dbc_para_parquet(caminho_dbc, destino):
    nome_base = os.path.splitext(os.path.basename(caminho_dbc))[0]

    # dbf fica só em pasta temporária, apagado ao final
    with tempfile.TemporaryDirectory() as tmp:
        caminho_dbf = os.path.join(tmp, f"{nome_base}.dbf")
        dbc2dbf(caminho_dbc, caminho_dbf)

        tabela = DBF(caminho_dbf, encoding="latin1")
        df = pd.DataFrame(iter(tabela))

    caminho_parquet = os.path.join(destino, f"{nome_base}.parquet")
    df.to_parquet(caminho_parquet, index=False)

    os.remove(caminho_dbc)  # remove o dbc original, só fica o parquet
    print(f"convertido: {caminho_parquet}")
    return caminho_parquet

# %%
arquivos = [
    "RDAC0901.dbc",
    "SPAC0901.dbc",
]

parquets = []
for arq in arquivos:
    caminho_dbc = baixar_dbc(arq, pasta, destino, host)
    caminho_parquet = dbc_para_parquet(caminho_dbc, destino)
    parquets.append(caminho_parquet)