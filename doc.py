# %%
import ftplib
import zipfile
import os

# %%
host = "ftp.datasus.gov.br"
pasta = "/dissemin/publicos/SIHSUS/200801_/Auxiliar"
arquivo = "TAB_SIH.zip"
destino = "./dados_sih"

os.makedirs(destino, exist_ok=True)
caminho_zip = os.path.join(destino, arquivo)

# %%
with ftplib.FTP(host) as ftp:
    ftp.login()  # anônimo
    ftp.cwd(pasta)
    with open(caminho_zip, "wb") as f:
        ftp.retrbinary(f"RETR {arquivo}", f.write)

print(f"baixado: {caminho_zip}")

# %%
with zipfile.ZipFile(caminho_zip) as z:
    for info in z.infolist():
        tamanho_kb = info.file_size / 1024
        print(f"{info.filename:<40} {tamanho_kb:>8.1f} KB")