import os
from ftplib import FTP, error_perm
from pathlib import Path
from tqdm import tqdm
import sys

SRC_DIR = Path(__file__).parent.parent.parent  # src/
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/dissemin/publicos/SIHSUS/200801_/Doc/"
FTP_TIMEOUT = 30  


def get_remote_filesize(ftp: FTP, filename: str) -> int:
    """Retorna o tamanho do arquivo remoto em bytes, ou -1 se não disponível."""
    try:
        return ftp.size(filename)
    except Exception:
        return -1


def download_documentacao():
    docs_local_dir = Settings.DATA_DIR / "docs"
    docs_local_dir.mkdir(parents=True, exist_ok=True)

    print("=== DOWNLOAD DOCUMENTAÇÃO DATASUS ===")
    print(f"Conectando em: {FTP_HOST}...")

    try:
        with FTP(FTP_HOST, timeout=FTP_TIMEOUT) as ftp:
            ftp.login()  # Acesso público
            ftp.cwd(FTP_DIR)

            print("Buscando arquivos de documentação...")
            arquivos = [f for f in ftp.nlst() if f.lower().endswith(".pdf")]
            print(f"Encontrados {len(arquivos)} arquivos PDF.")

            erros = []

            for nome_arquivo in tqdm(arquivos, desc="Baixando PDFs"):
                caminho_local = docs_local_dir / nome_arquivo
                tamanho_remoto = get_remote_filesize(ftp, nome_arquivo)

                # Pula se arquivo já existe e tem o tamanho correto
                if caminho_local.exists():
                    if tamanho_remoto == -1 or caminho_local.stat().st_size == tamanho_remoto:
                        continue
                    else:
                        print(f"\nArquivo incompleto detectado, baixando novamente: {nome_arquivo}")

                try:
                    caminho_temp = caminho_local.with_suffix(".tmp")
                    with open(caminho_temp, "wb") as f:
                        ftp.retrbinary(f"RETR {nome_arquivo}", f.write)

                    # Verifica integridade após download
                    if tamanho_remoto != -1 and caminho_temp.stat().st_size != tamanho_remoto:
                        caminho_temp.unlink()
                        raise IOError(f"Tamanho incorreto após download: {nome_arquivo}")

                    caminho_temp.rename(caminho_local)

                except Exception as e:
                    if 'caminho_temp' in locals() and caminho_temp.exists():
                        caminho_temp.unlink()
                    erros.append((nome_arquivo, str(e)))
                    tqdm.write(f"Erro ao baixar {nome_arquivo}: {e}")

        print(f"\nCONCLUÍDO! Documentos salvos em: {docs_local_dir}")

        if erros:
            print(f"\nAVISO: {len(erros)} arquivo(s) com erro:")
            for nome, erro in erros:
                print(f"  - {nome}: {erro}")

    except Exception as e:
        print(f"Erro ao acessar FTP: {e}")

def main():
    download_documentacao()


if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging("download_docs")
    main()