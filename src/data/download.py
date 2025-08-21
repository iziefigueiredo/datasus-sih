"""
Download DATASUS
Localização: projeto_sih/src/data/download.py
Função: EXTRACT - Baixa dados SIH/SUS (.dbc) e converte para .parquet
"""
import sys
from pathlib import Path
from pysus.online_data.SIH import SIH
from tqdm import tqdm

SRC_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

def verificar_arquivos_existentes():
    """Conta arquivos já baixados"""
    pasta = Settings.PARQUET_DIR
    
    if not pasta.exists():
        return 0, set()
    
    nomes_existentes = {
        arquivo.stem for arquivo in pasta.glob("*.parquet")
        if arquivo.stat().st_size > 0
    }
    
    return len(nomes_existentes), nomes_existentes

def filtrar_arquivos_novos(arquivos_encontrados, nomes_existentes):
    """Remove arquivos já baixados"""
    arquivos_novos = []
    
    for arquivo in arquivos_encontrados:
        nome_base = Path(str(arquivo)).stem
        if nome_base.endswith('.dbc'):
            nome_base = nome_base[:-4]
        
        if nome_base not in nomes_existentes:
            arquivos_novos.append(arquivo)
    
    return arquivos_novos

def main():
    """Execução principal do download"""
    
    print("=== DOWNLOAD DATASUS ===")
    
    Settings.criar_diretorios()
    print(f"Pasta de destino: {Settings.PARQUET_DIR}")
    
    print("Verificando arquivos já baixados...")
    qtd_existentes, nomes_existentes = verificar_arquivos_existentes()
    print(f"Já baixados: {qtd_existentes} arquivos")
    
    print("Carregando SIH...")
    sih = SIH().load()
    print("SIH carregado!")
    
    uf = Settings.UF_DEFAULT
    anos = Settings.get_anos_range()
    meses = Settings.MESES
    tipo = Settings.TIPO_ARQUIVO
    
    print(f"Config: UF={uf}, Anos={min(anos)}-{max(anos)}, Tipo={tipo}")
    
    print("Buscando arquivos...")
    arquivos_encontrados = []
    
    for ano in tqdm(anos, desc="Buscando"):
        try:
            arquivos = sih.get_files(tipo, uf=uf, year=ano, month=meses)
            arquivos_encontrados.extend(arquivos)
        except Exception as e:
            print(f"Erro {ano}: {e}")
    
    arquivos_a_baixar = filtrar_arquivos_novos(arquivos_encontrados, nomes_existentes)
    
    print(f"\nRESUMO:")
    print(f"   Total: {len(arquivos_encontrados)} | Baixados: {qtd_existentes} | Restam: {len(arquivos_a_baixar)}")
    
    if not arquivos_a_baixar:
        print("Todos arquivos já baixados!")
        return
    
    print("Primeiros arquivos:")
    for i, arquivo in enumerate(arquivos_a_baixar[:3]):
        print(f"   {i+1}. {arquivo}")
    if len(arquivos_a_baixar) > 3:
        print(f"   ... +{len(arquivos_a_baixar) - 3} arquivos")
    
    if input(f"Baixar {len(arquivos_a_baixar)} arquivos? (s/N): ").lower() != 's':
        print("Cancelado")
        return
    
    print(f"Baixando {len(arquivos_a_baixar)} arquivos...")
    
    try:
        if len(arquivos_a_baixar) > 20:
            baixados_total = []
            lote_size = 18
            
            for i in range(0, len(arquivos_a_baixar), lote_size):
                lote = arquivos_a_baixar[i:i + lote_size]
                lote_num = i // lote_size + 1
                total_lotes = (len(arquivos_a_baixar) - 1) // lote_size + 1
                
                print(f"Lote {lote_num}/{total_lotes} ({len(lote)} arquivos)...")
                
                try:
                    baixados_lote = sih.download(lote, local_dir=Settings.PARQUET_DIR)
                    baixados_total.extend(baixados_lote)
                    
                    progresso = len(baixados_total) / len(arquivos_a_baixar) * 100
                    print(f"{len(baixados_total)}/{len(arquivos_a_baixar)} ({progresso:.0f}%)")
                    
                except Exception as e:
                    print(f"Erro lote {lote_num}: {e}")
                    continue
            
            baixados = baixados_total
        else:
            baixados = sih.download(arquivos_a_baixar, local_dir=Settings.PARQUET_DIR)
        
        print("CONCLUÍDO!")
        print(f"{len(baixados)} baixados | Total: {qtd_existentes + len(baixados)} arquivos")
        
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()