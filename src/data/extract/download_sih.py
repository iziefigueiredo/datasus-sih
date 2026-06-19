"""
Download DATASUS — SIH
Localização: sihrd5/src/data/extract/download_sih.py
Função: EXTRACT - Baixa dados SIH/SUS (.dbc) e converte para .parquet
Destino: data/raw/sih/

Estratégia:
  - Multiprocessing: N processos Python isolados (1 por ano ativo)
  - Barra de progresso por ano, atualizada em tempo real
  - Cada processo: conexão FTP própria, retry com reconexão
  - Skip automático de arquivos já baixados
  - Output do pysus silenciado
"""
import sys
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from tqdm import tqdm
import logging
import time
import psutil
import pyarrow.parquet as pq

SRC_DIR = Path(__file__).parent.parent.parent  # src/
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────────────

MAX_WORKERS = Settings.DOWNLOAD_WORKERS
MAX_RETRIES = 3
RETRY_BASE  = 5       # backoff: 5s, 10s, 20s
STAGGER     = 2       # segundos entre workers
TIMEOUT     = 120     # segundos max por arquivo (2 min) — se travar, mata e retenta


# ─────────────────────────────────────────────────────────────────────
# Auxiliares
# ─────────────────────────────────────────────────────────────────────

def verificar_arquivos_existentes():
    pasta = Settings.RAW_SIH_DIR
    if not pasta.exists():
        return set()
    return {
        arq.stem for arq in pasta.glob("*.parquet")
        if arq.stat().st_size > 0
    }


def _nome_limpo(arq):
    nome = Path(str(arq)).stem
    return nome[:-4] if nome.endswith('.dbc') else nome


def log_desempenho(pasta):
    arquivos = list(pasta.glob("*.parquet"))
    total_reg = 0

    import subprocess
    result = subprocess.run(['du', '-sb', str(pasta)], capture_output=True, text=True)
    total_bytes = int(result.stdout.split()[0])
    
    for arq in arquivos:
        
        try:
            total_reg += pq.read_table(arq).num_rows
            
            
        except Exception:
            continue
    logger.info(
        f"Arquivos: {len(arquivos)} | Registros: {total_reg:,} | "
        f"Tamanho: {total_bytes / (1024**3):.2f} GB"
    )


# ─────────────────────────────────────────────────────────────────────
# Worker — processo separado
# ─────────────────────────────────────────────────────────────────────
# Mensagens via queue:
#   ("ok",   ano)                   — 1 arquivo baixado
#   ("skip", ano)                   — 1 arquivo já existia
#   ("fail", ano, nome, erro)       — falha definitiva
#   ("log",  ano, msg)              — info (erro, reconexão)
#   ("done", ano)                   — ano finalizado
#   ("total", ano, n)              — total de arquivos neste ano
# ─────────────────────────────────────────────────────────────────────

def _worker(args):
    (ano, uf, tipo, meses, existentes, destino_str,
     idx, max_retries, retry_base, stagger, timeout, queue) = args

    from pysus.online_data.SIH import SIH
    from pathlib import Path
    import time, os, sys, threading

    os.environ["TQDM_DISABLE"] = "1"
    devnull = open(os.devnull, 'w')
    real_stdout, real_stderr = sys.stdout, sys.stderr
    destino = Path(destino_str)

    def _silence():
        sys.stdout, sys.stderr = devnull, devnull

    def _restore():
        sys.stdout, sys.stderr = real_stdout, real_stderr

    def _download_com_timeout(sih_inst, arq, dest, secs):
        """Roda sih.download em thread com timeout. Retorna True/False."""
        resultado = [False]
        erro = [None]

        def _run():
            try:
                sih_inst.download([arq], local_dir=dest)
                resultado[0] = True
            except Exception as e:
                erro[0] = e

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        thread.join(timeout=secs)

        if thread.is_alive():
            # Thread travou — não dá pra matar, mas daemon=True garante
            # que morre quando o processo encerrar
            raise TimeoutError(f"Timeout após {secs}s")
        if erro[0]:
            raise erro[0]
        return resultado[0]

    # Stagger
    time.sleep(idx * stagger)

    # Conectar
    try:
        _silence()
        sih = SIH().load()
        _restore()
    except Exception as e:
        _restore()
        queue.put(("log", ano, f"Falha ao conectar: {e}"))
        queue.put(("done", ano))
        devnull.close()
        return

    # Listar
    try:
        _silence()
        arquivos = sih.get_files(tipo, uf=uf, year=ano, month=meses)
        _restore()
    except Exception as e:
        _restore()
        queue.put(("log", ano, f"Erro ao listar: {e}"))
        queue.put(("done", ano))
        devnull.close()
        return

    # Informar total
    queue.put(("total", ano, len(arquivos)))

    # Filtrar e baixar
    for arq in arquivos:
        nome = _nome_limpo(arq)
        if nome in existentes:
            queue.put(("skip", ano))
            continue

        baixou = False
        for t in range(1, max_retries + 1):
            try:
                _silence()
                _download_com_timeout(sih, arq, destino, timeout)
                _restore()
                queue.put(("ok", ano))
                baixou = True
                break
            except Exception as e:
                _restore()
                # Limpar arquivo parcial/corrompido que pode ter ficado em disco
                for parcial in destino.glob(f"*{nome}*"):
                    try:
                        parcial.unlink()
                    except Exception:
                        pass
                if t < max_retries:
                    wait = retry_base * (2 ** (t - 1))
                    queue.put(("log", ano, f"{nome} falhou ({t}/{max_retries}) — {e}"))
                    queue.put(("log", ano, f"Reconectando em {wait}s..."))
                    time.sleep(wait)
                    try:
                        _silence()
                        sih = SIH().load()
                        _restore()
                    except Exception as e2:
                        _restore()
                        queue.put(("log", ano, f"Reconexão falhou: {e2}"))
                else:
                    queue.put(("fail", ano, nome, str(e)))

    devnull.close()
    queue.put(("done", ano))


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────

def main():
    inicio = time.time()

    print("=" * 60)
    print("DOWNLOAD DATASUS — SIH")
    print("=" * 60)

    Settings.create_directories()

    # mkdir logs removido - usa logging centralizado

    uf    = Settings.UF_DEFAULT
    anos  = Settings.get_years_range()
    meses = Settings.MESES
    tipo  = Settings.TIPO_ARQUIVO

    existentes = verificar_arquivos_existentes()
    total_estimado = len(anos) * len(meses) * len(uf)
    restantes = total_estimado - len(existentes)

    mem = psutil.virtual_memory()
    print(f"  UF={uf} | Anos={min(anos)}–{max(anos)} | Tipo={tipo}")
    print(f"  Destino: {Settings.RAW_SIH_DIR}")
    print(f"  Já baixados: {len(existentes)} | Restam: ~{restantes}")
    print(f"  Processos: {MAX_WORKERS} simultâneos")
    print(f"  RAM: {mem.used / (1024**3):.1f} GB em uso | {mem.available / (1024**3):.1f} GB livre | {mem.total / (1024**3):.1f} GB total")

    if restantes <= 0:
        print("\n  Todos os arquivos já estão baixados!")
        return

    print()

    # Queue para comunicação
    manager = Manager()
    queue = manager.Queue()

    # Args para cada ano
    args_list = [
        (ano, uf, tipo, meses, existentes, str(Settings.RAW_SIH_DIR),
         idx, MAX_RETRIES, RETRY_BASE, STAGGER, TIMEOUT, queue)
        for idx, ano in enumerate(anos)
    ]

    # Barras de progresso — uma por ano
    barras = {}
    for ano in anos:
        barras[ano] = tqdm(
            total=len(meses) * len(uf),   # estimativa inicial (12 * UFs)
            desc=f"  {ano}",
            unit="arq",
            ncols=60,
            bar_format="  {desc} {bar} {n_fmt}/{total_fmt}",
            position=anos.index(ano),
            leave=True
        )

    total_sucesso = 0
    total_falhas = []
    anos_concluidos = 0

    # Despachar processos
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for args in args_list:
            executor.submit(_worker, args)

        # Consumir mensagens até todos os anos finalizarem
        while anos_concluidos < len(anos):
            try:
                msg = queue.get(timeout=2)
            except Exception:
                continue

            tipo_msg = msg[0]

            if tipo_msg == "total":
                _, ano, n = msg
                barras[ano].total = n
                barras[ano].refresh()

            elif tipo_msg == "ok":
                _, ano = msg
                barras[ano].update(1)
                total_sucesso += 1

            elif tipo_msg == "skip":
                _, ano = msg
                barras[ano].update(1)

            elif tipo_msg == "fail":
                _, ano, nome, erro = msg
                barras[ano].update(1)
                total_falhas.append((nome, erro))
                tqdm.write(f"         ERRO [{ano}] {nome}: {erro}")

            elif tipo_msg == "log":
                _, ano, texto = msg
                tqdm.write(f"         [{ano}] {texto}")

            elif tipo_msg == "done":
                _, ano = msg
                anos_concluidos += 1
                b = barras[ano]
                if b.n >= b.total:
                    b.bar_format = "  {desc} {bar} {n_fmt}/{total_fmt} OK"
                b.refresh()

    # Fechar barras
    for b in barras.values():
        b.close()

    # Resumo
    print()
    print("=" * 60)
    total_disco = len(verificar_arquivos_existentes())
    print(f"  Novos:     {total_sucesso}")
    print(f"  Em disco:  {total_disco}")
    print(f"  Falhas:    {len(total_falhas)}")

    if total_falhas:
        print(f"\n  Falhas ({len(total_falhas)}):")
        for arq, erro in total_falhas[:15]:
            print(f"    ERRO {arq} — {erro}")
        if len(total_falhas) > 15:
            print(f"    ... +{len(total_falhas) - 15}")

    duracao_min = (time.time() - inicio) / 60
    print(f"\n  Tempo total: {duracao_min:.1f} minutos")
    print("=" * 60)

    log_desempenho(Settings.RAW_SIH_DIR)
    logger.info(f"Tempo: {duracao_min:.1f} min | Novos: {total_sucesso} | Falhas: {len(total_falhas)}")


if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging("download_sih")
    main()