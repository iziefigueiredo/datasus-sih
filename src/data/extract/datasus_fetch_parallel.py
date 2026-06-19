"""
Download paralelo de dados DATASUS via pysus
Localização: sihrd5/src/data/extract/datasus_fetch_parallel.py

Arquitetura:
  - subprocess.run: cada download roda em processo Python ISOLADO
  - Necessário porque pysus polui estado global após get_files()
    (instâncias subsequentes no mesmo processo retornam lista vazia)
  - ThreadPoolExecutor: N threads lançam subprocessos em paralelo
  - Retry com backoff exponencial

Trabalho dividido por UF+ano — cada subprocess baixa 1 UF × 1 ano.
Suporta CNES, SIM e SINASC.

"""

import sys
import json
import time
import subprocess
import textwrap
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


# ─────────────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────────────

DEFAULT_WORKERS = 3
TIMEOUT         = 600     
MAX_RETRIES     = 3
RETRY_BASE      = 5      
STAGGER         = 1       # intervalo entre lançamento de workers


# ─────────────────────────────────────────────────────────────────────
# Script que roda em subprocess isolado
# ─────────────────────────────────────────────────────────────────────

_SUBPROCESS_SCRIPT = textwrap.dedent(r'''
import sys, os, json

os.environ["TQDM_DISABLE"] = "1"

sistema  = sys.argv[1]
grupo    = sys.argv[2]
uf       = sys.argv[3]
ano      = int(sys.argv[4])
destino  = sys.argv[5]
meses    = json.loads(sys.argv[6])

# Redirecionar stdout/stderr do pysus para /dev/null
import io
_real_stdout = sys.stdout
sys.stdout = io.StringIO()
sys.stderr = io.StringIO()

try:
    if sistema == "CNES":
        from pysus.online_data.CNES import CNES
        inst = CNES().load()
        arquivos = inst.get_files(grupo, uf=uf, year=[ano], month=meses)
    elif sistema == "SIM":
        from pysus.online_data.SIM import SIM
        inst = SIM().load()
        arquivos = inst.get_files(grupo, uf=uf, year=[ano])
    elif sistema == "SINASC":
        from pysus.online_data.SINASC import SINASC
        inst = SINASC().load()
        arquivos = inst.get_files(grupo, uf=uf, year=[ano])
    else:
        raise ValueError(f"Sistema não suportado: {sistema}")

    if not arquivos:
        sys.stdout = _real_stdout
        print(json.dumps({"status": "empty", "n": 0}))
        sys.exit(0)

    # Download
    if sistema == "CNES":
        for arq in arquivos:
            inst.download(arq, local_dir=destino)
    else:
        list(inst.download(arquivos, local_dir=destino))

    sys.stdout = _real_stdout
    print(json.dumps({"status": "ok", "n": len(arquivos)}))

except Exception as e:
    sys.stdout = _real_stdout
    print(json.dumps({"status": "error", "error": str(e)}))
    sys.exit(1)
''')


# ─────────────────────────────────────────────────────────────────────
# Download de 1 UF × 1 ano via subprocess
# ─────────────────────────────────────────────────────────────────────

def _download_uf_ano(sistema, grupo, uf, ano, destino, meses,
                     timeout, max_retries, retry_base):
    """
    Baixa todos os meses de 1 UF × 1 ano em subprocess isolado.
    Retorna (uf, ano, n_baixados, erro_ou_None).
    """
    meses_json = json.dumps(meses)

    for tentativa in range(1, max_retries + 1):
        try:
            result = subprocess.run(
                [sys.executable, "-c", _SUBPROCESS_SCRIPT,
                 sistema, grupo, uf, str(ano), str(destino), meses_json],
                capture_output=True, text=True, timeout=timeout,
            )

            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout.strip())
                if data["status"] == "ok":
                    return (uf, ano, data["n"], None)
                elif data["status"] == "empty":
                    return (uf, ano, 0, None)
                else:
                    erro = data.get("error", "desconhecido")
                    if tentativa < max_retries:
                        time.sleep(retry_base * (2 ** (tentativa - 1)))
                        continue
                    return (uf, ano, 0, erro)
            else:
                erro = result.stderr.strip()[:200] if result.stderr else "retorno vazio"
                if tentativa < max_retries:
                    time.sleep(retry_base * (2 ** (tentativa - 1)))
                    continue
                return (uf, ano, 0, erro)

        except subprocess.TimeoutExpired:
            if tentativa < max_retries:
                time.sleep(retry_base * (2 ** (tentativa - 1)))
                continue
            return (uf, ano, 0, f"timeout ({timeout}s)")

        except Exception as e:
            if tentativa < max_retries:
                time.sleep(retry_base * (2 ** (tentativa - 1)))
                continue
            return (uf, ano, 0, str(e))

    return (uf, ano, 0, "max retries")


# ─────────────────────────────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────────────────────────────

def datasus_fetch_parallel(
    sistema: str,
    grupo: str,
    ufs: list,
    anos: list,
    destino: Path,
    meses: list = None,
    max_workers: int = DEFAULT_WORKERS,
    timeout: int = TIMEOUT,
    max_retries: int = MAX_RETRIES,
    **kwargs,  
) -> dict:
    """
    Download paralelo de dados DATASUS via pysus.

    Cada task = 1 UF × 1 ano, executado em subprocess Python isolado.
    Isso evita a poluição de estado global do pysus.

    Args:
        sistema:      "CNES", "SIM" ou "SINASC"
        grupo:        subtipo — "LT", "PF", "CID10", "DN"
        ufs:          siglas de UF (ex: ["RS"])
        anos:         anos (ex: list(range(2008, 2024)))
        destino:      diretório de saída (Path)
        meses:        meses (default: 1–12, relevante para CNES)
        max_workers:  subprocessos simultâneos (default: 3)
        timeout:      segundos por subprocess (default: 180)
        max_retries:  tentativas por task (default: 3)

    Returns:
        {"baixados": int, "falhas": int, "total": int, "skipped_anos": int}
    """
    if meses is None:
        meses = list(range(1, 13))

    destino.mkdir(parents=True, exist_ok=True)

    # Criar tasks: 1 por UF × ano
    tasks = [(uf, ano) for uf in ufs for ano in anos]
    n_workers = min(max_workers, len(tasks))

    label = f"{sistema}/{grupo}"
    print(f"  {label}: {len(ufs)} UFs × {len(anos)} anos = {len(tasks)} tasks | "
          f"{n_workers} workers | timeout={timeout}s | retries={max_retries}")

    inicio = time.time()
    baixados = 0
    falhas = 0
    skipped_anos = 0
    total_arquivos = 0
    ufs_done = set()

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {}
        for i, (uf, ano) in enumerate(tasks):
            # Stagger para não sobrecarregar FTP
            if i > 0 and i % n_workers == 0:
                time.sleep(STAGGER)

            fut = pool.submit(
                _download_uf_ano,
                sistema, grupo, uf, ano, destino, meses,
                timeout, max_retries, RETRY_BASE,
            )
            futures[fut] = (uf, ano)

        for fut in as_completed(futures):
            uf, ano = futures[fut]
            try:
                _uf, _ano, n, erro = fut.result()
            except Exception as e:
                n, erro = 0, str(e)

            if erro:
                falhas += 1
                if falhas <= 5:
                    print(f"\n  ✗ {uf}/{ano}: {erro}")
                elif falhas == 6:
                    print(f"\n  ... suprimindo falhas subsequentes")
            elif n == 0:
                skipped_anos += 1
            else:
                baixados += n
                total_arquivos += n

            ufs_done.add(uf)
            elapsed = time.time() - inicio
            print(f"\r  {label}: {baixados} baixados | "
                  f"UFs: {len(ufs_done)}/{len(ufs)} [{elapsed:.0f}s]",
                  end="", flush=True)

    elapsed = time.time() - inicio
    print(f"\n  {label}: {baixados} baixados, {falhas} falhas, "
          f"{skipped_anos} anos sem dados ({elapsed:.1f}s)")

    return {
        "baixados": baixados,
        "falhas": falhas,
        "total": total_arquivos,
        "skipped_anos": skipped_anos,
    }