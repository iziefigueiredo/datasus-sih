# src/config/logging_config.py
#
# Configuracao centralizada de logging do pipeline SIH/SUS.
#
# Principio DAMA: logs sao metadados de execucao — devem ser gerenciados
# com o mesmo rigor que os dados que descrevem.
#
# Regras:
#   1. NENHUM modulo chama logging.basicConfig() — so este modulo configura handlers.
#   2. Cada modulo usa apenas: logger = logging.getLogger(__name__)
#   3. O ponto de entrada (main.py ou __main__) chama setup_logging() UMA VEZ.
#   4. Todos os logs vao para: {BASE_DIR}/logs/
#
# Convencao de arquivos:
#   step_name fornecido  ->  logs/{step_name}.log
#   step_name omitido    ->  logs/pipeline.log

import logging
import sys
from pathlib import Path


_SRC_DIR = Path(__file__).parent.parent          # src/
_BASE_DIR = _SRC_DIR.parent                       # projeto/
LOGS_DIR = _BASE_DIR / "logs"

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    step_name: str = None,
    level: int = logging.INFO,
    log_dir: Path = None,
) -> Path:
    """
    Configura o logging do pipeline.

    Deve ser chamado UMA VEZ pelo ponto de entrada (main.py ou bloco
    __main__ de cada script standalone). Todos os loggers filhos herdam
    esta configuracao automaticamente.

    Args:
        step_name: Nome da etapa (define o nome do arquivo de log).
        level:     Nivel de logging (default: INFO).
        log_dir:   Diretorio de logs. Se None, usa {BASE_DIR}/logs/.

    Returns:
        Path do arquivo de log criado.
    """
    log_dir = log_dir or LOGS_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{step_name}.log" if step_name else "pipeline.log"
    log_filepath = log_dir / filename

    # Limpa handlers existentes (evita duplicacao quando chamado via menu)
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Handler: arquivo (append — preserva historico entre execucoes)
    file_handler = logging.FileHandler(log_filepath, mode="a", encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Handler: console
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    return log_filepath
