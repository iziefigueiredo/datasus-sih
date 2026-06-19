"""
Modulo de processamento de dados do projeto DataVisSUS

Subpacotes:
    extract/   — download de dados de fontes externas (DATASUS, IBGE)
    transform/ — pre-processamento
"""

from .transform.preprocess import SIHPreprocessor, main as preprocess_main

__all__ = [
    'SIHPreprocessor', 'preprocess_main',
]