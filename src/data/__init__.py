"""
Módulo de processamento de dados do projeto SIH/SUS
"""

from .unify import SIHUnifier, main as unify_main
from .preprocess import SIHPreprocessor, main as preprocess_main
from .split import TableSplitter, main as split_main



__all__ = [
    'SIHUnifier', 'unify_main',
    'SIHPreprocessor', 'preprocess_main',
    'TableSplitter', 'split_main'
]
