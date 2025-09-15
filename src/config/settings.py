# src/config/settings.py


import os
from pathlib import Path
from typing import List, Tuple


class Settings:
    """Configurações centralizadas do projeto SIH/SUS"""
    
    # === CONFIGURAÇÕES DE PATHS ===
    # Base: arquivo está em projeto_sih/src/config/settings.py
    SRC_DIR = Path(__file__).parent.parent          # src/config/../ = src/
    BASE_DIR = SRC_DIR.parent                       # src/../ = projeto_sih/
    DATA_DIR = BASE_DIR / "data"                    # projeto_sih/data/
    
    # Subpastas de dados
    RAW_DIR = DATA_DIR / "raw"                      
    INTERIM_DIR = DATA_DIR / "interim"              
    PROCESSED_DIR = DATA_DIR / "processed"  
    SUPPORT_FILES_DIR = DATA_DIR / "support"        
    BACKUPS_DIR = DATA_DIR / "backups"      
      


    # === CONFIGURAÇÕES DO DATASUS ===
    # Estados de interesse
    UF_DEFAULT = "RS"
    
    # Período de dados
    ANOS_INICIO = 2008
    ANOS_FIM = 2023
    MESES = list(range(1, 13))  # Todos os meses
    
    # Tipo de arquivo SIH
    TIPO_ARQUIVO = "RD"  # RD = Dados reduzidos
    
    # === CONFIGURAÇÕES DE PROCESSAMENTO ===
    # Configurações do Polars
    POLARS_CONFIG = {
        "n_threads": None,  # Usar todos os threads disponíveis
        "streaming": True,  # Para datasets grandes
    }
    
    # Tamanho de chunk para processamento em lotes
    CHUNK_SIZE = 1000
    
    # === CONFIGURAÇÕES DE BANCO ===
    DB_CONFIG = {
        "host": "localhost",
        "port": 5432,
        "database": "sih_rs",
        "user": "postgres",
        "password": "1234"  # Mudar em produção
    }
    
    # === ARQUIVOS DE APOIO ===
    SUPPORT_FILES = {
        "procedimentos": "procedimentos.csv",
        "municipios": "municipios.csv", 
        "cid10": "cid10.csv"
    }

    # --- CORREÇÃO: Adicionado PARQUET_TREATED_FILENAME como atributo de classe ---
    PARQUET_TREATED_FILENAME = "sih_rs_tratado.parquet"

    PARQUET_UNIFIED_FILENAME = "sih_rs.parquet"
    # --- FIM DA CORREÇÃO ---

    INTERNACOES_FILENAME = "internacoes.parquet"
    UTI_DETALHES_FILENAME = "uti_detalhes.parquet"
    CONDICOES_ESPECIFICAS_FILENAME = "condicoes_especificas.parquet" 
    HOSPITAL_FILENAME = "hospital.parquet"
    MUNICIPIOS_FILENAME = "municipios.parquet"
    PROCEDIMENTOS_FILENAME = "procedimentos.parquet"
    CID10_FILENAME = "cid10.parquet"
    DIAGNOSTICOS_SECUNDARIOS_FILENAME = "diagnosticos_secundarios.parquet"
    OBSTETRICOS_FILENAME = "obstetricos.parquet"
    INSTRUCAO_FILENAME = "instrucao.parquet"
    ETINA_FILENAME = "etnia.parquet"
    MORTES_FILENAME = "mortes.parquet"
    INFEHOSP_FILENAME = "infehosp.parquet"
    VINCPREV_FILENAME = "vincprev.parquet"
    CBOR_FILENAME = "cbor.parquet"
    CONTRACEPTIVOS_FILENAME = "contraceptivos.parquet"
    CID_NOTIF_FILENAME = "notificacoes.parquet"
    PERNOITE_FILENAME = "pernoite.parquet" 
    DIAG_FILENAME = "diagnosticos.parquet"

  


    @classmethod
    def get_anos_range(cls) -> List[int]:
        """Retorna lista com range de anos configurado"""
        return list(range(cls.ANOS_INICIO, cls.ANOS_FIM + 1))
    
    @classmethod
    def get_periodo_completo(cls) -> List[Tuple[int, int]]:
        """Retorna lista de tuplas (ano, mês) para todo o período"""
        periodo = []
        for ano in cls.get_anos_range():
            for mes in cls.MESES:
                periodo.append((ano, mes))
        return periodo
    
    @classmethod
    def criar_diretorios(cls) -> None:
        """Cria todos os diretórios necessários"""
        diretorios = [
            cls.BASE_DIR,
            cls.DATA_DIR,
            cls.RAW_DIR,
            cls.INTERIM_DIR,
            cls.PROCESSED_DIR,
            cls.SUPPORT_FILES_DIR,
            cls.BACKUPS_DIR,
            
        ]
        
        for dir_path in diretorios:
            dir_path.mkdir(parents=True, exist_ok=True)
            
    @classmethod
    def get_support_file_path(cls, file_key: str) -> Path:
        """Retorna o path completo para um arquivo de apoio"""
        if file_key not in cls.SUPPORT_FILES:
            raise ValueError(f"Arquivo de apoio '{file_key}' não encontrado")
        
        return cls.SUPPORT_FILES_DIR / cls.SUPPORT_FILES[file_key]

    @classmethod
    def debug_paths(cls) -> None:
        """Método para debug - mostra todos os paths configurados"""
        print("=== DEBUG PATHS ===")
        print(f"SRC_DIR:              {cls.SRC_DIR}")
        print(f"BASE_DIR:             {cls.BASE_DIR}")
        print(f"DATA_DIR:             {cls.DATA_DIR}")
        print(f"RAW_DIR:              {cls.RAW_DIR}") 
        print(f"INTERIM_DIR:          {cls.INTERIM_DIR}")
        print(f"PROCESSED_DIR:        {cls.PROCESSED_DIR}")
        print(f"SUPPORT_FILES_DIR:    {cls.SUPPORT_FILES_DIR}") 
        print("===================")

