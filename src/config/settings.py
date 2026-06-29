# src/config/settings.py
#
# Fonte para configurações do pipeline SIH/SUS.
# Princípio DAMA: metadados de pipeline são dados — devem ser gerenciados
# com o mesmo rigor que os dados que descrevem.
#
# Convenção de idioma:
#   EN — infraestrutura (métodos, variáveis de código)
#   PT — domínio (constantes do modelo, nomes de coluna, conceitos DATASUS/IBGE)

from pathlib import Path
from typing import List, Tuple


class Settings:
    """
    Configurações centralizadas do pipeline SIH/SUS.

    Organização por camada de dados (linhagem DAMA):
        1. Paths          — localização física de cada camada
        2. Fontes         — parâmetros de extração DATASUS/IBGE
        3. Processamento  — parâmetros de transformação
        4. Banco          — conexão DuckDB (analítico) e PostgreSQL (futuro)
        5. Arquivos apoio — tabelas de domínio (dimensões)
        6. Filenames      — nomes canônicos dos artefatos gerados
    """

    # =========================================================================
    # 1. PATHS — linhagem: raw → interim → processed
    # =========================================================================

    SRC_DIR  = Path(__file__).parent.parent
    BASE_DIR = SRC_DIR.parent
    DATA_DIR = BASE_DIR / "data"

    # Camada raw — dados brutos por sistema fonte 
    RAW_DIR         = DATA_DIR / "raw"
    RAW_SIH_DIR     = DATA_DIR / "raw" / "sih"      # SIH/RD — internações
    RAW_SIM_DIR     = DATA_DIR / "raw" / "sim"      # SIM/CID10 — óbitos
    RAW_SINASC_DIR  = DATA_DIR / "raw" / "sinasc"   # SINASC/DN — nascimentos
    RAW_CNES_LT_DIR = DATA_DIR / "raw" / "cnes_lt"  # CNES/LT — leitos
    RAW_CNES_PF_DIR = DATA_DIR / "raw" / "cnes_pf"  # CNES/PF — profissionais

    # Camada interim — transformações intermediárias 
    INTERIM_DIR = DATA_DIR / "interim"

    # Camada processed — artefatos finais prontos para carga
    PROCESSED_DIR = DATA_DIR / "processed"

    # Suporte — tabelas de domínio e dimensões estáticas
    SUPPORT_FILES_DIR = DATA_DIR / "support"

    # Backups — snapshots de segurança
    BACKUPS_DIR = DATA_DIR / "backups"

    # =========================================================================
    # 2. FONTES — parâmetros de extração DATASUS/IBGE
    # =========================================================================

    # Escopo padrão de UFs — pode ser sobrescrito via CLI em cada script
    UF_DEFAULT = ["AC"]
    
    # Janela temporal do pipeline
    ANOS_INICIO  = 2008
    ANOS_FIM     = 2024
    MESES        = list(range(1, 13))

    # Tipo de arquivo SIH: RD = Dados Reduzidos (sem dados sensíveis nominais)
    TIPO_ARQUIVO = "RD"

    # Processos simultâneos para download (multiprocessing)
    # Cada processo abre sua própria conexão FTP isolada.
    # 2–4 recomendado — o FTP do DATASUS limita banda por IP.
    DOWNLOAD_WORKERS = 6

    # =========================================================================
    # 3. PROCESSAMENTO
    # =========================================================================

    POLARS_CONFIG = {
        "n_threads": None,   # None = usa todos os núcleos disponíveis
        "streaming": True,   # processa em chunks para controle de memória
    }

    CHUNK_SIZE = 1_000  # linhas por chunk em operações pandas legacy

    # =========================================================================
    # 4. BANCO DE DADOS
    # =========================================================================

    # DuckDB — banco analítico local gerado pelo pipeline
    DB_PATH = BASE_DIR / "sihrd6.duckdb"

    # PostgreSQL — reservado para deploy em ambiente compartilhado
    DB_CONFIG = {
        "host":     "localhost",
        "port":     5432,
        "database": "sihrd5",
        "user":     "postgres",
        "password": "1234",  # substituir por variável de ambiente em produção
    }

    # =========================================================================
    # 5. ARQUIVOS DE APOIO — tabelas de domínio (dimensões estáticas)
    #
    # Fonte: TAB_SIH.zip (FTP DATASUS) via download_docs.py
    # Exceto: municipios.csv, cid.csv,
    #         especialidade.csv, etnia.csv 
    # =========================================================================

    SUPPORT_FILES = {
        "municipios":     "municipios.csv",     
        "cid":            "cid.csv",            
        "procedimentos":  "procedimentos.csv",  
        "especialidade":  "especialidade.csv", 
        "nacionalidade":  "nacionalidade.csv",  # TAB_SIH / NACION3D.cnv
        "instrucao":      "instrucao.csv",      # TAB_SIH / INSTRU.cnv
        "raca_cor":       "raca_cor.csv",       # TAB_SIH / RACACOR.cnv
        "sexo":           "sexo.csv",           # TAB_SIH / SEXO.cnv
        "vincprev":       "vincprev.csv",       # TAB_SIH / VINCPREV.cnv
        "contraceptivos": "contraceptivos.csv", # TAB_SIH / CONTRAC.cnv
        "car_int":        "car_int.csv",        # TAB_SIH / CARATEND.cnv — caráter da internação
        "cbor":           "cbor.csv",           # TAB_SIH / CBO.cnv — CBO reduzido
        "marca_uti":      "marca_uti.csv",      # TAB_SIH / MARCAUTI.cnv
        "etnia":          "etnia.csv",          # IBGE — etnias indígenas
        "complexidade":   "complexidade.csv",   # TAB_SIH / COMPLEX2.cnv — complexidade assistencial
        "regsaud":        "regsaud.csv",        # TAB_SIH / br_regsaud.cnv — regiões de saúde (TD_MUNICIPIO.NO_REGIAO_SAUDE)
        "cadhosp":        "cadhosp.csv",        # TAB_SIH / CADHOSP.DBF — razão social hospitais (TD_HOSPITAL.NO_HOSPITAL)
        # socioeconomico: gerado pelo pipeline (parquet) — não é arquivo de apoio estático
    }

    # =========================================================================
    # 6. FILENAMES 
    #
    # Convenção: <sistema>_<escopo>_<estado>.parquet
    # Interim: artefatos intermediários 
    # Processed: artefatos finais prontos para carga no banco
    # =========================================================================

    # SIH — camada interim (estados do processamento)
    PARQUET_UNIFICADO_FILENAME  = "sih_unified.parquet"
    PARQUET_TRATADO_FILENAME    = "sih_unified_tratado.parquet"
    PARQUET_CONTRAIDO_FILENAME  = "sih_unified_contraido.parquet"

    # Fatos — camada processed (prefixo TF_ no modelo)
    INTERNACOES_FILENAME = "internacoes.parquet"

    # Relacionamento N:N — camada processed (prefixo RL_ no modelo)
    ATENDIMENTO_FILENAME = "internacao_procedimento.parquet"  # RL_INTERNACAO_PROCEDIMENTO

    # Dimensões — camada processed (prefixo TD_ no modelo)
    MUNICIPIOS_FILENAME       = "municipios.parquet"
    PROCEDIMENTOS_FILENAME    = "procedimentos.parquet"
    CID10_FILENAME            = "cid.parquet"
    NACIONALIDADE_FILENAME    = "nacionalidade.parquet"
    INSTRUCAO_FILENAME        = "instrucao.parquet"
    ETNIA_FILENAME            = "etnia.parquet"
    RACA_FILENAME             = "raca_cor.parquet"
    SEXO_FILENAME             = "sexo.parquet"
    ESPECIALIDADE_FILENAME    = "especialidade.parquet"
    VINCPREV_FILENAME         = "vincprev.parquet"
    CONTRACEPTIVOS_FILENAME   = "contraceptivos.parquet"
    CAR_INT_FILENAME          = "car_int.parquet"
    CBOR_FILENAME             = "cbor.parquet"
    MARCA_UTI_FILENAME        = "marca_uti.parquet"
    COMPLEXIDADE_FILENAME     = "complexidade.parquet"    
    HOSPITAL_FILENAME         = "hospital.parquet"       
    TEMPO_FILENAME            = "tempo.parquet"

    # Socioeconômico — gerado pelo pipeline socioeconômico
    # Cobertura temporal por fonte:
    #   VL_PIB_PERCAPITA   : 2008–2021 (lag ~2 anos, IBGE)
    #   VL_MORT_INFANTIL   : 2008–2023 (SIM+SINASC)
    #   VL_LEITOS_SUS_1000 : 2008–2023 (CNES/LT)
    #   VL_MEDICOS_1000    : 2008–2023 (CNES/PF)
    POPULACAO_FILENAME      = "populacao.parquet"
    PIB_FILENAME            = "pib_percapita.parquet"
    MORT_INFANTIL_FILENAME  = "mort_infantil.parquet"
    LEITOS_FILENAME         = "leitos.parquet"
    MEDICOS_FILENAME        = "medicos.parquet"
    SOCIOECONOMICO_FILENAME = "socioeconomico.parquet"
    IPEA_FILENAME           = "ipea_saude.parquet"
    # =========================================================================
    # MÉTODOS
    # =========================================================================

    @classmethod
    def get_years_range(cls) -> List[int]:
        """Retorna lista de anos do pipeline conforme janela temporal configurada."""
        return list(range(cls.ANOS_INICIO, cls.ANOS_FIM + 1))

    @classmethod
    def get_full_period(cls) -> List[Tuple[int, int]]:
        """Retorna produto cartesiano ano × mês para o período completo."""
        return [
            (ano, mes)
            for ano in cls.get_years_range()
            for mes in cls.MESES
        ]

    @classmethod
    def create_directories(cls) -> None:
        """
        Cria a estrutura de diretórios do pipeline.
        Idempotente — seguro para chamar múltiplas vezes.
        """
        directories = [
            cls.DATA_DIR,
            cls.RAW_DIR,
            cls.RAW_SIH_DIR,
            cls.RAW_SIM_DIR,
            cls.RAW_SINASC_DIR,
            cls.RAW_CNES_LT_DIR,
            cls.RAW_CNES_PF_DIR,
            cls.INTERIM_DIR,
            cls.PROCESSED_DIR,
            cls.SUPPORT_FILES_DIR,
            cls.BACKUPS_DIR,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_support_file_path(cls, file_key: str) -> Path:
        """
        Retorna o path completo de um arquivo de apoio pelo seu identificador.
            ValueError: se o identificador não existir em SUPPORT_FILES.
        """
        if file_key not in cls.SUPPORT_FILES:
            available = list(cls.SUPPORT_FILES.keys())
            raise ValueError(
                f"Arquivo de apoio '{file_key}' não encontrado. "
                f"Disponíveis: {available}"
            )
        return cls.SUPPORT_FILES_DIR / cls.SUPPORT_FILES[file_key]

    @classmethod
    def debug_paths(cls) -> None:
        """Exibe todos os paths configurados. Útil para diagnóstico de ambiente."""
        paths = {
            "SRC_DIR":           cls.SRC_DIR,
            "BASE_DIR":          cls.BASE_DIR,
            "DATA_DIR":          cls.DATA_DIR,
            "RAW_DIR":           cls.RAW_DIR,
            "RAW_SIH_DIR":       cls.RAW_SIH_DIR,
            "RAW_SIM_DIR":       cls.RAW_SIM_DIR,
            "RAW_SINASC_DIR":    cls.RAW_SINASC_DIR,
            "RAW_CNES_LT_DIR":   cls.RAW_CNES_LT_DIR,
            "RAW_CNES_PF_DIR":   cls.RAW_CNES_PF_DIR,
            "INTERIM_DIR":       cls.INTERIM_DIR,
            "PROCESSED_DIR":     cls.PROCESSED_DIR,
            "SUPPORT_FILES_DIR": cls.SUPPORT_FILES_DIR,
            "BACKUPS_DIR":       cls.BACKUPS_DIR,
            "DB_PATH":           cls.DB_PATH,
        }
        print("=== DEBUG PATHS ===")
        for name, path in paths.items():
            exists = "✓" if path.exists() else "✗"
            print(f"  {exists} {name:<20} {path}")
        print("===================")