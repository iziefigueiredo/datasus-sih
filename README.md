# DATASUS_SIH

Structured database built from the Brazilian Hospital Information System (SIH/DATASUS). 
This project provides ETL scripts, schema design, and sample data for research, analytics, and predictive modeling in healthcare.

---

## Overview
This repository organizes hospital admission data from SIH/DATASUS in a clean and reproducible way.

Main objectives:
- Standardize and structure raw health data
- Provide ETL and data loading scripts
- Share schema diagrams and documentation
- Enable exploratory analysis and predictive studies

⚠️ **Performance note**: the pipeline processes **23,792,498 hospital admission records** and was tested on a machine with **32GB RAM**, which is recommended for smooth execution. 


---




## Getting Started  

### Requirements

- **Python 3.12**
- **PostgreSQL 12+**
- **Git**

### ⚠️ Platform Compatibility

The `pysus` package (used to download DATASUS data) **does not work on Windows** because it depends on Unix/Linux libraries.

**If you are on Windows, use:**

#### WSL2 (Windows Subsystem for Linux) - Recommended

1. Enable WSL2 on Windows
2. Install Ubuntu from Microsoft Store
3. Follow the Linux/Mac instructions below

#### Or use Docker

---

## Installation Steps

### 1️⃣ Install PostgreSQL


###  Create Database

### Install Python

###


### Create and activate a virtual environment  

```
python -m venv .venv

```

```
source .venv/bin/activate   # Linux/Mac

```

```
.venv\Scripts\activate      # Windows

```
### Install dependencies

```
pip install -r requirements.txt

```

### Run the pipeline  
```
python main.py
```


## Repository Structure

```
Aqui está o conteúdo para o README:

---

```markdown
# datasus-sih

Pipeline ETL/ELT para microdados de internações hospitalares do SUS (SIH/RD),
cobrindo 2008–2024, todas as 27 UFs brasileiras.

---

## Stack

- **Python**: `polars`, `pysus`, `duckdb`, `pandas`
- **Banco analítico**: DuckDB local (`sihrd6.duckdb`)
- **Transformações SQL**: dbt-duckdb
- **Download**: multiprocessing com 6 workers via FTP DATASUS

---

## Fluxo do pipeline

Execute `python main.py` e escolha a etapa no menu:

```
Etapa 1 — Download dos microdados SIH/RD (FTP DATASUS → parquets raw)
Etapa 2 — Download de documentação + tabelas de domínio (TAB_SIH.zip)
Etapa 3 — Extração de indicadores socioeconômicos (IBGE, CNES/LT, CNES/PF, SIM, SINASC)
Etapa 4 — Carga ELT incremental por UF no DuckDB + transformações dbt
```

A Etapa 4 executa automaticamente ao fim:
`dbt deps → dbt seed → dbt run (staging) → dbt test (auditoria)`

---

## Estrutura de diretórios

```
datasus-sih/
├── main.py                        # menu de etapas
├── requirements.txt
├── src/
│   ├── config/
│   │   ├── settings.py            # configurações centralizadas (paths, UFs, anos)
│   │   └── logging_config.py
│   ├── data/
│   │   ├── extract/
│   │   │   ├── download_sih.py          # Etapa 1
│   │   │   ├── download_docs.py         # Etapa 2
│   │   │   ├── download_domain_tables.py
│   │   │   ├── extract_socioeconomic.py # Etapa 3
│   │   │   ├── download_cnes.py
│   │   │   ├── download_sim_sinasc.py
│   │   │   └── datasus_fetch_parallel.py
│   │   ├── transform/
│   │   │   └── preprocess.py      # cast de tipos, datas, normalização por chunk
│   │   └── pipeline_load.py       # Etapa 4: ELT incremental por UF
│   └── database/
│       ├── schema.py              # definição canônica do schema DuckDB
│       └── load.py
├── dbt_sih/                       # projeto dbt principal
│   ├── models/
│   │   ├── sources/               # definições das tabelas fonte
│   │   └── staging/               # correções T2 (stg_*.sql)
│   ├── seeds/                     # tabelas auxiliares (cid_manuais, procedimentos_manuais)
│   ├── tests/                     # SQLs de auditoria
│   ├── dbt_project.yml
│   └── profiles.yml
├── sih_analytics/                 # projeto dbt analítico (views)
│   ├── models/
│   │   ├── sources.yml
│   │   └── data_quality/
│   ├── tests/                     # validações (age_val, cid_padrao, cnes_munic, etc.)
│   └── predictive_models/
│       └── preditivo_transplante.ipynb
└── data/
    ├── raw/
    │   ├── sih/        # parquets brutos RDUF<ano><mes>.parquet
    │   ├── sim/        # óbitos (SIM)
    │   ├── sinasc/     # nascimentos (SINASC)
    │   ├── cnes_lt/    # leitos (CNES/LT)
    │   └── cnes_pf/    # profissionais (CNES/PF)
    ├── interim/        # artefatos intermediários
    ├── processed/      # parquets finais prontos para carga
    ├── support/        # tabelas de domínio CSV
    └── backups/        # snapshots automáticos
```



## Dependências

```bash
pip install -r requirements.txt
```

Requer Python 3.10+.
```

```

Developed by Isadora Figueiredo and Victoria Marques


