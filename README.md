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
datasus-sih/
├─ data/                           # Data layer
│  ├─ raw/                         # Raw parquet files downloaded from DATASUS
│  ├─ interim/                     # Unified and preprocessed parquet files
│  └─ support/                     # Lookup CSVs (cid10.csv, municipios.csv, procedimentos.csv)
│
├─ docs/                           # Documentation (for GitHub/Pages)
│  ├─ diagrams/                    # Schema and ETL diagrams
│  ├─ decisions/                   # Architecture decision records
│  └─ reports/                     # Optional: copies of summary.md reports for visualization
│
├─ reports/                        # Generated Reports (logs/data quality/viz)
│  ├─ logs/                   
│
├─ src/                            # Source code (pipeline and reports)
│  ├─ config/
│  │  ├─ __init__.py
│  │  └── settings.py              # Global parameters (paths, DB, UF, years, months)
│  │
│  ├─ data/                        # ETL scripts
│  │  ├─ __init__.py
│  │  ├─ download.py               # EXTRACT: Download DATASUS → parquet
│  │  ├─ unify.py                  # TRANSFORM 1: Merge parquet files
│  │  ├─ preprocess.py             # TRANSFORM 2: Clean & standardize
│  │  ├─ aggregate.py              # TRANSFORM 3: Contract 
│  │  └─ split.py                  # TRANSFORM 4: Split into fact/dim tables
│  │
│  ├─ database/                    # Database schema and loader
│  │  ├─ __init__.py
│  │  ├─ schema.py                 # Table schemas (columns, PK, FK, types)
│  │  └─ load.py                   # LOAD: Insert parquet tables into PostgreSQL
│  │
│
├─ .gitignore                      # Ignore rules (exclude data/raw, interim, etc.)
├─ requirements.txt                # Dependencies
├─ main.py                         # Orchestration (menu to run ETL steps 1–5)
└─ README.md                       # Documentation: how to install, run, DB setup

```

Developed by Isadora Figueiredo and Victoria Marques


