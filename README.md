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
│  ├─ interim/                     # Intermediate transformations during preprocessing/unification
│  ├─ processed/                   # Final datasets ready to be loaded into PostgreSQL
│  └─ support/                     # Auxiliary data (IBGE, IPEA, CID-10, etc.)
│
│ 
├─ src/                            # Source code (ETL and database)
│  ├─ config/                      # Global settings
│  │  ├─ __init__.py
│  │  └─ settings.py               # Paths, DB, UF, years, months
│  │
│  ├─ data/                        # ETL scripts
│  │  ├─ __init__.py
│  │  ├─ download.py               # EXTRACT: Download DATASUS → parquet
│  │  ├─ unify.py                  # TRANSFORM 1: Merge parquet files
│  │  ├─ preprocess.py             # TRANSFORM 2: Clean & standardize
│  │  ├─ aggregate.py              # TRANSFORM 3: Aggregations 
│  │  └─ split.py                  # TRANSFORM 4: Split into fact/dim tables
│  │
│  ├─ database/                    # Database schema and loader
│  │  ├─ __init__.py
│  │  ├─ schema.py                 # Table schemas (columns, PK, FK, types)
│  │  └─ load.py                   # LOAD: Insert parquet tables into PostgreSQL
│
│
├─ sih_analytics/                  # Analytical and governance layer
│  ├─ dbt/                         # dbt project for SQL and YAML validations
│  │  ├─ models/
│  │  ├─ seeds/
│  │  └─ tests/
│  │
│  ├─ predictive_models/           # Predictive layer (Python ML models)
│  │  ├─ model.py
│  │  ├─ preprocessing.py          # Optional: data preparation for ML
│  │  └─ results/                  # Saved outputs (predictions, metrics)
│
├─ docs/                           # Documentation
│
├─ reports/                        # Generated reports (logs, quality, viz)
│  ├─ logs/
│
├─ .gitignore                      # Ignore rules (exclude large data folders)
├─ requirements.txt                # Dependencies
├─ main.py                         # Orchestration: runs ETL steps 1–5
└─ README.md                       # Documentation: install, run, DB setup


```

Developed by Isadora Figueiredo and Victoria Marques


