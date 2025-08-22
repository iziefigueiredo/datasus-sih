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

---

## Repository Structure

```
datasus-sih/
│── data/
│   │── support/
│── docs/
│── src/
│   │── config/
│       │── __init__.py
│       │── settings.py
│   │── data/
│       │── __init__.py
│       │── download.py
│       │── unify.py
│       │── preprocess.py
│       │── split.py
│── requirements.txt
│── README.md
│── .gitignore
```

Developed by Isadora Figueiredo and Victoria Marques

