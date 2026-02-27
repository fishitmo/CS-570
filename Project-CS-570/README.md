# CS570 — Big Data Processing & Analytics
### MovieLens 1M · End-to-End Data Pipeline · Spring 2026

> **Team repo:** `fishitmo/CS-570`
> **Stack:** PySpark · Streamlit · Jupyter · GitHub Actions
> **Dataset:** [MovieLens 1M](https://grouplens.org/datasets/movielens/1m/) — 1M ratings, 6K users, 3.9K movies

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA LAYER                               │
│  data/raw/           MovieLens .dat files (local, gitignored)   │
│  data/processed/     Parquet cache (local, gitignored)          │
└──────────────────────────────┬──────────────────────────────────┘
                               │ PySpark read/write
┌──────────────────────────────▼──────────────────────────────────┐
│                      PROCESSING LAYER  (src/)                   │
│  spark_session.py    SparkSession factory                       │
│  data_loader.py      Load .dat → typed DataFrames               │
│  eda.py              Reusable analysis functions                 │
│  utils.py            Helpers (path resolution, caching)         │
└──────────┬──────────────────────────────┬───────────────────────┘
           │                              │
┌──────────▼───────────┐    ┌─────────────▼──────────────────────┐
│   NOTEBOOK LAYER     │    │        APP LAYER  (app/)            │
│   notebooks/         │    │  Streamlit multi-page dashboard     │
│   D1 → D5 course     │    │  ┌─ main.py (entry point)          │
│   deliverables       │    │  ├─ 01 Data Overview                │
│                      │    │  ├─ 02 EDA & Visualizations         │
└──────────────────────┘    │  ├─ 03 Top Movies & Genre Trends    │
                            │  └─ 04 Recommendations (D4/D5)      │
                            └─────────────────────────────────────┘
```

## Project Structure

```
Project-CS-570/
├── README.md
├── .gitignore
├── requirements.txt
│
├── data/
│   ├── raw/                    # Place ml-1m/ files here (gitignored)
│   │   ├── ratings.dat
│   │   ├── users.dat
│   │   └── movies.dat
│   └── processed/              # Parquet cache (gitignored)
│
├── notebooks/                  # Course deliverables — one per team
│   ├── D1_data_loading_eda.ipynb          ← Due Week 7 (Feb 25)
│   ├── D2_data_cleaning.ipynb             ← Due Week 9
│   ├── D3_feature_engineering.ipynb       ← Due Week 11
│   ├── D4_modeling.ipynb                  ← Due Week 13
│   └── D5_final_report.ipynb              ← Due Week 15
│
├── src/                        # Reusable modules shared across notebooks + app
│   ├── __init__.py
│   ├── spark_session.py        # get_spark() factory
│   ├── data_loader.py          # load_ratings(), load_users(), load_movies(), join_all()
│   ├── eda.py                  # eda functions called by both notebooks and app
│   └── utils.py                # path helpers, data directory resolution
│
├── app/                        # Streamlit dashboard
│   ├── main.py                 # streamlit run app/main.py
│   └── pages/
│       ├── 01_📊_Data_Overview.py
│       ├── 02_🔍_EDA.py
│       ├── 03_⭐_Top_Movies.py
│       └── 04_🤖_Recommendations.py      ← built in D4/D5
│
├── tests/
│   └── test_data_loader.py
│
└── .github/
    ├── workflows/
    │   └── ci.yml              # GitHub Actions: validate notebooks on PR
    └── PULL_REQUEST_TEMPLATE.md
```

---

## Five Deliverables at a Glance

| # | Deliverable | Due | Branch |
|---|-------------|-----|--------|
| D1 | Data Loading & EDA | Week 7 (Feb 25) | `feature/D1` |
| D2 | Data Cleaning | Week 9 | `feature/D2` |
| D3 | Feature Engineering | Week 11 | `feature/D3` |
| D4 | ML Modeling | Week 13 | `feature/D4` |
| D5 | Final Report | Week 15 | `feature/D5` |

---

## Git Branching Strategy (Team of 5)

```
main         ← stable, only merged via PR
  └─ dev     ← integration branch, always working
       ├─ feature/D1-pair-a    (Pair A: Sections 1 & 2)
       ├─ feature/D1-pair-b    (Pair B: Sections 3, 4, 5)
       ├─ feature/D2
       └─ ...
```

**Rules:**
- Never commit directly to `main`
- Always branch from `dev`
- Open a PR to merge back into `dev`, then `dev` → `main`
- One PR review from a teammate required

---

## Quick Start

### 1. Clone & Setup

```bash
git clone https://github.com/fishitmo/CS-570.git
cd CS-570/Project-CS-570
pip install -r requirements.txt
```

### 2. Download Data

Download [MovieLens 1M](https://grouplens.org/datasets/movielens/1m/) and extract into:
```
data/raw/ratings.dat
data/raw/users.dat
data/raw/movies.dat
```

### 3. Run Notebooks

Open in VS Code / Jupyter:
```bash
jupyter notebook notebooks/D1_data_loading_eda.ipynb
```

### 4. Launch Dashboard

```bash
streamlit run app/main.py
```

---

## Why Streamlit (not React)?

| Factor | Streamlit | React + FastAPI |
|--------|-----------|-----------------|
| Language | Python (same as Spark) | JS + Python |
| Data viz | Built-in (Plotly, Altair) | Requires D3 / recharts |
| Deployment | Free on Streamlit Cloud | Requires hosting both |
| Resume value | Strong for Data/ML roles | Strong for full-stack |
| Setup time | ~30 mins | ~3–4 hours |

For a Big Data / Analytics portfolio, Streamlit is the industry standard.
You can always add a React frontend later as D5 polish.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Big Data | Apache Spark 3.x (PySpark) |
| Notebook | Jupyter / VS Code Jupyter |
| Dashboard | Streamlit |
| Visualization | Plotly Express, Matplotlib |
| Data | MovieLens 1M |
| CI/CD | GitHub Actions |
| Hosting | Streamlit Community Cloud |







One-liner to share with teammates
After cloning the repo, they just run:


python -m venv .venv && source .venv/Scripts/activate && pip install -r requirements.txt