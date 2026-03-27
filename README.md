# ST554 Analysis of Big Data Project 2
### Cole Hammett
### 27th of March, 2026

# Project 2: PySpark Data Quality & NFL Analysis

This is a mess. I tried to connect the JupyterLab window to the repo and I had all of my commits leading up to where I was and then is fell out of sync or something and they didn't show up in the repo like the JupyterLab window was showing it was... The instructions said to download the files and commit and that was more literal than I thought it would be...

## Overview

This project demonstrates the use of Apache Spark (PySpark) for data quality checking 

1. **Part I** — Design and implementation of a custom `SparkDataCheck` class for validating and summarizing Spark SQL DataFrames.
2. **Part II** — Analysis of NFL quarterback statistics using both the Spark SQL DataFrame API and the pandas-on-Spark API.

---

## Repository Structure

```
project2/
├── spark_data_check.py       # SparkDataCheck class definition (Part I)
├── project2_notebook.ipynb   # Main analysis notebook (Parts I & II)
├── weekly_nfl_data.csv       # NFL weekly player data (uploaded to JupyterHub)
└── README.md
```

---

## Data Sources

| Dataset | Description | Source |
|---|---|---|
| Air Quality (`air.csv`) | Air quality measurements used to demo the `SparkDataCheck` class | https://www4.stat.ncsu.edu/online/datasets/air.csv |
| NFL Weekly Data (`weekly_nfl_data.csv`) | Weekly NFL player statistics (2005–2023) | Provided on course project page |

