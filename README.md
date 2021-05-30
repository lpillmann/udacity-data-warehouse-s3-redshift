# Data Warehouse with Redshift and S3

Udacity Data Engineering Nano Degree - Project #3: ETL using Redshift and S3

## Overview
The project consists of loading data from S3 to Redshift and modeling it in a star schema. Data is first loaded into staging tables and then transformed into a fact table for **songplays** and several dimensions (**songs**, **artists**, **users** and **time**). 

What is in this repository:
```
.
├── create_tables.py        -> Drop/create all tables used in the project
├── etl.py                  -> ETL steps to load staging tables and transform them into star schema
├── poetry.lock             -> Poetry frozen set of dependencies
├── pyproject.toml          -> Poetry project and dependencies description
├── quality_checks.py       -> Executes quality checks on top of star schema tables
├── README.md               -> Intro to project and how to use (this file you are reading)
├── scripts                 -> Infrastructure-as-code scripts
│   ├── create_cluster.py   -> Create Redshift cluster
│   └── delete_cluster.py   -> Delete Redshift cluster
└── sql_queries.py          -> Declaration of CREATE, DROP and INSERT query templates
```

## Getting Started
This repository contains the code for the third project in Udacity's Data Engineering Nanodegree program.
### Prerequisites

- [Poetry](https://python-poetry.org/docs/)

### Installing

Install Python dependencies using Poetry:

```bash
poetry install
```

## Running the ETL steps

Activate virtualenv

```bash
poetry shell
```

Create Redshift cluster

```bash
python scripts/create_cluster.py
```

Create tables

```bash
python create_tables.py
```

Run ETL

```bash
python etl.py
```

Run data quality checks

```bash
python quality_checks.py
```

Delete Redshift cluster

```bash
python scripts/delete_cluster.py
```
### Apply code style

This project uses [Black](https://github.com/psf/black) as formatter:

```bash
black .
```

## Built With

  - [AWS Redshift](https://aws.amazon.com/redshift/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) - Used as Data Warehouse
  - [AWS S3](https://aws.amazon.com/s3/) - Storage of data sources
  - [Psycopg](https://www.psycopg.org) - Used to interact with Redshift
  - [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) - Used to interact with AWS

## Acknowledgments

  - Based on Udacity's code template and project guide
