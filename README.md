
# Simple ETL Pipeline Using Apache Airflow

## Description
An ETL pipeline built using Apache Airflow, FastAPI, Pandas, and PostgreSQL to automate data extraction, transformation, and loading.

## Overview
This project demonstrates an end-to-end ETL workflow that extracts data from a REST API, processes it using Pandas, and loads it into PostgreSQL using SQLAlchemy and PostgreSQL's bulk `COPY` command.

## Tech Stack
- Python
- Apache Airflow
- FastAPI
- Pandas
- PostgreSQL
- SQLAlchemy

## Workflow
1. **Extract:** Fetch user data from a FastAPI endpoint and store it in a CSV file.
2. **Transform:** Read the raw CSV using Pandas, derive an `age_group` column, and save the transformed data.
3. **Create Tables:** Create the `users` and `employees` tables in PostgreSQL.
4. **Load:** Load the transformed data into `users` using Pandas and SQLAlchemy, and into `employees` using PostgreSQL's `COPY` command.
