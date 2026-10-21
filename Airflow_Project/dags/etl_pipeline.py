
from airflow.sdk import dag, task
import requests
import os
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag
def etl_pipeline():
    
    @task
    def timestamp():
        from datetime import datetime
        return datetime.now().isoformat()

    @task
    def extract(ti):
        ts = ti.xcom_pull(task_ids="timestamp")

        url = "http://fastapi:8000/fetch_data"
        response = requests.get(url)
        response.raise_for_status()

        data = response.json().get("data", [])

        os.makedirs("/tmp/raw", exist_ok=True)

        with open(f"/tmp/raw/data_{ts}.csv", "w") as f:
            f.write("id,name,age\n")
            for item in data:
                f.write(f"{item['id']},{item['name']},{item['age']}\n")

        return "Data extracted and written to staging layer."

    @task
    def transform(ti):
        ts = ti.xcom_pull(task_ids="timestamp")
        df = pd.read_csv(f"/tmp/raw/data_{ts}.csv")

        df['age_group'] = df['age'].apply(lambda x: 'Adult' if x >= 18 else 'Minor')

        os.makedirs("/tmp/transformed", exist_ok=True)
        df.to_csv(f"/tmp/transformed/data_transformed_{ts}.csv", index=False)

        return "Data transformed and written to transformed layer."

    @task
    def create_table():
        query = """
            CREATE TABLE IF NOT EXISTS users (
                id INT,
                name VARCHAR(100),
                age INT,
                age_group VARCHAR(10)
            );
        """

        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

        try:
            with engine.begin() as conn:
                conn.execute(text(query))
        finally:
            engine.dispose()

    @task
    def load(ti):
        ts = ti.xcom_pull(task_ids="timestamp", key="return_value")

        df = pd.read_csv(f"/tmp/transformed/data_transformed_{ts}.csv")
        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

        try:
            df.to_sql('users', con=engine, if_exists='append', index=False)
        finally:
            engine.dispose()

    create_new_table = SQLExecuteQueryOperator(
        task_id="create_new_table",
        conn_id="my_postgresql",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                id INT,
                name VARCHAR(100),
                age INT,
                age_group VARCHAR(10)
            );
        """
    )

    @task
    def write_to_postgres(ti):
        ts = ti.xcom_pull(task_ids="timestamp", key="return_value")
        file_path = f"/tmp/transformed/data_transformed_{ts}.csv"

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        hook = PostgresHook(postgres_conn_id="my_postgresql")
        hook.copy_expert(
            sql="""
                COPY employees (id, name, age, age_group)
                FROM STDIN WITH CSV HEADER
            """,
            filename=file_path
        )

    ts = timestamp()
    ex = extract()
    tr = transform()
    ct = create_table()
    ld = load()
    wt = write_to_postgres()

    ts >> ex >> tr >> [ct,create_new_table]
    ct >> ld
    create_new_table >> wt

dag = etl_pipeline()