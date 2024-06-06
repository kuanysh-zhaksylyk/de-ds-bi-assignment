import logging
import os
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Constants
DATA_DIR = Path("/opt/airflow/dataset/")
CSV_FILES = [
    DATA_DIR / "Measurement_info.csv",
    DATA_DIR / "Measurement_item_info.csv",
    DATA_DIR / "Measurement_station_info.csv",
    DATA_DIR / "Measurement_summary.csv"
]

DB_TABLES = [
    "measurement_info",
    "measurement_item_info",
    "measurement_station_info",
    "measurement_summary"
]

args = {
    'owner': 'Kuanysh',
    'email_on_failure': True,
    'email': ['k.zhaksylyk@sem.systems']
}

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

def migrate_data(path: str, db_table: str) -> None:
    """
    Migrates data from CSV file to PostgreSQL database table.

    :param path: Path to the CSV file.
    :type path: str
    :param db_table: Name of the database table to migrate the data to.
    :type db_table: str
    """
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    engine = create_engine(db_url, echo=True, future=True)
    logging.info(f"Start migrating data to {db_table}")
    
    chunksize = 10000

    for chunk in pd.read_csv(path, sep=",", quotechar='"', quoting=2, chunksize=chunksize, index_col=False):
        chunk.columns = [col.strip().replace(' ', '_').replace('(', '_').replace(')', '').replace('.', '_').lower() for col in chunk.columns]
        chunk.to_sql(db_table, con=engine, if_exists='append', index=False)
    
    logging.info(f"Completed migration to {db_table}")

dag = DAG(
    dag_id="migrate_data",
    default_args=args,
    schedule_interval=None,
    catchup=False,
)

migrate_tasks = []
for csv_file, db_table in zip(CSV_FILES, DB_TABLES):
    migrate_task = PythonOperator(
        task_id=f'migrate_{db_table}',
        dag=dag,
        python_callable=migrate_data,
        op_kwargs={"path": csv_file, "db_table": db_table}
    )
    migrate_tasks.append(migrate_task)

migrate_tasks[0] >> migrate_tasks[1] >> migrate_tasks[2] >> migrate_tasks[3]
