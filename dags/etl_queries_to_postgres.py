from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


args = {
    'owner': 'Kuanysh',
    'email_on_failure': True,
    'email': ['k.zhaksylyk@sem.systems']
}
with DAG(
    dag_id='etl_queries',
    default_args=args,
    schedule_interval=None,
) as dag:
    start = DummyOperator(task_id='start', dag=dag)
    task = PostgresOperator(
        task_id='insert_data_query',
        postgres_conn_id='postgres',
        sql=(['query1.sql', 'query2.sql', 'query3.sql']),
        autocommit=True
    )
    end = DummyOperator(task_id='end', dag=dag)
    
start >> task >> end
