from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import logging

log = logging.getLogger(__name__)

BASE_SQL_PATH = "./sql"

@dag(
    dag_id="init_schemas_dag",
    start_date=datetime(2022, 5, 5),
    schedule_interval=None,
    catchup=False
)
def init_schemas_dag():
    
    init_stg = PostgresOperator(
        task_id="init_stg",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql=f"{BASE_SQL_PATH}/01_DDL_stg.sql",
    )
    
    init_dds = PostgresOperator(
        task_id="init_dds",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql=f"{BASE_SQL_PATH}/02_DDL_dds.sql",
    )
    
    init_cdm = PostgresOperator(
        task_id="init_cdm",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql=f"{BASE_SQL_PATH}/03_DDL_cdm.sql",
    )
    
    init_stg >> init_dds >> init_cdm


init_schemas_dag = init_schemas_dag()
