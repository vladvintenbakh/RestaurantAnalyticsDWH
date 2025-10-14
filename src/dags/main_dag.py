from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

BASE_SQL_PATH = "./sql"

@dag(
    dag_id="main_dag",
    start_date=datetime(2022, 5, 5),
    schedule_interval=None,
    catchup=False
)
def main_dag():

    with TaskGroup("load_upstream_dims") as load_upstream_dims:
        
        insert_into_dds_dm_users = PostgresOperator(
            task_id="insert_into_dds_dm_users",
            postgres_conn_id="PG_WAREHOUSE_CONNECTION",
            sql=f"{BASE_SQL_PATH}/04_DML_dds_dm_users.sql",
        )
    
        insert_into_dds_dm_restaurants = PostgresOperator(
            task_id="insert_into_dds_dm_restaurants",
            postgres_conn_id="PG_WAREHOUSE_CONNECTION",
            sql=f"{BASE_SQL_PATH}/05_DML_dds_dm_restaurants.sql",
        )
        
        insert_into_dds_dm_timestamps = PostgresOperator(
            task_id="insert_into_dds_dm_timestamps",
            postgres_conn_id="PG_WAREHOUSE_CONNECTION",
            sql=f"{BASE_SQL_PATH}/06_DML_dds_dm_timestamps.sql",
        )

    with TaskGroup("load_downstream_dims") as load_downstream_dims:
        
        insert_into_dds_dm_products = PostgresOperator(
            task_id="insert_into_dds_dm_products",
            postgres_conn_id="PG_WAREHOUSE_CONNECTION",
            sql=f"{BASE_SQL_PATH}/07_DML_dds_dm_products.sql",
        )
        
        insert_into_dds_dm_orders = PostgresOperator(
            task_id="insert_into_dds_dm_orders",
            postgres_conn_id="PG_WAREHOUSE_CONNECTION",
            sql=f"{BASE_SQL_PATH}/08_DML_dds_dm_orders.sql",
        )
    
    insert_into_fct_product_sales = PostgresOperator(
        task_id="insert_into_fct_product_sales",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql=f"{BASE_SQL_PATH}/09_DML_dds_fct_product_sales.sql",
    )
    
    insert_into_cdm_dm_settlement_report = PostgresOperator(
        task_id="insert_into_cdm_dm_settlement_report",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql=f"{BASE_SQL_PATH}/10_DML_cdm_dm_settlement_report.sql",
    )
    
    load_upstream_dims >> load_downstream_dims >> insert_into_fct_product_sales >> insert_into_cdm_dm_settlement_report

main_dag = main_dag()
