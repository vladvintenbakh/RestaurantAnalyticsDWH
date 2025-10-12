import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from helpers.pg_saver import PgSaver

from helpers.restaurant_loader import RestaurantLoader
from helpers.restaurant_reader import RestaurantReader
from helpers.users_loader import UsersLoader
from helpers.users_reader import UsersReader
from helpers.orders_loader import OrdersLoader
from helpers.orders_reader import OrdersReader

from helpers.pg_connect import ConnectionBuilder
from helpers.mongo_connect import MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *', 
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'mongodb'],
    is_paused_upon_creation=True
)
def mongodb_to_stg_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = RestaurantReader(mongo_connect)
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()
    
    @task()
    def load_users():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = UsersReader(mongo_connect)
        loader = UsersLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()
    
    @task()
    def load_orders():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = OrdersReader(mongo_connect)
        loader = OrdersLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()
        
        
    load_restaurants()
    load_users()
    load_orders()


mongodb_to_stg_dag = mongodb_to_stg_dag()
