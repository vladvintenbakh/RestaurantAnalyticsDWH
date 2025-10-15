import logging
import requests
from datetime import datetime, timedelta
import json

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0 0 * * *', 
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'mongodb'],
    is_paused_upon_creation=True
)
def delivery_api_to_stg_dag():
    headers = {
        "X-Nickname": Variable.get('DELIVERY_API_NICKNAME'),
        "X-Cohort": Variable.get('DELIVERY_API_COHORT'),
        "X-API-KEY": Variable.get('DELIVERY_API_KEY')
    }
    
    api_endpoint = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
    
    dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    @task()
    def load_couriers():
        
        params = {
            "sort_field": "id",
            "sort_direction": "asc",
            "limit": 50,
            "offset": 0
        }
        
        all_records = []

        while True:
            response = requests.get(api_endpoint + "/couriers", headers=headers, params=params)
            
            response_json = response.json()
            if len(response_json) == 0:
                break
            
            all_records += response_json
            params["offset"] += len(response_json)

        log.info(f"{len(all_records)} records loaded")
        
        if len(all_records) > 0:
            with dwh_hook.get_conn() as dwh_conn:
        
                dwh_cursor = dwh_conn.cursor()
                
                for record in all_records:
                    
                    dwh_cursor.execute(
                        f"""
                        INSERT INTO stg.deliverysystem_couriers
                        (id, courier_name)
                        VALUES ('{record['_id']}', '{record['name']}')
                        ON CONFLICT (id) DO UPDATE
                        SET
                            courier_name = EXCLUDED.courier_name
                        """
                    )
                
                dwh_conn.commit()
                dwh_cursor.close()
                
    @task()
    def load_deliveries():
        params = {
            "sort_field": "date",
            "sort_direction": "asc",
            "limit": 50,
            "offset": 0,
            "from": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d") + ' 00:00:00',
            "to": datetime.now().strftime("%Y-%m-%d") + ' 00:00:00'
        }
        
        with dwh_hook.get_conn() as dwh_conn:
            
            dwh_cursor = dwh_conn.cursor()
            num_loaded = 0
        
            while True:
                response = requests.get(api_endpoint + "/deliveries", headers=headers, params=params)
                response.encoding = 'utf-8'
                
                response_json = response.json()
                if len(response_json) == 0:
                    break
                
                for delivery in response_json:
                    dwh_cursor.execute(
                        f"""
                        INSERT INTO stg.deliverysystem_deliveries
                        (object_id, object_value, update_ts)
                        VALUES ('{delivery['delivery_id']}', '{json.dumps(delivery, ensure_ascii=False)}', '{delivery['delivery_ts']}')
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts
                        """
                    )
                    
                num_loaded += len(response_json)
                params["offset"] += len(response_json)
                
                log.info(f"Records loaded so far: {num_loaded}")
        
            dwh_conn.commit()
            dwh_cursor.close()
            
        log.info(f"A total of {num_loaded} records loaded")
    
    load_couriers()
    load_deliveries()


delivery_api_to_stg_dag = delivery_api_to_stg_dag()
