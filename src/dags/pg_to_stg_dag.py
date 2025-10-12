import logging

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

log = logging.getLogger(__name__)

src_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
dest_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

def load_pg_ranks_to_stg():
    
    with src_hook.get_conn() as src_conn, dest_hook.get_conn() as dest_conn:
    
        src_cursor = src_conn.cursor()
        src_cursor.execute("SELECT * FROM ranks")
        rows = src_cursor.fetchall()
        
        dest_cursor = dest_conn.cursor()
        
        dest_cursor.executemany(
            """
            INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET
                name = EXCLUDED.name,
                bonus_percent = EXCLUDED.bonus_percent,
                min_payment_threshold = EXCLUDED.min_payment_threshold;
            """,
            rows
        )
        dest_conn.commit()
        
        src_cursor.close()
        dest_cursor.close()

def load_pg_users_to_stg():
    
    with src_hook.get_conn() as src_conn, dest_hook.get_conn() as dest_conn:
        src_cursor = src_conn.cursor()
        src_cursor.execute("SELECT * FROM users")
        rows = src_cursor.fetchall()
        
        dest_cursor = dest_conn.cursor()
        
        dest_cursor.executemany(
            """
            INSERT INTO stg.bonussystem_users (id, order_user_id)
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE
            SET order_user_id = EXCLUDED.order_user_id
            """,
            rows
        )
        dest_conn.commit()
        
        src_cursor.close()
        dest_cursor.close()
    
def load_pg_outbox_to_stg():
    
    WF_KEY = 'load_pg_outbox_to_stg'
    
    with src_hook.get_conn() as src_conn, dest_hook.get_conn() as dest_conn:
        src_cursor = src_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        dest_cursor.execute(f"""
        SELECT
            id,
            workflow_key,
            workflow_settings
        FROM stg.srv_wf_settings
        WHERE workflow_key = '{WF_KEY}';
        """)
        setting = dest_cursor.fetchone()
        
        if not setting:
            max_loaded_id = str(-1)
        else:
            max_loaded_id = json.loads(str(setting[2]).replace("'", '"'))['id']
        
        src_cursor.execute(
            """
                SELECT id, event_ts, event_type, event_value
                FROM outbox
                WHERE id > %(max_loaded_id)s
                ORDER BY id ASC
                LIMIT %(limit)s;
            """, {
                "max_loaded_id": max_loaded_id,
                "limit": 100000
            }
        )
        rows = src_cursor.fetchall()
        
        if rows:
            dest_cursor.executemany(
                """
                INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET
                    event_ts = EXCLUDED.event_ts,
                    event_type = EXCLUDED.event_type,
                    event_value = EXCLUDED.event_value;
                """,
                rows
            )
            
            last_loaded_id = str(rows[-1][0])
            
            dest_cursor.execute("""
                INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%(etl_key)s, %(etl_setting)s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": WF_KEY,
                    "etl_setting": json.dumps({'id': last_loaded_id})
                },
            )
            
            log.info(f"Last loaded id: {last_loaded_id}")
        
            src_conn.commit()
            dest_conn.commit()
        
        src_cursor.close()
        dest_cursor.close()

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'ranks', 'users', 'outbox'],
    is_paused_upon_creation=True
)
def pg_to_stg_dag():

    @task
    def load_ranks():
        load_pg_ranks_to_stg()
        
    @task
    def load_users():
        load_pg_users_to_stg()
    
    @task
    def load_outbox():
        load_pg_outbox_to_stg()
    
    load_ranks()
    load_users()
    load_outbox()


pg_to_stg_dag = pg_to_stg_dag()
