# Postgres DWH with Airflow

A compact data-warehouse project that ingests from multiple sources into **stg → dds → cdm** layers using **Apache Airflow** and **PostgreSQL**. A ready-to-run Docker Compose spins up the stack plus **Metabase** for BI.

## Stack
- **Airflow** (ETL orchestration, DAGs in `src/dags`)
- **PostgreSQL** (warehouse; exposed on host)
- **Metabase** (analytics, optional)
- **Docker Compose** (local dev)

## Repo Structure
```
src/
  dags/
    init_schemas_dag.py        # creates stg, dds, cdm schemas
    pg_to_stg_dag.py           # loads from Postgres (bonus system) → stg
    mongodb_to_stg_dag.py      # loads from MongoDB → stg
    delivery_api_to_stg_dag.py # loads from external delivery API → stg
    main_dag.py                # builds dds dims/facts + cdm marts
    helpers/                   # connectors, readers/loaders, utils
    sql/                       # 01_DDL_* and 04–13_DML_* scripts
docker-compose.yml
.env
```

## Quick Start
> Requires Docker & Docker Compose.

```bash
git clone <your-fork-url>
cd de-project-sprint-5-main
docker compose up -d
```

**Default ports**
- Postgres: `localhost:15432`
- Airflow: `localhost:3000`  
- Metabase: `localhost:3333`

## Airflow Setup

Create the following **Connections** in Airflow:

- `PG_WAREHOUSE_CONNECTION` — Postgres to the local warehouse
- `PG_ORIGIN_BONUS_SYSTEM_CONNECTION` — source Postgres (bonus system)

Set the following **Variables** (as applicable):

- `MONGO_DB_CERTIFICATE_PATH`, `MONGO_DB_USER`, `MONGO_DB_PASSWORD`,  
  `MONGO_DB_REPLICA_SET`, `MONGO_DB_DATABASE_NAME`, `MONGO_DB_HOST`
- `DELIVERY_API_NICKNAME`, `DELIVERY_API_COHORT`, `DELIVERY_API_KEY`

## Run the Pipelines

1. **Initialize schemas**  
   Trigger **`init_schemas_dag`** (one-time). This runs:
   - `01_DDL_stg.sql`, `02_DDL_dds.sql`, `03_DDL_cdm.sql`

2. **Load staging** (unpause/trigger as needed)
   - **`pg_to_stg_dag`** — loads `ranks`, `users`, etc. → `stg.bonussystem_*`
   - **`mongodb_to_stg_dag`** — loads `restaurants`, `users`, `orders` from Mongo
   - **`delivery_api_to_stg_dag`** — daily delivery data from external API

3. **Build warehouse + marts**  
   Trigger **`main_dag`** to populate:
   - **DDS dims**: `dm_users`, `dm_restaurants`, `dm_timestamps`, `dm_couriers`, `dm_products`, `dm_orders`
   - **DDS facts**: `fct_product_sales`, `fct_deliveries`
   - **CDM marts**: `dm_settlement_report`, `dm_courier_ledger`
