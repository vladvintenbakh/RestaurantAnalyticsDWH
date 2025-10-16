CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

CREATE TABLE IF NOT EXISTS dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pkey PRIMARY KEY (id),
	CONSTRAINT dm_users_unique_user_id UNIQUE (user_id)
);

CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id),
	CONSTRAINT dm_couriers_unique_courier_id UNIQUE (courier_id)
);

CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dds_dm_timestamp_unique_ts UNIQUE (ts),
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

CREATE TABLE IF NOT EXISTS dds.dm_products (
	id serial4 NOT NULL,
	product_id varchar NOT NULL,
	restaurant_id int4 NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) DEFAULT 0 NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_pkey PRIMARY KEY (id),
	CONSTRAINT dm_products_product_price_check CHECK (((product_price >= (0)::numeric) AND (product_price <= 999000000000.99))),
	CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);

CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_order_key_key UNIQUE (order_key),
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT dm_orders_fk_restaurant_id FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
	CONSTRAINT dm_orders_fk_timestamp_id FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
	CONSTRAINT dm_orders_fk_user_id FOREIGN KEY (user_id) REFERENCES dds.dm_users(id),
	CONSTRAINT dm_orders_fk_courier_id FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id)
);

CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 DEFAULT 0 NOT NULL,
	price numeric(14, 2) DEFAULT 0 NOT NULL,
	total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	bonus_payment numeric(14, 2) DEFAULT 0 NOT NULL,
	bonus_grant numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pkey PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric)),
	CONSTRAINT unique_product_order_pair UNIQUE (product_id, order_id),
	CONSTRAINT fct_product_sales_fk_order_id FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
	CONSTRAINT fct_product_sales_fk_product_id FOREIGN KEY (product_id) REFERENCES dds.dm_products(id)
);
