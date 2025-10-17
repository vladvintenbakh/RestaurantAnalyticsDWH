CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id varchar(25) NOT NULL,
	restaurant_name varchar(100) NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 DEFAULT 0 NOT NULL,
	orders_total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_payment_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_granted_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	order_processing_fee numeric(14, 2) DEFAULT 0 NOT NULL,
	restaurant_reward_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT dm_settlement_report_pkey PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date))),
	CONSTRAINT order_processing_fee_check CHECK ((order_processing_fee >= 0)),
	CONSTRAINT orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= 0)),
	CONSTRAINT orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= 0)),
	CONSTRAINT orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT orders_total_sum_check CHECK ((orders_total_sum >= 0)),
	CONSTRAINT restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= 0)),
	CONSTRAINT unique_restaurant_id_settlement_date UNIQUE (restaurant_id, settlement_date)
);

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int2 NOT NULL,
	settlement_month int2 NOT NULL,
	orders_count int4 DEFAULT 0 NOT NULL,
	orders_total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	rate_avg numeric(3, 2) DEFAULT 0 NOT NULL,
	order_processing_fee numeric(14, 2) DEFAULT 0 NOT NULL,
	courier_order_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	courier_tips_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	courier_reward_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT dm_courier_ledger_year_check CHECK ((settlement_year >= 2022) AND (settlement_year < 2500)),
	CONSTRAINT dm_courier_ledger_month_check CHECK ((settlement_month >= 1) AND (settlement_month <= 12)),
	CONSTRAINT dm_courier_ledger_orders_count_check CHECK (orders_count >= 0),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK (orders_total_sum >= 0),
	CONSTRAINT dm_courier_ledger_rate_avg_check CHECK ((rate_avg >= 0) AND (rate_avg <= 5)),
	CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee >= 0)),
	CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= 0)),
	CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum >= 0)),
	CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum >= 0)),
	CONSTRAINT unique_courier_id_settlement_date UNIQUE (courier_id, settlement_year, settlement_month)
);
