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
	CONSTRAINT order_processing_fee CHECK ((orders_count >= 0)),
	CONSTRAINT orders_bonus_granted_sum CHECK ((orders_count >= 0)),
	CONSTRAINT orders_bonus_payment_sum CHECK ((orders_count >= 0)),
	CONSTRAINT orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT orders_total_sum CHECK ((orders_count >= 0)),
	CONSTRAINT restaurant_reward_sum CHECK ((orders_count >= 0)),
	CONSTRAINT unique_restaurant_id_settlement_date UNIQUE (restaurant_id, settlement_date)
);
