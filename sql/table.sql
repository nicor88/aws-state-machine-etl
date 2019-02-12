CREATE TABLE public.exchange_rates (
	report_date DATE,
	base_currency VARCHAR(3),
	currency VARCHAR(3),
	rate REAL,
	exported_at TIMESTAMP DEFAULT sysdate
);

