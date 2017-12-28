CREATE TABLE yahoo.dividend
(
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    ex_date date NOT NULL,
    amount numeric NOT NULL,
    CONSTRAINT dividend_pkey PRIMARY KEY (act_symbol, ex_date),
    CONSTRAINT dividend_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE yahoo.stock_split
(
    act_symbol text COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    new_share_amount integer NOT NULL,
    old_share_amount integer NOT NULL,
    CONSTRAINT stock_split_pkey PRIMARY KEY (act_symbol, date),
    CONSTRAINT stock_split_act_symbol_fkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
