-- Schema for table equities
CREATE TABLE public.equities (
    isin text NOT NULL,
    symbol text NOT NULL,
    name text,
    market text,
    currency text,
    last_trade_mic_time text,
    time_zone text,
    valid boolean,
    ia_selected boolean DEFAULT false,
    ticker text,
    api text,
    cnt0 integer NOT NULL DEFAULT 0,
    cnt1 integer NOT NULL DEFAULT 0,
    cnt2 integer NOT NULL DEFAULT 0,
    cnt3 integer NOT NULL DEFAULT 0,
    cnt4 integer NOT NULL DEFAULT 0,
    cnt5 integer NOT NULL DEFAULT 0,
    remarques text,
    activ boolean NOT NULL DEFAULT false,
    cnt_total integer NOT NULL DEFAULT 0,
    CONSTRAINT equities_pkey PRIMARY KEY (isin, symbol)
);

CREATE INDEX equities_symbol_idx ON public.equities (symbol);

-- Schema for table equities_prices
CREATE TABLE public.equities_prices (
    isin text NOT NULL,
    symbol text NOT NULL,
    price_date date NOT NULL,
    open_price numeric,
    close_price numeric,
    high_price numeric,
    low_price numeric,
    volume bigint,
    CONSTRAINT equities_prices_pkey PRIMARY KEY (isin, symbol, price_date),
    CONSTRAINT equities_prices_isin_symbol_fkey FOREIGN KEY (isin, symbol)
        REFERENCES public.equities (isin, symbol)
        ON DELETE CASCADE
);

CREATE INDEX equities_prices_isin_date_idx ON public.equities_prices (isin, price_date);
CREATE INDEX equities_prices_symbol_date_idx ON public.equities_prices (symbol, price_date);
