-- Initialize future_index database with ES and NQ tables
-- Full OHLC schema matching Sierra Chart SCID tick data

-- ES (E-mini S&P 500) table
CREATE TABLE IF NOT EXISTS "ES" (
    id BIGSERIAL PRIMARY KEY,
    datetime TIMESTAMPTZ NOT NULL,          -- Converted UTC timestamp
    raw_time BIGINT NOT NULL,               -- Sierra Chart timestamp (microseconds since 1899-12-30)
    open DOUBLE PRECISION NOT NULL,         -- Open price (or bundle trade marker)
    high DOUBLE PRECISION NOT NULL,         -- High price
    low DOUBLE PRECISION NOT NULL,          -- Low price
    close DOUBLE PRECISION NOT NULL,        -- Close price
    num_trades INTEGER NOT NULL,            -- Number of trades
    volume INTEGER NOT NULL,                -- Total volume
    bid_volume INTEGER NOT NULL,            -- Bid volume
    ask_volume INTEGER NOT NULL,            -- Ask volume
    contract VARCHAR(50),                   -- Contract symbol (e.g., ESZ24)
    CONSTRAINT es_unique_tick UNIQUE (raw_time)
);

CREATE INDEX IF NOT EXISTS idx_es_datetime ON "ES" (datetime);
CREATE INDEX IF NOT EXISTS idx_es_contract ON "ES" (contract);

-- NQ (E-mini NASDAQ 100) table
CREATE TABLE IF NOT EXISTS "NQ" (
    id BIGSERIAL PRIMARY KEY,
    datetime TIMESTAMPTZ NOT NULL,          -- Converted UTC timestamp
    raw_time BIGINT NOT NULL,               -- Sierra Chart timestamp (microseconds since 1899-12-30)
    open DOUBLE PRECISION NOT NULL,         -- Open price (or bundle trade marker)
    high DOUBLE PRECISION NOT NULL,         -- High price
    low DOUBLE PRECISION NOT NULL,          -- Low price
    close DOUBLE PRECISION NOT NULL,        -- Close price
    num_trades INTEGER NOT NULL,            -- Number of trades
    volume INTEGER NOT NULL,                -- Total volume
    bid_volume INTEGER NOT NULL,            -- Bid volume
    ask_volume INTEGER NOT NULL,            -- Ask volume
    contract VARCHAR(50),                   -- Contract symbol (e.g., NQZ24)
    CONSTRAINT nq_unique_tick UNIQUE (raw_time)
);

CREATE INDEX IF NOT EXISTS idx_nq_datetime ON "NQ" (datetime);
CREATE INDEX IF NOT EXISTS idx_nq_contract ON "NQ" (contract);

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE "ES" TO postgres;
GRANT ALL PRIVILEGES ON TABLE "NQ" TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;
