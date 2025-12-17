-- Create database for futures tick data
CREATE DATABASE IF NOT EXISTS future_index;

-- ES (E-mini S&P 500) Table
-- Using ReplacingMergeTree for automatic deduplication on restart
CREATE TABLE IF NOT EXISTS future_index.ES
(
    datetime DateTime64(3, 'UTC'),
    raw_time Int64,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    num_trades UInt32,
    volume UInt32,
    bid_volume UInt32,
    ask_volume UInt32,
    contract String
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (datetime, raw_time)
SETTINGS index_granularity = 8192;

-- NQ (E-mini Nasdaq 100) Table
CREATE TABLE IF NOT EXISTS future_index.NQ
(
    datetime DateTime64(3, 'UTC'),
    raw_time Int64,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    num_trades UInt32,
    volume UInt32,
    bid_volume UInt32,
    ask_volume UInt32,
    contract String
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (datetime, raw_time)
SETTINGS index_granularity = 8192;
