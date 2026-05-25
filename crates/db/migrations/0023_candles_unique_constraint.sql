DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_candles_exchange_symbol_interval_open_time'
    ) THEN
        ALTER TABLE candles
            ADD CONSTRAINT uq_candles_exchange_symbol_interval_open_time
            UNIQUE (exchange, symbol, interval, open_time);
    END IF;
END $$;
