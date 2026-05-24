CREATE TABLE IF NOT EXISTS paper_accounts (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    base_currency TEXT NOT NULL,
    initial_equity NUMERIC(20, 8) NOT NULL,
    current_equity NUMERIC(20, 8) NOT NULL,
    realized_pnl NUMERIC(20, 8) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(20, 8) NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_positions (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES paper_accounts(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity NUMERIC(20, 8) NOT NULL,
    entry_price NUMERIC(20, 8) NOT NULL,
    mark_price NUMERIC(20, 8),
    price_status TEXT NOT NULL DEFAULT 'missing',
    notional NUMERIC(20, 8) NOT NULL,
    realized_pnl NUMERIC(20, 8) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(20, 8) NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    strategy_id TEXT,
    signal_id UUID,
    risk_decision_id UUID,
    order_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_positions_open_unique
ON paper_positions (account_id, symbol, side)
WHERE status = 'open';

CREATE INDEX IF NOT EXISTS idx_paper_positions_account_status
ON paper_positions (account_id, status, opened_at DESC);

CREATE TABLE IF NOT EXISTS paper_fills (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES paper_accounts(id) ON DELETE CASCADE,
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    position_id UUID REFERENCES paper_positions(id) ON DELETE SET NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    quantity NUMERIC(20, 8) NOT NULL,
    notional NUMERIC(20, 8) NOT NULL,
    fee NUMERIC(20, 8) NOT NULL DEFAULT 0,
    slippage_cost NUMERIC(20, 8) NOT NULL DEFAULT 0,
    filled_at TIMESTAMPTZ NOT NULL,
    strategy_id TEXT,
    signal_id UUID,
    risk_decision_id UUID,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_fills_account_filled_at
ON paper_fills (account_id, filled_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_fills_order_id
ON paper_fills (order_id);

CREATE TABLE IF NOT EXISTS paper_equity_snapshots (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES paper_accounts(id) ON DELETE CASCADE,
    equity NUMERIC(20, 8) NOT NULL,
    realized_pnl NUMERIC(20, 8) NOT NULL,
    unrealized_pnl NUMERIC(20, 8) NOT NULL,
    drawdown_pct NUMERIC(20, 8) NOT NULL,
    snapshot_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_equity_snapshots_account_snapshot_at
ON paper_equity_snapshots (account_id, snapshot_at DESC);

CREATE TABLE IF NOT EXISTS paper_trade_journal (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES paper_accounts(id) ON DELETE CASCADE,
    position_id UUID REFERENCES paper_positions(id) ON DELETE SET NULL,
    order_id UUID REFERENCES orders(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    symbol TEXT,
    pnl NUMERIC(20, 8),
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_trade_journal_account_created_at
ON paper_trade_journal (account_id, created_at DESC);
