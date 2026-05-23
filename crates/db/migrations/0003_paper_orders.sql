ALTER TABLE orders
ADD COLUMN IF NOT EXISTS risk_decision_id UUID,
ADD COLUMN IF NOT EXISTS idempotency_key TEXT,
ADD COLUMN IF NOT EXISTS status_reason TEXT,
ADD COLUMN IF NOT EXISTS filled_price NUMERIC(20, 8),
ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS filled_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rejected_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

UPDATE orders
SET risk_decision_id = '00000000-0000-0000-0000-000000000000'
WHERE risk_decision_id IS NULL;

UPDATE orders
SET idempotency_key = id::TEXT
WHERE idempotency_key IS NULL;

ALTER TABLE orders
ALTER COLUMN risk_decision_id SET NOT NULL,
ALTER COLUMN idempotency_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_idempotency_key
ON orders (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_orders_risk_decision_id
ON orders (risk_decision_id);
