export type ApiError = {
  error: string;
  message: string;
  request_id?: string;
  correlation_id?: string;
  timestamp?: string;
};

export type AuthUser = {
  id: string;
  email: string;
  role: "OWNER" | "OPERATOR" | "VIEWER";
  status: "ACTIVE" | "DISABLED";
  created_at: string;
  updated_at: string;
  last_login_at: string | null;
};

export type AuthLoginRequest = {
  email: string;
  password: string;
};

export type AuthLoginResponse = {
  user: AuthUser;
  access_token: string;
  expires_at: string;
};

export type AuthUserResponse = {
  user: AuthUser;
};

export type AuthLogoutResponse = {
  logged_out: boolean;
};

export type HealthResponse = {
  status: string;
  service: string;
  environment: string;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StatusResponse = {
  service: string;
  environment: string;
  market_mode: string;
  started_at: string;
  request_id: string;
  correlation_id: string;
  timestamp: string;
  dependencies: {
    database: { status: string };
    event_bus: { status: string };
    exchange_execution: { status: string };
  };
};

export type SystemEventRecord = {
  event_id: string;
  event_type: string;
  source: string;
  correlation_id: string;
  payload: Record<string, unknown> | null;
  occurred_at: string;
  created_at: string;
};

export type SystemStateSnapshot = {
  enabled: boolean;
  reason: string | null;
  updated_at: string;
  updated_by: {
    actor: string;
    actor_id: string | null;
  };
  last_correlation_id: string;
};

export type RiskStatusResponse = {
  status: string;
  market_mode: string;
  paper_trading_allowed: boolean;
  live_trading_allowed: boolean;
  resume_confirmation_required: string;
  kill_switch: SystemStateSnapshot;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type RiskActionResponse = {
  status: string;
  message: string;
  market_mode: string;
  paper_trading_allowed: boolean;
  live_trading_allowed: boolean;
  kill_switch: SystemStateSnapshot;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type RiskConfig = {
  max_open_positions: number;
  max_daily_loss_pct: string;
  max_weekly_loss_pct: string;
  max_position_notional: string;
  max_slippage_pct: string;
  max_consecutive_losses: number;
  cooldown_seconds: number;
  max_signal_age_ms: number;
  stale_feed_threshold_seconds: number;
};

export type RiskConfigView = RiskConfig & {
  config_id: string;
  config_version: number;
  created_at: string;
  updated_at: string;
};

export type RiskConfigResponse = {
  config: RiskConfigView;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type RiskConfigValidationIssue = {
  severity: "ERROR" | "WARN";
  code: string;
  field: string;
  message: string;
};

export type RiskConfigValidationResult = {
  valid: boolean;
  issues: RiskConfigValidationIssue[];
  normalized_config: RiskConfig | null;
  validated_at: string;
};

export type RiskConfigValidationResponse = {
  validation: RiskConfigValidationResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type RiskConfigVersion = {
  config_id: string;
  version: number;
  config: RiskConfig;
  actor_id: string | null;
  correlation_id: string;
  created_at: string;
};

export type RiskConfigVersionsResponse = {
  versions: RiskConfigVersion[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type RiskConfigAuditEntry = {
  audit_id: string;
  config_id: string;
  version: number | null;
  old_config: RiskConfig | null;
  new_config: RiskConfig | null;
  validation_issues: RiskConfigValidationIssue[];
  actor_id: string | null;
  correlation_id: string;
  created_at: string;
};

export type RiskConfigAuditResponse = {
  audit: RiskConfigAuditEntry[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type RiskDecisionRecord = {
  id: string;
  signal_id: string | null;
  decision: string;
  approved_notional: string | null;
  risk_score: string | null;
  reasons: string[];
  created_at: string;
  correlation_id: string;
  strategy_id: string | null;
  symbol: string | null;
};

export type RiskDecisionsResponse = {
  decisions: RiskDecisionRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type RiskDecisionResponse = {
  decision: RiskDecisionRecord;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type MarketSymbolsResponse = {
  exchange: string;
  symbols: string[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type MarketTickRecord = {
  id: string;
  exchange: string;
  symbol: string;
  price: string;
  quantity: string;
  trade_time: string;
  received_at: string;
  raw_payload?: unknown;
};

export type MarketTickResponse = {
  tick: MarketTickRecord;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type CandleRecord = {
  id: string;
  exchange: string;
  symbol: string;
  interval: string;
  open_time: string;
  close_time: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  quote_volume: string | null;
  trade_count: number;
  is_closed: boolean;
  created_at: string;
  updated_at: string;
};

export type CandlesResponse = {
  candles: CandleRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type CandleBackfillRequest = {
  exchange?: string;
  symbol: string;
  interval: string;
  start_time: string;
  end_time: string;
  limit_per_request?: number;
  correlation_id?: string;
};

export type CandleBackfillResult = {
  run_id: string;
  exchange: string;
  symbol: string;
  interval: string;
  start_time: string;
  end_time: string;
  status: string;
  requested_candles_estimate: number;
  fetched_candles: number;
  inserted_candles: number;
  updated_candles: number;
  skipped_candles: number;
  failed_reason: string | null;
  correlation_id: string;
  created_at: string;
  completed_at: string | null;
};

export type CandleBackfillRunsResponse = {
  runs: CandleBackfillResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type CandleBackfillRunResponse = {
  run: CandleBackfillResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type MarketFeedStatusRecord = {
  exchange: string;
  symbol: string;
  status: string;
  freshness_status: string;
  last_event_at: string | null;
  last_error: string | null;
  reconnect_count: number;
  updated_at: string;
};

export type FeedStatusResponse = {
  feeds: MarketFeedStatusRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyStatusView = {
  strategy_id: string;
  enabled: boolean;
  mode: string;
  symbols: string[];
  timeframe: string;
  suggested_notional: string;
  max_signal_age_ms: number;
  cooldown_seconds: number;
  lookback_candles: number;
  confidence_floor: string | null;
  stop_loss_pct: string | null;
  take_profit_pct: string | null;
  holding_candles: number | null;
  notes: string | null;
  config_version: number;
  last_evaluated_at: string | null;
  last_evaluation_reason: string | null;
  last_signal_id: string | null;
  last_signal_at: string | null;
};

export type StrategyListResponse = {
  strategies: StrategyStatusView[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyStatusResponse = {
  strategy: StrategyStatusView;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyToggleResponse = StrategyStatusResponse;

export type StrategyConfigUpdateRequest = {
  strategy_id: string;
  enabled: boolean;
  mode: string;
  symbols: string[];
  timeframe: string;
  suggested_notional: string;
  max_signal_age_ms: number;
  cooldown_seconds: number;
  lookback_candles: number;
  confidence_floor?: string | null;
  stop_loss_pct?: string | null;
  take_profit_pct?: string | null;
  holding_candles?: number | null;
  notes?: string | null;
};

export type StrategyConfigValidationIssue = {
  severity: "ERROR" | "WARN";
  code: string;
  field: string;
  message: string;
};

export type StrategyConfigValidationResult = {
  strategy_id: string;
  valid: boolean;
  issues: StrategyConfigValidationIssue[];
  normalized_config: StrategyConfigUpdateRequest | null;
  validated_at: string;
};

export type StrategyConfigValidationResponse = {
  validation: StrategyConfigValidationResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyConfigVersion = {
  strategy_id: string;
  version: number;
  config: StrategyConfigUpdateRequest;
  actor_id: string | null;
  correlation_id: string;
  created_at: string;
};

export type StrategyConfigVersionsResponse = {
  versions: StrategyConfigVersion[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyConfigAuditEntry = {
  audit_id: string;
  strategy_id: string;
  version: number | null;
  old_config: StrategyConfigUpdateRequest | null;
  new_config: StrategyConfigUpdateRequest | null;
  validation_issues: StrategyConfigValidationIssue[];
  actor_id: string | null;
  correlation_id: string;
  created_at: string;
};

export type StrategyConfigAuditResponse = {
  audit: StrategyConfigAuditEntry[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyDryRunResult = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  config_valid: boolean;
  validation_issues: StrategyConfigValidationIssue[];
  would_generate_signal: boolean;
  reason: string;
  source_candle_open_time: string | null;
  confidence: string | null;
  correlation_id: string;
  evaluated_at: string;
};

export type StrategyDryRunResponse = {
  result: StrategyDryRunResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type EvaluateStrategyResponse = {
  strategy_id: string;
  symbol: string;
  generated: boolean;
  signal_id: string | null;
  side: string | null;
  confidence: string | null;
  reason: string;
  source_candle_open_time: string | null;
  correlation_id: string;
};

export type SignalRecord = {
  id: string;
  strategy_id: string;
  symbol: string;
  side: string;
  confidence: string;
  timeframe: string;
  reason: string;
  suggested_notional: string;
  stop_loss_pct: string | null;
  take_profit_pct: string | null;
  source_candle_open_time: string;
  correlation_id: string;
  created_at: string;
};

export type RecentSignalsResponse = {
  signals: SignalRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type PaperPipelineRequest = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  correlation_id?: string;
};

export type PaperPipelineResult = {
  pipeline_decision: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  signal_generated: boolean;
  signal_reused: boolean;
  signal_id: string | null;
  risk_decision_id: string | null;
  paper_order_id: string | null;
  execution_state: string | null;
  reasons: string[];
  correlation_id: string;
  trace: {
    strategy_evaluation: string;
    signal: string;
    risk_evaluation: string;
    paper_order: string;
    order_intent_source: string | null;
  };
};

export type OrderRecord = {
  order_id: string;
  client_order_id: string;
  exchange_order_id: string | null;
  signal_id: string | null;
  correlation_id: string;
  risk_decision_id: string;
  strategy_id: string | null;
  idempotency_key: string;
  requested_notional: string | null;
  symbol: string;
  side: string;
  quantity: string;
  filled_qty: string;
  limit_price: string | null;
  mode: string;
  market_mode: string;
  status: string;
  execution_state: string;
  status_reason: string | null;
  filled_price: string | null;
  avg_fill_price: string | null;
  submitted_at: string | null;
  filled_at: string | null;
  cancelled_at: string | null;
  rejected_at: string | null;
  expired_at: string | null;
  expires_at: string | null;
  created_at: string;
  updated_at: string;
};

export type OrdersResponse = {
  orders: OrderRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type OrderResponse = {
  order: OrderRecord;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type PaperAccountRecord = {
  id: string;
  name: string;
  base_currency: string;
  initial_equity: string;
  current_equity: string;
  realized_pnl: string;
  unrealized_pnl: string;
  status: string;
  created_at: string;
  updated_at: string;
};

export type PaperAccountResponse = {
  account: PaperAccountRecord;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type PaperPositionRecord = {
  id: string;
  account_id: string;
  symbol: string;
  side: string;
  quantity: string;
  entry_price: string;
  mark_price: string | null;
  price_status: string;
  notional: string;
  realized_pnl: string;
  unrealized_pnl: string;
  status: string;
  opened_at: string;
  closed_at: string | null;
  strategy_id: string | null;
  signal_id: string | null;
  risk_decision_id: string | null;
  order_id: string | null;
  updated_at: string;
};

export type PaperPositionsResponse = {
  positions: PaperPositionRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type PaperClosePositionRequest = {
  confirmation_text: string;
  reason?: string | null;
  close_mode?: "MARKET_SIMULATED";
  correlation_id?: string;
};

export type PaperClosePositionResponse = {
  status: string;
  position_id: string;
  symbol: string;
  entry_price: string;
  exit_price: string;
  quantity: string;
  realized_pnl: string;
  fee: string;
  slippage_cost: string;
  close_fill_id: string;
  journal_entry_id: string;
  correlation_id: string;
  closed_at: string;
  request_id: string;
  timestamp: string;
};

export type PaperPnlSummaryRecord = {
  realized_pnl: string;
  unrealized_pnl: string;
  equity: string;
  daily_pnl: string;
  drawdown_pct: string;
  price_status: string;
  open_positions_count: number;
  calculated_at: string;
};

export type PaperPnlResponse = {
  pnl: PaperPnlSummaryRecord;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type PaperEquitySnapshotRecord = {
  id: string;
  account_id: string;
  equity: string;
  realized_pnl: string;
  unrealized_pnl: string;
  drawdown_pct: string;
  snapshot_at: string;
};

export type PaperEquityResponse = {
  equity: PaperEquitySnapshotRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type PaperTradeJournalRecord = {
  id: string;
  account_id: string;
  position_id: string | null;
  order_id: string | null;
  event_type: string;
  symbol: string | null;
  pnl: string | null;
  payload: Record<string, unknown> | null;
  created_at: string;
  correlation_id: string;
};

export type PaperTradeJournalResponse = {
  journal: PaperTradeJournalRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type BacktestRunAcceptedResponse = {
  run_id: string;
  status: string;
  strategy_id: string;
  symbol: string;
  trade_count: number;
  pnl: string;
  pnl_pct: string;
  max_drawdown_pct: string;
  win_rate: string;
  fee_paid: string;
  slippage_cost: string;
  correlation_id: string | null;
};

export type BacktestResult = {
  run_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  initial_capital: string;
  final_equity: string;
  pnl: string;
  pnl_pct: string;
  max_drawdown_pct: string;
  win_rate: string;
  trade_count: number;
  winning_trades: number;
  losing_trades: number;
  avg_win: string;
  avg_loss: string;
  fee_paid: string;
  slippage_cost: string;
  status: string;
  created_at: string;
  correlation_id: string | null;
};

export type BacktestRunsResponse = {
  runs: BacktestResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type BacktestRunResponse = {
  run: BacktestResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type BacktestTradeRecord = {
  id: string;
  run_id: string;
  strategy_id: string;
  symbol: string;
  side: string;
  entry_time: string;
  entry_price: string;
  exit_time: string | null;
  exit_price: string | null;
  quantity: string;
  notional: string;
  fee_paid: string;
  slippage_cost: string;
  realized_pnl: string;
  reason: string;
  created_at: string;
};

export type BacktestTradesResponse = {
  trades: BacktestTradeRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type BacktestEquityPointRecord = {
  id: string;
  run_id: string;
  timestamp: string;
  equity: string;
  drawdown_pct: string;
};

export type BacktestEquityCurveResponse = {
  equity: BacktestEquityPointRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type BacktestRequest = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  holding_candles?: number;
  risk_config_id?: string | null;
  risk_config?: Record<string, unknown> | null;
  correlation_id?: string;
};
