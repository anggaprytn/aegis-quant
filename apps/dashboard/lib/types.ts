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

export type AuthRefreshResponse = {
  user: AuthUser;
  access_token: string;
  expires_at: string;
  refresh_token?: string;
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

export type ExchangeTestnetStatusResponse = {
  exchange: string;
  environment: string;
  rest_base_url: string;
  ws_base_url: string;
  configured: boolean;
  request_mode: string;
  rate_limits: Record<string, unknown>;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeTestnetSymbolInfo = {
  exchange: string;
  environment: string;
  symbol: string;
  base_asset: string;
  quote_asset: string;
  status: string;
  min_price: string | null;
  tick_size: string | null;
  min_qty: string | null;
  step_size: string | null;
  min_notional: string | null;
};

export type ExchangeTestnetSymbolsResponse = {
  symbols: ExchangeTestnetSymbolInfo[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeTestnetBalanceRecord = {
  exchange: string;
  environment: string;
  asset: string;
  free: string;
  locked: string;
};

export type ExchangeTestnetBalancesResponse = {
  balances: ExchangeTestnetBalanceRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeTestnetOrderRecord = {
  id: string;
  exchange: string;
  environment: string;
  client_order_id: string;
  exchange_order_id: string | null;
  symbol: string;
  side: string;
  order_type: string;
  time_in_force: string | null;
  requested_qty: string | null;
  requested_notional: string | null;
  limit_price: string | null;
  status: string;
  execution_state: string;
  last_transition_at: string | null;
  lifecycle_summary: {
    current_state: string;
    total_events: number;
    last_transition_at: string | null;
  };
  ack_payload: Record<string, unknown> | null;
  latest_status_payload: Record<string, unknown> | null;
  risk_decision_id: string | null;
  created_by: string | null;
  created_at: string;
  updated_at: string;
};

export type ExchangeTestnetOrdersResponse = {
  orders: ExchangeTestnetOrderRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeTestnetOrderResponse = {
  order: ExchangeTestnetOrderRecord;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeTestnetPipelinePreview = {
  strategy_id: string | null;
  signal_id: string | null;
  risk_decision_id: string;
  symbol: string;
  side: string;
  order_type: string;
  quantity: string;
  quote_notional: string;
  reference_price: string;
  reference_price_received_at: string;
  confirmation_text: string;
  execution_state_preview: string;
  correlation_id: string;
  previewed_at: string;
};

export type ExchangeTestnetPipelinePreviewResponse = {
  preview: ExchangeTestnetPipelinePreview;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeTestnetPipelineSubmitResponse = {
  preview: ExchangeTestnetPipelinePreview;
  order: ExchangeTestnetOrderRecord;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetShadowIntent = {
  exchange: string;
  environment: string;
  symbol: string;
  side: string;
  order_type: string;
  time_in_force: string | null;
  quantity: string | null;
  quote_notional: string | null;
  limit_price: string | null;
  risk_decision_id: string | null;
};

export type TestnetShadowRunResult = {
  run_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  decision: string;
  signal_id: string | null;
  risk_decision_id: string | null;
  would_submit_order: TestnetShadowIntent | null;
  reasons: string[];
  price_source: string | null;
  resolved_price: string | null;
  status: string;
  created_at: string;
  correlation_id: string;
};

export type TestnetShadowRunResponse = {
  run: TestnetShadowRunResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetShadowRunsResponse = {
  runs: TestnetShadowRunResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeTestnetOrderLifecycleEvent = {
  previous_state: string | null;
  next_state: string;
  transition_source: string;
  reason: string | null;
  created_at: string;
};

export type ExchangeTestnetOrderLifecycleResponse = {
  client_order_id: string;
  current_state: string;
  events: ExchangeTestnetOrderLifecycleEvent[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeTestnetRepairValidationIssue = {
  code: string;
  message: string;
};

export type ExchangeTestnetRepairActionRecord = {
  id: string;
  client_order_id: string;
  action: string;
  status: string;
  previous_state: string | null;
  next_state: string | null;
  reason: string | null;
  payload: Record<string, unknown> | null;
  actor_id: string | null;
  created_at: string;
  correlation_id: string | null;
};

export type ExchangeTestnetRepairResponse = {
  client_order_id: string;
  action: string;
  status: string;
  previous_state: string | null;
  next_state: string | null;
  correlation_id: string;
  issues: ExchangeTestnetRepairValidationIssue[];
  request_id: string;
  timestamp: string;
};

export type ExchangeTestnetRepairsResponse = {
  client_order_id: string;
  repairs: ExchangeTestnetRepairActionRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangePrivateStreamStateRecord = {
  exchange: string;
  environment: string;
  status: string;
  listen_key_hash: string | null;
  connected_at: string | null;
  last_event_at: string | null;
  last_error: string | null;
  reconnect_count: number;
  updated_at: string;
  is_stale: boolean;
};

export type ExchangePrivateStreamEventRecord = {
  id: string;
  exchange: string;
  environment: string;
  source: string;
  event_type: string;
  symbol: string | null;
  client_order_id: string | null;
  exchange_order_id: string | null;
  execution_type: string | null;
  order_status: string | null;
  payload: Record<string, unknown>;
  event_time: string;
  received_at: string;
  correlation_id: string | null;
};

export type ExchangePrivateStreamStatusResponse = {
  state: ExchangePrivateStreamStateRecord;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangePrivateStreamEventsResponse = {
  events: ExchangePrivateStreamEventRecord[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangePrivateStreamListenKeyResponse = {
  state: ExchangePrivateStreamStateRecord;
  listen_key_status: string;
  listen_key_masked: string | null;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeReconciliationRequest = {
  limit?: number;
  status_filter?: string[];
  correlation_id?: string;
};

export type ExchangeReconciliationResult = {
  run_id: string;
  status: string;
  checked_orders: number;
  matched_orders: number;
  mismatched_orders: number;
  unknown_orders: number;
  correlation_id: string;
};

export type ExchangeReconciliationRun = {
  id: string;
  exchange: string;
  environment: string;
  status: string;
  checked_orders: number;
  matched_orders: number;
  mismatched_orders: number;
  unknown_orders: number;
  failed_reason: string | null;
  started_at: string;
  completed_at: string | null;
  correlation_id: string;
};

export type ExchangeReconciliationMismatch = {
  id: string;
  run_id: string;
  client_order_id: string;
  local_status: string | null;
  exchange_status: string | null;
  mismatch_kind: string;
  action: string;
  payload: Record<string, unknown>;
  created_at: string;
};

export type ExchangeReconciliationResultResponse = {
  result: ExchangeReconciliationResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeReconciliationRunsResponse = {
  runs: ExchangeReconciliationRun[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeReconciliationRunResponse = {
  run: ExchangeReconciliationRun;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExchangeReconciliationMismatchesResponse = {
  mismatches: ExchangeReconciliationMismatch[];
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
