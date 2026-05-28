export type ApiError = {
  error: string;
  message: string;
  rejection?: {
    reason_code: string;
    recommendation: string;
    last_observed_at: string | null;
    observation_expires_at: string | null;
    observation_age_seconds: number | null;
    observation_max_age_seconds: number | null;
  };
  request_id?: string;
  correlation_id?: string;
  timestamp?: string;
};

export type StrategyPerformanceMode = "BACKTEST" | "PAPER" | "SHADOW" | "COMBINED";

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

export type MarketProviderAttempt = {
  provider: string;
  base_url: string;
  endpoint: string;
  success: boolean;
  latency_ms: number | null;
  http_status: number | null;
  error_kind: string | null;
  recommendation: string | null;
};

export type MarketProviderDiagnostic = {
  provider: string;
  base_url: string;
  endpoint: string;
  symbol: string | null;
  interval: string | null;
  start_time: string | null;
  end_time: string | null;
  http_status: number | null;
  error_kind: string;
  retryable: boolean;
  message: string;
  recommendation: string;
};

export type MarketProviderHealth = {
  provider: string;
  status: string;
  base_url: string;
  endpoint: string;
  latency_ms: number | null;
  http_status: number | null;
  error_kind: string | null;
  recommendation: string | null;
  fallback_available: boolean;
  fallback_base_urls: string[];
  attempts: MarketProviderAttempt[];
};

export type ProviderHealthResponse = {
  health: MarketProviderHealth;
  websocket_checked: boolean;
  websocket_base_url: string | null;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyPerformanceSummary = {
  strategy_id: string | null;
  symbol: string | null;
  timeframe: string | null;
  mode: StrategyPerformanceMode;
  window_start: string;
  window_end: string;
  total_runs: number;
  total_signals: number;
  approved_risk_decisions: number;
  rejected_risk_decisions: number;
  risk_rejection_rate: string;
  shadow_would_submit_count: number;
  shadow_no_signal_count: number;
  shadow_risk_rejected_count: number;
  paper_orders_count: number;
  paper_positions_opened: number;
  paper_positions_closed: number;
  realized_pnl: string;
  unrealized_pnl: string;
  win_rate: string | null;
  avg_win: string | null;
  avg_loss: string | null;
  max_drawdown_pct: string | null;
  backtest_runs_count: number;
  best_backtest_pnl_pct: string | null;
  worst_backtest_pnl_pct: string | null;
  avg_backtest_pnl_pct: string | null;
  created_at: string;
  computed_at: string;
};

export type StrategyComparisonSummary = {
  strategy_id: string;
  symbol: string | null;
  timeframe: string | null;
  mode: StrategyPerformanceMode;
  realized_pnl: string;
  unrealized_pnl: string;
  risk_rejection_rate: string;
  win_rate: string | null;
  best_backtest_pnl_pct: string | null;
  worst_backtest_pnl_pct: string | null;
  avg_backtest_pnl_pct: string | null;
  shadow_would_submit_count: number;
  shadow_no_signal_count: number;
  shadow_risk_rejected_count: number;
  approved_risk_decisions: number;
  rejected_risk_decisions: number;
  paper_orders_count: number;
  total_signals: number;
  total_runs: number;
  computed_at: string;
};

export type StrategyDecisionBreakdown = {
  strategy_id: string;
  symbol: string | null;
  timeframe: string | null;
  window_start: string;
  window_end: string;
  total_runs: number;
  would_submit_count: number;
  no_signal_count: number;
  risk_rejected_count: number;
  skipped_count: number;
  error_count: number;
  computed_at: string;
};

export type StrategyPnlBreakdown = {
  strategy_id: string | null;
  symbol: string | null;
  timeframe: string | null;
  mode: StrategyPerformanceMode;
  window_start: string;
  window_end: string;
  positions_opened: number;
  positions_closed: number;
  realized_pnl: string;
  unrealized_pnl: string;
  win_rate: string | null;
  avg_win: string | null;
  avg_loss: string | null;
  max_drawdown_pct: string | null;
  computed_at: string;
};

export type StrategyPerformanceSummaryResponse = {
  summary: StrategyPerformanceSummary;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExecutionReadinessTarget =
  | "PAPER_PIPELINE"
  | "TESTNET_SHADOW"
  | "TESTNET_PROMOTION"
  | "TESTNET_SUBMIT";

export type ExecutionReadinessStatus =
  | "READY"
  | "NOT_READY"
  | "DEGRADED"
  | "UNKNOWN";

export type ExecutionReadinessCheckSeverity =
  | "LOW"
  | "MEDIUM"
  | "HIGH"
  | "CRITICAL";

export type ExecutionReadinessCheck = {
  code: string;
  name: string;
  passed: boolean;
  blocking: boolean;
  severity: ExecutionReadinessCheckSeverity;
  summary: string;
  details?: Record<string, unknown> | null;
};

export type ExecutionReadinessRequest = {
  target: ExecutionReadinessTarget;
  symbol?: string | null;
  strategy_id?: string | null;
  timeframe?: string | null;
  promotion_id?: string | null;
  risk_decision_id?: string | null;
  start_time?: string | null;
  end_time?: string | null;
  persist?: boolean;
  correlation_id?: string | null;
};

export type ExecutionReadinessResult = {
  readiness_id: string;
  target: ExecutionReadinessTarget;
  status: ExecutionReadinessStatus;
  score: number;
  blocking_reasons: string[];
  warnings: ExecutionReadinessCheck[];
  checks: ExecutionReadinessCheck[];
  recommendations: string[];
  computed_at: string;
  correlation_id: string;
};

export type ExecutionReadinessSnapshot = {
  id: string;
  target: ExecutionReadinessTarget;
  status: ExecutionReadinessStatus;
  score: number;
  blocking_reasons: string[];
  warnings: ExecutionReadinessCheck[];
  checks: ExecutionReadinessCheck[];
  recommendations: string[];
  created_by: string | null;
  created_at: string;
  correlation_id: string | null;
};

export type ExecutionReadinessResponse = {
  readiness: ExecutionReadinessResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ExecutionReadinessSnapshotsResponse = {
  snapshots: ExecutionReadinessSnapshot[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyPerformanceRankingsResponse = {
  rankings: StrategyComparisonSummary[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyDecisionBreakdownResponse = {
  breakdown: StrategyDecisionBreakdown;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyPnlBreakdownResponse = {
  breakdown: StrategyPnlBreakdown;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetPromotionFunnelStage = {
  stage: string;
  count: number;
  rate_pct: string;
};

export type TestnetPromotionOutcomeBreakdown = {
  outcome: string;
  count: number;
  rate_pct: string;
};

export type TestnetPromotionDropoffBreakdown = {
  stage: string;
  dropped_count: number;
  dropoff_rate_pct: string;
};

export type TestnetPromotionLifecycleBreakdown = {
  execution_state: string;
  count: number;
  rate_pct: string;
};

export type TestnetPromotionQualitySignal = {
  signal: string;
  value_pct: string;
  numerator: number;
  denominator: number;
};

export type TestnetPromotionFunnelRow = {
  shadow_run_id: string;
  promotion_id: string | null;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  promotion_status: string | null;
  promotion_rejection_reasons: string[];
  testnet_order_id: string | null;
  client_order_id: string | null;
  execution_state: string | null;
  linked_order_missing: boolean;
  shadow_created_at: string;
  promotion_created_at: string | null;
  submitted_at: string | null;
  acked_at: string | null;
  last_lifecycle_at: string | null;
};

export type TestnetPromotionFunnelSummary = {
  strategy_id: string | null;
  symbol: string | null;
  timeframe: string | null;
  window_start: string | null;
  window_end: string | null;
  shadow_would_submit_count: number;
  promotion_previewed_count: number;
  promotion_submitted_count: number;
  promotion_rejected_count: number;
  promotion_expired_count: number;
  promotion_duplicate_rejected_count: number;
  testnet_orders_created_count: number;
  acked_count: number;
  filled_count: number;
  partially_filled_count: number;
  cancelled_count: number;
  rejected_count: number;
  expired_count: number;
  reconciliation_required_count: number;
  unknown_exchange_state_count: number;
  failed_count: number;
  preview_rate_pct: string;
  submit_rate_pct: string;
  ack_rate_pct: string;
  fill_rate_pct: string;
  reconciliation_required_rate_pct: string;
  avg_time_shadow_to_preview_seconds: string | null;
  avg_time_preview_to_submit_seconds: string | null;
  stages: TestnetPromotionFunnelStage[];
  outcome_breakdown: TestnetPromotionOutcomeBreakdown[];
  dropoff_breakdown: TestnetPromotionDropoffBreakdown[];
  lifecycle_breakdown: TestnetPromotionLifecycleBreakdown[];
  quality_signals: TestnetPromotionQualitySignal[];
  computed_at: string;
};

export type TestnetPromotionFunnelSummaryResponse = {
  summary: TestnetPromotionFunnelSummary;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetPromotionFunnelOutcomesResponse = {
  outcomes: TestnetPromotionOutcomeBreakdown[];
  lifecycle: TestnetPromotionLifecycleBreakdown[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetPromotionFunnelRowsResponse = {
  rows: TestnetPromotionFunnelRow[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type OperatorReportFormat = "JSON" | "MARKDOWN";
export type OperatorReportStatus = "OK" | "WARNING" | "CRITICAL";
export type OperatorReportSeverity = "INFO" | "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";

export type OperatorReportRequest = {
  start_time?: string;
  end_time?: string;
  symbol?: string;
  interval?: string;
  strategy_id?: string;
  format?: OperatorReportFormat;
  persist?: boolean;
  correlation_id?: string;
};

export type OperatorReportHighlight = {
  label: string;
  value: string;
};

export type OperatorReportFinding = {
  code: string;
  severity: OperatorReportSeverity;
  title: string;
  detail: string;
  section: string;
};

export type OperatorReportRecommendation = {
  code: string;
  priority: OperatorReportSeverity;
  detail: string;
  related_finding_codes: string[];
};

export type OperatorReportSection = {
  key: string;
  title: string;
  status: OperatorReportStatus;
  summary: string;
  highlights: OperatorReportHighlight[];
  snapshot: Record<string, unknown> | null;
};

export type OperatorReportSummary = {
  total_findings: number;
  critical_findings: number;
  high_findings: number;
  medium_findings: number;
  low_findings: number;
  info_findings: number;
  highest_severity: OperatorReportSeverity | null;
  kill_switch_active: boolean;
  stale_feed_count: number;
  risk_rejection_rate_pct: string;
  paper_daily_pnl: string;
  shadow_would_submit_count: number;
  reconciliation_required_count: number;
};

export type OperatorReport = {
  report_id: string;
  window_start: string;
  window_end: string;
  generated_at: string;
  status: OperatorReportStatus;
  summary: OperatorReportSummary;
  findings: OperatorReportFinding[];
  recommendations: OperatorReportRecommendation[];
  sections: OperatorReportSection[];
  format: OperatorReportFormat;
  persisted: boolean;
  correlation_id: string;
  markdown: string | null;
};

export type OperatorReportListItem = {
  report_id: string;
  window_start: string;
  window_end: string;
  format: string;
  status: string;
  created_at: string;
  created_by: string | null;
  correlation_id: string | null;
};

export type OperatorReportResponse = {
  report: OperatorReport;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type OperatorReportsListResponse = {
  reports: OperatorReportListItem[];
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
  provider_attempts: MarketProviderAttempt[];
  selected_provider: string | null;
  failure_diagnostic: MarketProviderDiagnostic | null;
  recommendation: string | null;
  correlation_id: string;
  created_at: string;
  completed_at: string | null;
};

export type CandleAggregationRequest = {
  exchange?: string;
  symbol: string;
  source_interval: string;
  target_interval: string;
  start_time: string;
  end_time: string;
  correlation_id?: string | null;
};

export type CandleAggregationResult = {
  exchange: string;
  symbol: string;
  source_interval: string;
  target_interval: string;
  start_time: string;
  end_time: string;
  source_candles: number;
  aggregated_candles: number;
  inserted: number;
  updated: number;
  skipped_incomplete: number;
  correlation_id: string | null;
};

export type CandleAggregationStatus = "FRESH" | "DEGRADED" | "STALE" | "MISSING";

export type CandleAggregationStatusRow = {
  symbol: string;
  source_interval: string;
  target_interval: string;
  latest_source_closed_candle: string | null;
  latest_target_closed_candle: string | null;
  lag_seconds: number | null;
  status: CandleAggregationStatus;
  inserted_last_tick: number | null;
  updated_last_tick: number | null;
  recommendation: string;
};

export type CandleAggregationStatusResponse = {
  rows: CandleAggregationStatusRow[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type CandleIntervalCoverage = {
  interval: string;
  candle_count: number;
};

export type MarketCandleCoverageSummary = {
  exchange: string;
  symbol: string;
  intervals: CandleIntervalCoverage[];
};

export type CandleCoverageSummary = MarketCandleCoverageSummary;

export type CandleCoverageResponse = {
  coverage: MarketCandleCoverageSummary;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type MarketDataQualityStatus =
  | "GOOD"
  | "DEGRADED"
  | "BAD"
  | "INSUFFICIENT_DATA"
  | "UNKNOWN";

export type MarketDataQualityRequest = {
  exchange?: string;
  symbol: string;
  interval: string;
  start_time: string;
  end_time: string;
  expected_interval_seconds?: number | null;
  max_allowed_gap_count?: number | null;
  max_allowed_gap_pct?: string | null;
};

export type MarketDataGap = {
  start_time: string;
  end_time: string;
  missing_candle_count: number;
  gap_seconds: number;
};

export type MarketDataQualityFinding = {
  severity: string;
  code: string;
  message: string;
};

export type MarketDataQualityRecommendation = {
  code: string;
  message: string;
};

export type MarketDataQualityReport = {
  exchange: string;
  symbol: string;
  interval: string;
  window_start: string;
  window_end: string;
  expected_candle_count: number;
  actual_candle_count: number;
  closed_candle_count: number;
  open_candle_count: number;
  missing_candle_count: number;
  coverage_pct: string;
  gap_count: number;
  largest_gap_seconds: number;
  gaps: MarketDataGap[];
  first_candle_time: string | null;
  last_candle_time: string | null;
  status: MarketDataQualityStatus;
  findings: MarketDataQualityFinding[];
  recommendations: MarketDataQualityRecommendation[];
};

export type MarketDataQualityResponse = {
  report: MarketDataQualityReport;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type MarketDataRepairStatus =
  | "NO_REPAIR_NEEDED"
  | "REPAIR_PLANNED"
  | "REPAIR_COMPLETED"
  | "PARTIAL_REPAIR"
  | "REPAIR_FAILED"
  | "INSUFFICIENT_DATA"
  | "UNSUPPORTED_INTERVAL";

export type MarketDataRepairRange = {
  source_interval: string;
  start_time: string;
  end_time: string;
  missing_candle_count: number;
};

export type MarketDataRepairRecommendation = {
  code: string;
  message: string;
};

export type MarketDataRepairPlanRequest = {
  exchange?: string;
  symbol: string;
  interval: string;
  start_time: string;
  end_time: string;
  repair_mode: "PLAN_ONLY" | "REPAIR";
  max_ranges?: number;
  reaggregate_derived_intervals?: boolean;
  correlation_id?: string | null;
};

export type MarketDataRepairPlan = {
  exchange: string;
  symbol: string;
  interval: string;
  start_time: string;
  end_time: string;
  status: MarketDataRepairStatus;
  initial_quality_status: MarketDataQualityStatus;
  gap_count: number;
  repair_ranges: MarketDataRepairRange[];
  estimated_source_interval: string | null;
  requires_source_interval: boolean;
  reaggregate_derived_intervals: boolean;
  recommendations: MarketDataRepairRecommendation[];
};

export type MarketDataRepairRunResult = {
  run_id: string;
  plan: MarketDataRepairPlan;
  status: MarketDataRepairStatus;
  before_quality_status: MarketDataQualityStatus;
  after_quality_status: MarketDataQualityStatus;
  gap_count_before: number;
  gap_count_after: number;
  attempted_ranges: MarketDataRepairRange[];
  inserted_candles: number;
  updated_candles: number;
  skipped_candles: number;
  failed_ranges: number;
  selected_provider: string | null;
  recommendations: MarketDataRepairRecommendation[];
  created_at: string;
  completed_at: string | null;
};

export type MarketDataRepairPlanResponse = {
  plan: MarketDataRepairPlan;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type MarketDataRepairRunResponse = {
  run: MarketDataRepairRunResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type MarketDataRepairRunsResponse = {
  runs: MarketDataRepairRunResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchDataReadinessStatus = "READY" | "DEGRADED" | "INSUFFICIENT";

export type ResearchDataGap = {
  start_time: string;
  end_time: string;
  missing_candles: number;
};

export type ResearchIntervalCoverageSummary = {
  interval: string;
  expected_candles: number;
  actual_candles: number;
  coverage_pct: string;
  first_candle_at: string | null;
  last_candle_at: string | null;
  missing_ranges: ResearchDataGap[];
  status: ResearchDataReadinessStatus;
};

export type ResearchDataCoverageRequest = {
  exchange?: string;
  symbol: string;
  intervals: string[];
  start_time: string;
  end_time: string;
  required_coverage_pct?: string;
  correlation_id?: string | null;
};

export type ResearchDataCoverageResult = {
  exchange: string;
  symbol: string;
  window_start: string;
  window_end: string;
  required_coverage_pct: string;
  status: ResearchDataReadinessStatus;
  per_interval: ResearchIntervalCoverageSummary[];
  correlation_id: string | null;
};

export type ResearchDataCoverageResponse = {
  coverage: ResearchDataCoverageResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchDatasetBuildStepStatus = "STARTED" | "COMPLETED" | "FAILED";
export type ResearchDatasetBuildStatus = "STARTED" | "COMPLETED" | "FAILED";

export type ResearchDatasetBuildStep = {
  step: string;
  status: ResearchDatasetBuildStepStatus;
  details: unknown | null;
  started_at: string;
  completed_at: string | null;
};

export type ResearchDatasetBuildRequest = {
  exchange?: string;
  symbol: string;
  intervals: string[];
  start_time: string;
  end_time: string;
  required_coverage_pct?: string;
  correlation_id?: string | null;
};

export type ResearchDatasetBuildResult = {
  build_id: string;
  exchange: string;
  symbol: string;
  requested_intervals: string[];
  start_time: string;
  end_time: string;
  status: ResearchDatasetBuildStatus;
  coverage_before: ResearchDataCoverageResult;
  coverage_after: ResearchDataCoverageResult;
  steps: ResearchDatasetBuildStep[];
  failed_reason: string | null;
  correlation_id: string;
  created_at: string;
  completed_at: string | null;
};

export type ResearchDatasetBuildResponse = {
  build: ResearchDatasetBuildResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchDatasetBuildsResponse = {
  builds: ResearchDatasetBuildResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchBatchStatus = "STARTED" | "PARTIAL" | "COMPLETED" | "FAILED";
export type ResearchBatchTriageStatus =
  | "ACTIONABLE"
  | "WEAK"
  | "OVERFIT_ONLY"
  | "NO_CANDIDATES"
  | "DATA_QUALITY_BLOCKED"
  | "FAILED"
  | "UNKNOWN";
export type ResearchBatchStepStatus =
  | "PENDING"
  | "RUNNING"
  | "COMPLETED"
  | "SKIPPED"
  | "FAILED";

export type ResearchBatchRequest = {
  strategy_id: string;
  symbol: string;
  base_interval?: string;
  target_intervals: string[];
  start_time: string;
  end_time: string;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  experiment_timeframes: string[];
  lookback_candidates: number[];
  trend_lookback_candidates?: number[] | null;
  momentum_lookback_candidates?: number[] | null;
  breakout_lookback_candidates?: number[] | null;
  lower_band_pct_candidates?: string[] | null;
  upper_band_pct_candidates?: string[] | null;
  min_range_width_pct_candidates?: string[] | null;
  max_range_width_pct_candidates?: string[] | null;
  min_close_above_sma_pct_candidates?: string[] | null;
  max_close_above_sma_pct_candidates?: string[] | null;
  min_momentum_return_pct_candidates?: string[] | null;
  holding_candles_candidates?: number[] | null;
  walk_forward_top_n?: number;
  repair_degraded_data?: boolean;
  create_candidates?: boolean;
  max_candidates?: number;
  correlation_id?: string | null;
};

export type ResearchBatchStep = {
  id: string;
  batch_id: string;
  step_name: string;
  status: ResearchBatchStepStatus;
  started_at: string;
  completed_at: string | null;
  result: unknown | null;
  error: string | null;
};

export type ResearchBatchCandidateSummary = {
  experiment_id: string;
  experiment_run_id: string;
  walk_forward_run_id: string | null;
  candidate_id: string | null;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  score: string;
  pnl_pct: string;
  max_drawdown_pct: string;
  trade_count: number;
  win_rate: string;
  robustness_status: string | null;
};

export type ResearchBatchRecommendation = {
  severity: string;
  code: string;
  message: string;
};

export type ResearchBatchResult = {
  batch_id: string;
  status: ResearchBatchStatus;
  steps: ResearchBatchStep[];
  provider_health_summary: MarketProviderHealth | null;
  backfill_summary: unknown | null;
  quality_before: MarketDataQualityReport | null;
  repair_summary: unknown | null;
  quality_after: MarketDataQualityReport | null;
  aggregation_summary: unknown | null;
  experiment_ids: string[];
  walk_forward_run_ids: string[];
  created_candidate_ids: string[];
  top_candidates: ResearchBatchCandidateSummary[];
  recommendations: ResearchBatchRecommendation[];
  created_at: string;
  completed_at: string | null;
};

export type ResearchBatchResponse = {
  batch: ResearchBatchResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchBatchTriageFinding = {
  severity: string;
  code: string;
  message: string;
};

export type ResearchBatchTriageRecommendation = {
  priority: string;
  code: string;
  message: string;
};

export type ResearchBatchCandidateTriage = {
  candidate_id: string;
  experiment_run_id: string;
  walk_forward_run_id: string | null;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  experiment_score: string;
  experiment_pnl_pct: string;
  walk_forward_status: string | null;
  walk_forward_recommendation: string | null;
  qualification_status: string | null;
  dossier_status: string | null;
  triage_status: ResearchBatchTriageStatus;
  rank: number;
  reasons: string[];
  recommendations: string[];
};

export type ResearchBatchTriage = {
  batch_id: string;
  status: ResearchBatchTriageStatus;
  candidate_count: number;
  actionable_count: number;
  weak_count: number;
  overfit_count: number;
  candidates: ResearchBatchCandidateTriage[];
  findings: ResearchBatchTriageFinding[];
  recommendations: ResearchBatchTriageRecommendation[];
  generated_at: string;
};

export type ResearchBatchTriageResponse = {
  triage: ResearchBatchTriage;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchBatchesResponse = {
  batches: ResearchBatchResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchBatchStepsResponse = {
  steps: ResearchBatchStep[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCampaignStatus =
  | "COMPLETED"
  | "PARTIAL_SUCCESS"
  | "FAILED"
  | "CANCELLED";

export type ResearchCampaignWindow = {
  start_time: string;
  end_time: string;
  regime_label?: ResearchRegimeLabel | null;
};

export type ResearchCampaignRequest = {
  strategies: string[];
  symbols: string[];
  experiment_timeframes: string[];
  windows?: ResearchCampaignWindow[];
  campaign_start?: string | null;
  campaign_end?: string | null;
  window_hours?: number | null;
  step_hours?: number | null;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  max_batches?: number | null;
  regime_dataset_id?: string | null;
  target_regimes?: ResearchRegimeLabel[] | null;
  max_windows_per_regime?: number | null;
  max_candidates_per_batch?: number;
  create_candidates?: boolean;
  repair_degraded_data?: boolean;
  walk_forward_top_n?: number;
  base_interval?: string;
  lookback_candidates?: number[];
  trend_lookback_candidates?: number[] | null;
  momentum_lookback_candidates?: number[] | null;
  breakout_lookback_candidates?: number[] | null;
  lower_band_pct_candidates?: string[] | null;
  upper_band_pct_candidates?: string[] | null;
  min_range_width_pct_candidates?: string[] | null;
  max_range_width_pct_candidates?: string[] | null;
  min_close_above_sma_pct_candidates?: string[] | null;
  max_close_above_sma_pct_candidates?: string[] | null;
  min_momentum_return_pct_candidates?: string[] | null;
  holding_candles_candidates?: number[] | null;
  correlation_id?: string | null;
};

export type ResearchCampaignBatchPlan = {
  plan_index: number;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  regime_label?: ResearchRegimeLabel | null;
};

export type ResearchCampaignBatchResult = {
  plan: ResearchCampaignBatchPlan;
  research_batch_id: string | null;
  batch_status: ResearchBatchStatus | null;
  triage_status: ResearchBatchTriageStatus;
  candidates_created: number;
  top_candidates: ResearchBatchCandidateSummary[];
  error: string | null;
  started_at: string;
  completed_at: string | null;
};

export type ResearchCampaignFinding = {
  severity: string;
  code: string;
  message: string;
};

export type ResearchCampaignRecommendation = {
  priority: string;
  code: string;
  message: string;
};

export type ResearchCampaignSummary = {
  total_batches_planned: number;
  total_batches_completed: number;
  total_batches_failed: number;
  actionable_batches: number;
  overfit_only_batches: number;
  weak_batches: number;
  data_quality_blocked_batches: number;
  no_candidate_batches: number;
  candidates_created: number;
  top_candidates: ResearchBatchCandidateSummary[];
  best_strategy_symbol_timeframe: string | null;
  per_regime_performance?: ResearchCampaignRegimePerformance[];
  findings: ResearchCampaignFinding[];
  recommendations: ResearchCampaignRecommendation[];
};

export type ResearchCampaignRegimePerformance = {
  regime_label: ResearchRegimeLabel;
  planned_batches: number;
  completed_batches: number;
  failed_batches: number;
  actionable_batches: number;
  weak_batches: number;
  candidates_created: number;
};

export type ResearchRegimeLabel =
  | "TREND_UP"
  | "TREND_DOWN"
  | "RANGE"
  | "HIGH_VOLATILITY"
  | "LOW_VOLATILITY"
  | "MIXED"
  | "UNKNOWN";

export type ResearchCandidateFailureReason =
  | "OVERFIT_RISK"
  | "FEE_DRAG"
  | "TOO_MANY_TRADES"
  | "TOO_FEW_TRADES"
  | "LOW_WIN_RATE"
  | "HIGH_DRAWDOWN"
  | "WEAK_EDGE"
  | "DATA_QUALITY_DEGRADED"
  | "REGIME_MISMATCH"
  | "INSUFFICIENT_DATA";

export type ResearchCampaignRegimeSummary = {
  label: ResearchRegimeLabel;
  window_count: number;
  candidate_count: number;
  avg_return_pct: string;
  avg_realized_volatility: string;
  avg_candle_range_pct: string;
  failure_reasons: ResearchCandidateFailureReason[];
};

export type ResearchCandidateFailureAttributionRow = {
  candidate_id: string | null;
  experiment_run_id: string | null;
  walk_forward_run_id: string | null;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  window_start: string;
  window_end: string;
  regime_label: ResearchRegimeLabel;
  failure_reasons: ResearchCandidateFailureReason[];
  pnl_pct: string | null;
  gross_pnl_pct: string | null;
  fee_drag_pct: string | null;
  trade_count: number | null;
  win_rate: string | null;
  max_drawdown_pct: string | null;
  walk_forward_status: string | null;
  data_quality_status: MarketDataQualityStatus | null;
};

export type ResearchStrategyTimeframeFailureBreakdown = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  candidate_count: number;
  dominant_regime: ResearchRegimeLabel;
  top_failure_reasons: ResearchCandidateFailureReason[];
  avg_pnl_pct: string | null;
  avg_trade_count: string | null;
};

export type ResearchCampaignFailureAttribution = {
  campaign_id: string;
  overall_failure_reasons: ResearchCandidateFailureReason[];
  regime_summary: ResearchCampaignRegimeSummary[];
  candidate_failure_table: ResearchCandidateFailureAttributionRow[];
  strategy_timeframe_breakdown: ResearchStrategyTimeframeFailureBreakdown[];
  findings: ResearchCampaignFinding[];
  recommendations: ResearchCampaignRecommendation[];
  generated_at: string;
};

export type ResearchCampaignFailureAttributionResponse = {
  attribution: ResearchCampaignFailureAttribution;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeStrategyStatus =
  | "ROBUST"
  | "PROMISING"
  | "WEAK"
  | "NEGATIVE"
  | "OVERFIT"
  | "INSUFFICIENT_DATA"
  | "DATA_QUALITY_BLOCKED";

export type ResearchRegimeStrategyRanking = {
  rank: number;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  status: ResearchRegimeStrategyStatus;
  candidate_count: number;
  batch_count: number;
  avg_pnl_pct: string;
  median_pnl_pct: string;
  best_pnl_pct: string;
  worst_pnl_pct: string;
  profitable_candidate_ratio: string;
  overfit_count: number;
  weak_count: number;
  actionable_count: number;
  avg_walk_forward_score: string | null;
  avg_trade_count: string;
  avg_fee_drag_pct: string | null;
  data_quality_warning_count: number;
  robustness_score: number;
  ranking_score: string;
};

export type ResearchRegimeStrategyCell = {
  regime_label: ResearchRegimeLabel;
  rankings: ResearchRegimeStrategyRanking[];
};

export type ResearchRegimeStrategySelection = {
  regime_label: ResearchRegimeLabel;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  status: ResearchRegimeStrategyStatus;
  is_promising: boolean;
  is_least_bad: boolean;
  score: number;
  reason: string;
  robustness_score: number;
  median_pnl_pct: string;
};

export type ResearchRegimeSymbolTimeframeSelection = {
  regime_label: ResearchRegimeLabel;
  symbol: string;
  timeframe: string;
  strategy_id: string;
  status: ResearchRegimeStrategyStatus;
  robustness_score: number;
  median_pnl_pct: string;
};

export type ResearchRegimeStrategyFinding = {
  severity: string;
  code: string;
  message: string;
};

export type ResearchRegimeStrategyRecommendation = {
  priority: string;
  code: string;
  message: string;
};

export type ResearchRegimeStrategyLeaderboard = {
  campaign_id: string;
  generated_at: string;
  per_regime: ResearchRegimeStrategyCell[];
  overall_rankings: ResearchRegimeStrategyRanking[];
  overall_best?: ResearchRegimeStrategyRanking | null;
  overall_promising?: ResearchRegimeStrategyRanking | null;
  overall_least_bad?: ResearchRegimeStrategyRanking | null;
  best_strategy_by_regime: ResearchRegimeStrategySelection[];
  worst_strategy_by_regime: ResearchRegimeStrategySelection[];
  best_symbol_timeframe_by_regime: ResearchRegimeSymbolTimeframeSelection[];
  findings: ResearchRegimeStrategyFinding[];
  recommendations: ResearchRegimeStrategyRecommendation[];
};

export type ResearchRegimeStrategyLeaderboardResponse = {
  leaderboard: ResearchRegimeStrategyLeaderboard;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchHypothesisSource =
  | "CAMPAIGN_FAILURE_ATTRIBUTION"
  | "REGIME_LEADERBOARD"
  | "OPPORTUNITY_ANALYSIS"
  | "SIGNAL_FEATURE_ATTRIBUTION"
  | "EXIT_ATTRIBUTION"
  | "DATA_QUALITY";

export type ResearchHypothesisStatus =
  | "PROPOSED"
  | "ACCEPTED_FOR_EXPERIMENT"
  | "REJECTED"
  | "ARCHIVED";

export type ResearchHypothesisPriority = "HIGH" | "MEDIUM" | "LOW";

export type ResearchHypothesis = {
  id: string | null;
  source_type: ResearchHypothesisSource;
  status: ResearchHypothesisStatus;
  strategy_id: string | null;
  symbol: string | null;
  timeframe: string | null;
  regime: ResearchRegimeLabel | null;
  failure_reasons: ResearchCandidateFailureReason[];
  evidence: { summary: string; details: unknown };
  recommendation: { code: string; actions: string[] };
  proposed_action: string;
  proposed_experiment_config: unknown;
  priority: ResearchHypothesisPriority;
  expected_effect: string;
  risk: string;
  created_at: string;
};

export type ResearchHypothesisGenerationRequest = {
  campaign_id?: string;
  batch_id?: string;
  candidate_id?: string;
  include_sources?: string[];
  persist?: boolean;
};

export type ResearchHypothesisGenerationResult = {
  hypotheses: ResearchHypothesis[];
  generated_count: number;
  persisted_count: number;
  generated_at: string;
};

export type ResearchHypothesisGenerationResponse = {
  result: ResearchHypothesisGenerationResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchHypothesesResponse = {
  hypotheses: ResearchHypothesis[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchHypothesisResponse = {
  hypothesis: ResearchHypothesis;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchExperimentPlanStatus =
  | "DRAFT"
  | "READY"
  | "INVALID"
  | "RUNNABLE"
  | "ARCHIVED";

export type ResearchExperimentPlanType =
  | "STRATEGY_EXPERIMENT"
  | "RESEARCH_BATCH"
  | "RESEARCH_CAMPAIGN"
  | "ROBUSTNESS_MATRIX"
  | "WALK_FORWARD";

export type ResearchExperimentPlan = {
  id: string | null;
  hypothesis_id: string;
  source: "ACCEPTED_HYPOTHESIS" | "OPERATOR_DRAFT";
  source_campaign_id: string | null;
  strategy_id: string;
  symbol: string | null;
  timeframe: string | null;
  proposed_request: unknown;
  plan_type: ResearchExperimentPlanType;
  status: ResearchExperimentPlanStatus;
  validation_status: ResearchExperimentPlanStatus;
  validation_issues: string[];
  steps: { step_index: number; code: string; description: string; research_only: boolean }[];
  recommendation: { code: string; action: string; rationale: string };
  created_at: string;
  updated_at: string;
  correlation_id: string | null;
};

export type ResearchExperimentPlanRunStatus =
  | "READY"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "BLOCKED"
  | "INVALID_PLAN";

export type ResearchExperimentPlanRunMode = "PREVIEW" | "RUN";

export type ResearchExperimentPlanRunArtifact = {
  strategy_experiment_id: string | null;
  research_batch_id: string | null;
  research_campaign_id: string | null;
  robustness_matrix_run_id: string | null;
  walk_forward_run_id: string | null;
};

export type ResearchExperimentPlanRunResult = {
  plan_id: string;
  hypothesis_id: string;
  plan_type: ResearchExperimentPlanType;
  status: ResearchExperimentPlanRunStatus;
  mode: ResearchExperimentPlanRunMode;
  validation_status: ResearchExperimentPlanStatus;
  created_artifacts: ResearchExperimentPlanRunArtifact[];
  artifact_ids: string[];
  warnings: string[];
  blockers: string[];
  recommendation: string;
  correlation_id: string | null;
};

export type ResearchExperimentPlansResponse = {
  plans: ResearchExperimentPlan[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchExperimentPlanResponse = {
  plan: ResearchExperimentPlan;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchExperimentPlanRunResponse = {
  result: ResearchExperimentPlanRunResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCampaignResult = {
  campaign_id: string;
  status: ResearchCampaignStatus;
  request: ResearchCampaignRequest;
  batches: ResearchCampaignBatchResult[];
  summary: ResearchCampaignSummary;
  created_at: string;
  completed_at: string | null;
};

export type ResearchCampaignResponse = {
  campaign: ResearchCampaignResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCampaignsResponse = {
  campaigns: ResearchCampaignResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCampaignBatchesResponse = {
  batches: ResearchCampaignBatchResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCampaignSummaryResponse = {
  summary: ResearchCampaignSummary;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeDatasetStatus = "COMPLETED" | "PARTIAL" | "FAILED";
export type ResearchRegimeDiscoveryStatus =
  | "COMPLETED"
  | "PARTIAL"
  | "INSUFFICIENT_DATA"
  | "FAILED";
export type ResearchRegimeCalibrationStatus =
  | "COMPLETED"
  | "PARTIAL"
  | "INSUFFICIENT_DATA"
  | "FAILED";

export type ResearchRegimeClassifierConfig = {
  trend_return_threshold_pct: string;
  trend_slope_threshold: string;
  range_return_max_pct: string;
  range_choppiness_min: string;
  high_volatility_threshold_pct: string;
  low_volatility_threshold_pct: string;
  min_confidence: string;
  priority_order: ResearchRegimeLabel[];
};

export type ResearchRegimeDatasetRequest = {
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  window_hours: number;
  step_hours: number;
  min_candles_per_window: number;
  target_regimes?: ResearchRegimeLabel[] | null;
  max_windows_per_regime?: number | null;
  require_good_data_quality?: boolean;
  classifier_config?: ResearchRegimeClassifierConfig | null;
};

export type ResearchRegimeDiscoveryRequest = {
  symbol: string;
  timeframe: string;
  scan_start: string;
  scan_end: string;
  window_hours: number;
  step_hours: number;
  target_regimes?: ResearchRegimeLabel[] | null;
  max_windows_per_regime?: number;
  min_confidence?: string | null;
  require_existing_candles?: boolean;
  auto_backfill_missing?: boolean;
  classifier_config?: ResearchRegimeClassifierConfig | null;
  calibration_id?: string | null;
};

export type ResearchRegimeDatasetFromDiscoveryRequest = {
  discovery_id: string;
  target_regimes?: ResearchRegimeLabel[] | null;
  max_windows_per_regime?: number | null;
};

export type ResearchRegimeWindowMetric = {
  name: string;
  value: string;
  threshold: string | null;
  passed: boolean;
};

export type ResearchRegimeClassificationCondition = {
  label: ResearchRegimeLabel;
  metric: string;
  operator: string;
  value: string;
  threshold: string;
  passed: boolean;
  reason: string;
};

export type ResearchRegimeClassificationExplanation = {
  return_pct: string;
  realized_volatility: string;
  avg_range_pct: string;
  trend_slope: string;
  choppiness_proxy: string;
  thresholds_used: ResearchRegimeClassifierConfig;
  conditions: ResearchRegimeClassificationCondition[];
  final_label: ResearchRegimeLabel;
  confidence: string;
  alternate_labels_considered: ResearchRegimeLabel[];
};

export type ResearchRegimeWindow = {
  id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  regime_label: ResearchRegimeLabel;
  return_pct: string;
  realized_volatility: string;
  avg_range_pct: string;
  trend_slope: string;
  choppiness_proxy: string;
  data_quality_status: MarketDataQualityStatus;
  candle_count: number;
  score: string;
  confidence: string;
  metrics: ResearchRegimeWindowMetric[];
  explanation: ResearchRegimeClassificationExplanation;
};

export type ResearchRegimeDatasetRecommendation = {
  priority: string;
  code: string;
  message: string;
};

export type ResearchRegimeDatasetSummary = {
  total_candidate_windows: number;
  selected_windows: number;
  data_quality_blocked_windows: number;
  insufficient_candle_windows: number;
  regime_counts: Partial<Record<ResearchRegimeLabel, number>>;
  missing_regimes: ResearchRegimeLabel[];
  recommendations: ResearchRegimeDatasetRecommendation[];
};

export type ResearchRegimeDatasetResult = {
  dataset_id: string;
  status: ResearchRegimeDatasetStatus;
  request: ResearchRegimeDatasetRequest;
  summary: ResearchRegimeDatasetSummary;
  windows: ResearchRegimeWindow[];
  created_at: string;
};

export type ResearchRegimeDiscoveryCandidateWindow = {
  id: string;
  regime_label: ResearchRegimeLabel;
  start_time: string;
  end_time: string;
  confidence: string;
  return_pct: string;
  realized_volatility: string;
  avg_range_pct: string;
  trend_slope: string;
  choppiness_proxy: string;
  data_quality_status: MarketDataQualityStatus;
  candle_count: number;
  explanation: ResearchRegimeClassificationExplanation;
};

export type ResearchRegimeThresholdCandidate = {
  candidate_id: string;
  classifier_config: ResearchRegimeClassifierConfig;
};

export type ResearchRegimeCalibrationRequest = {
  symbol: string;
  timeframe: string;
  scan_start: string;
  scan_end: string;
  window_hours: number;
  step_hours: number;
  threshold_candidates?: ResearchRegimeThresholdCandidate[] | null;
  target_min_windows_per_regime?: number;
};

export type ResearchRegimeCalibrationRecommendation = {
  priority: string;
  code: string;
  message: string;
};

export type ResearchRegimeCalibrationCandidateResult = {
  candidate_id: string;
  classifier_config: ResearchRegimeClassifierConfig;
  counts_by_regime: Partial<Record<ResearchRegimeLabel, number>>;
  missing_regimes: ResearchRegimeLabel[];
  total_windows_scanned: number;
  data_quality_good_windows: number;
  avg_confidence: string;
  diversity_score: string;
  balance_score: string;
  dominant_regime_share: string;
  total_score: string;
  warnings: string[];
  explanation_samples: ResearchRegimeClassificationExplanation[];
};

export type ResearchRegimeCalibrationResult = {
  calibration_id: string;
  status: ResearchRegimeCalibrationStatus;
  request: ResearchRegimeCalibrationRequest;
  candidates: ResearchRegimeCalibrationCandidateResult[];
  recommended_config: ResearchRegimeClassifierConfig | null;
  recommended_candidate_id: string | null;
  missing_regimes: ResearchRegimeLabel[];
  recommendations: ResearchRegimeCalibrationRecommendation[];
  created_at: string;
};

export type ResearchRegimeDiscoveryRecommendation = {
  priority: string;
  code: string;
  message: string;
};

export type ResearchRegimeDiscoverySummary = {
  total_windows_scanned: number;
  selected_window_count: number;
  counts_by_regime: Partial<Record<ResearchRegimeLabel, number>>;
  missing_regimes: ResearchRegimeLabel[];
  data_quality_blocked_count: number;
  insufficient_data_count: number;
  recommendations: ResearchRegimeDiscoveryRecommendation[];
};

export type ResearchRegimeDiscoveryResult = {
  discovery_id: string;
  status: ResearchRegimeDiscoveryStatus;
  symbol: string;
  timeframe: string;
  scan_start: string;
  scan_end: string;
  total_windows_scanned: number;
  selected_windows: ResearchRegimeDiscoveryCandidateWindow[];
  counts_by_regime: Partial<Record<ResearchRegimeLabel, number>>;
  missing_regimes: ResearchRegimeLabel[];
  data_quality_blocked_count: number;
  recommendations: ResearchRegimeDiscoveryRecommendation[];
  request: ResearchRegimeDiscoveryRequest;
  summary: ResearchRegimeDiscoverySummary;
  created_at: string;
};

export type ResearchRegimeDatasetResponse = {
  dataset: ResearchRegimeDatasetResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeDiscoveryResponse = {
  discovery: ResearchRegimeDiscoveryResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeCalibrationResponse = {
  calibration: ResearchRegimeCalibrationResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeCalibrationsResponse = {
  calibrations: ResearchRegimeCalibrationResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeCalibrationCandidatesResponse = {
  candidates: ResearchRegimeCalibrationCandidateResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeDiscoveriesResponse = {
  discoveries: ResearchRegimeDiscoveryResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeDiscoveryWindowsResponse = {
  windows: ResearchRegimeDiscoveryCandidateWindow[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeDatasetsResponse = {
  datasets: ResearchRegimeDatasetResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchRegimeDatasetWindowsResponse = {
  windows: ResearchRegimeWindow[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateStatus =
  | "DISCOVERED"
  | "OBSERVING"
  | "ACCEPTED_FOR_SHADOW"
  | "PROMOTED_TO_SHADOW_CONFIG"
  | "REJECTED"
  | "ARCHIVED";

export type ResearchCandidateDecision =
  | "ACCEPT_FOR_SHADOW"
  | "PROMOTE_TO_SHADOW_CONFIG"
  | "REJECT"
  | "ARCHIVE"
  | "REOPEN";

export type ResearchCandidate = {
  id: string;
  experiment_id: string | null;
  experiment_run_id: string | null;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  config: Record<string, unknown>;
  score: string | null;
  pnl_pct: string | null;
  max_drawdown_pct: string | null;
  trade_count: number | null;
  win_rate: string | null;
  fee_drag: string | null;
  status: ResearchCandidateStatus;
  rejection_reason: string | null;
  notes: string | null;
  created_at: string;
  updated_at: string;
  correlation_id: string | null;
};

export type CreateResearchCandidateRequest = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  config: Record<string, unknown>;
  notes?: string | null;
  correlation_id?: string | null;
};

export type CreateResearchCandidateFromExperimentRunRequest = {
  experiment_run_id: string;
  notes?: string | null;
  correlation_id?: string | null;
};

export type ResearchCandidateLifecycleEvent = {
  id: string;
  candidate_id: string;
  previous_status: ResearchCandidateStatus | null;
  next_status: ResearchCandidateStatus;
  decision: ResearchCandidateDecision;
  reason: string | null;
  notes: string | null;
  actor_id: string | null;
  payload: Record<string, unknown>;
  created_at: string;
  correlation_id: string | null;
};

export type ResearchCandidateDecisionRequest = {
  decision: ResearchCandidateDecision;
  reason?: string | null;
  notes?: string | null;
  acknowledge_runner_mismatch?: boolean;
  acknowledge_overfit_risk?: boolean;
  correlation_id?: string | null;
};

export type ResearchCandidateWalkForwardEvidence = {
  walk_forward_run_id: string;
  robustness_status: StrategyWalkForwardRobustnessStatus;
  status: string;
  recommendation_action: string | null;
  recommendation_reason: string | null;
  total_windows: number;
  completed_windows: number;
  profitable_windows: number;
  losing_windows: number;
  avg_pnl_pct: string;
  worst_pnl_pct: string;
  best_pnl_pct: string;
  robustness_score: string;
  consistency_score: string;
  created_at: string;
  linked_at: string;
};

export type StrategyCandidateObservationStatus =
  | "OBSERVING"
  | "READY_FOR_REVIEW"
  | "FAILED"
  | "INSUFFICIENT_DATA"
  | "ARCHIVED";

export type StrategyCandidateObservationDecision =
  | "PASS"
  | "FAIL"
  | "CONTINUE_OBSERVING"
  | "INSUFFICIENT_DATA";

export type StrategyCandidateObservationFinding = {
  code: string;
  message: string;
  blocking: boolean;
};

export type StrategyCandidateRunnerAlignment = {
  strategy_config_matches_runner: boolean;
  runner_enabled: boolean;
  runner_status: string;
  runner_timeframe: string;
  runner_symbols: string[];
  runner_strategies: string[];
  mismatch_reasons: string[];
};

export type StrategyCandidateObservationRequirement = {
  candidate_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  min_observation_hours: number;
  min_shadow_runs: number;
  max_risk_rejection_rate: string | null;
  min_would_submit_count: number;
  max_no_signal_rate: string | null;
  require_readiness_ready: boolean;
};

export type StrategyCandidateObservationSummary = {
  candidate_id: string;
  window_start: string;
  window_end: string;
  shadow_runs: number;
  would_submit_count: number;
  no_signal_count: number;
  risk_rejected_count: number;
  skipped_count: number;
  risk_rejection_rate: string;
  no_signal_rate: string;
  latest_readiness_status: ExecutionReadinessStatus | null;
  latest_readiness_score: number | null;
  runner_alignment: StrategyCandidateRunnerAlignment;
  decision: StrategyCandidateObservationDecision;
  findings: StrategyCandidateObservationFinding[];
  recommendations: string[];
  created_at: string;
};

export type StrategyCandidateObservation = {
  observation_id: string;
  candidate_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  status: StrategyCandidateObservationStatus;
  requirements: StrategyCandidateObservationRequirement;
  runner_alignment: StrategyCandidateRunnerAlignment;
  summary: StrategyCandidateObservationSummary;
  decision: StrategyCandidateObservationDecision;
  started_at: string;
  evaluated_at: string;
  last_observed_at: string;
  observation_expires_at: string | null;
  observation_max_age_seconds: number | null;
  observation_snapshot_hash: string | null;
  runner_config_snapshot: Record<string, unknown> | null;
  readiness_snapshot: Record<string, unknown> | null;
  created_by: string | null;
  correlation_id: string | null;
};

export type ResearchCandidateObservationFreshnessStatus =
  | "FRESH"
  | "STALE"
  | "UNKNOWN";

export type ResearchCandidateObservationHistoryItem = {
  observation: StrategyCandidateObservation;
  freshness_status: ResearchCandidateObservationFreshnessStatus;
  observation_age_seconds: number | null;
  runner_config_drifted: boolean;
  accept_for_shadow_eligible: boolean;
};

export type ResearchCandidateObservationSummary = {
  candidate_id: string;
  total_observations: number;
  latest_observation_status: StrategyCandidateObservationStatus | null;
  latest_runner_alignment: StrategyCandidateRunnerAlignment | null;
  latest_readiness_status: ExecutionReadinessStatus | null;
  latest_recommendations: string[];
  stale_count: number;
  alignment_mismatch_count: number;
  runner_config_drift_count: number;
  last_observed_at: string | null;
  current_accept_for_shadow_eligible: boolean;
  current_accept_for_shadow_blockers: string[];
  computed_at: string;
};

export type StrategyCandidateObservationEvaluateRequest = {
  start_time?: string | null;
  min_observation_hours?: number;
  min_shadow_runs?: number;
  max_risk_rejection_rate?: string | null;
  min_would_submit_count?: number;
  max_no_signal_rate?: string | null;
  require_readiness_ready?: boolean;
  correlation_id?: string | null;
};

export type ResearchCandidatesResponse = {
  candidates: ResearchCandidate[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateResponse = {
  candidate: ResearchCandidate;
  walk_forward_evidence: ResearchCandidateWalkForwardEvidence | null;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateWalkForwardEvidenceResponse = {
  evidence: ResearchCandidateWalkForwardEvidence[];
  latest: ResearchCandidateWalkForwardEvidence | null;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateEventsResponse = {
  events: ResearchCandidateLifecycleEvent[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyCandidateObservationResponse = {
  observation: StrategyCandidateObservation;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyCandidateObservationsResponse = {
  observations: StrategyCandidateObservation[];
  history: ResearchCandidateObservationHistoryItem[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateObservationSummaryResponse = {
  summary: ResearchCandidateObservationSummary;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateQualificationStatus =
  | "QUALIFIED"
  | "NOT_QUALIFIED"
  | "NEEDS_MORE_DATA"
  | "DEGRADED"
  | "UNKNOWN";

export type ResearchCandidateQualificationSeverity =
  | "LOW"
  | "MEDIUM"
  | "HIGH"
  | "CRITICAL";

export type ResearchCandidateQualificationRecommendation =
  | "REFRESH_CANDIDATE_OBSERVATION"
  | "FIX_RUNNER_ALIGNMENT"
  | "EXPAND_SHADOW_RUNNER_COVERAGE"
  | "GATHER_MORE_SHADOW_RUNS"
  | "GENERATE_MORE_WOULD_SUBMIT_EVIDENCE"
  | "REVIEW_RISK_REJECTIONS"
  | "REDUCE_SHADOW_ERRORS_OR_SKIPS"
  | "RESTORE_TESTNET_SHADOW_READINESS"
  | "RE_ACCEPT_CANDIDATE_FOR_SHADOW"
  | "READY_FOR_TESTNET_PROMOTION_CONSIDERATION";

export type ResearchCandidateQualificationThresholds = {
  min_shadow_runs: number;
  min_would_submit_count: number;
  max_risk_rejection_rate_pct: string;
  max_error_or_skipped_rate_pct: string;
  max_runner_mismatch_count: number;
  require_fresh_observation: boolean;
  require_runner_alignment: boolean;
  require_readiness_not_not_ready: boolean;
};

export type ResearchCandidateQualificationCheck = {
  code: string;
  name: string;
  passed: boolean;
  blocking: boolean;
  severity: ResearchCandidateQualificationSeverity;
  summary: string;
  details?: Record<string, unknown> | null;
};

export type ResearchCandidateShadowPerformanceStatus =
  | "NOT_PROMOTED_TO_SHADOW_CONFIG"
  | "INSUFFICIENT_DATA"
  | "HEALTHY"
  | "UNDER_OBSERVATION"
  | "NEEDS_REVIEW";

export type ResearchCandidateShadowPerformanceRecommendation =
  | "PROMOTE_TO_SHADOW_CONFIG"
  | "INSUFFICIENT_DATA"
  | "KEEP_OBSERVING"
  | "NEEDS_REVIEW"
  | "CANDIDATE_NOT_COVERED_BY_RUNNER"
  | "REJECT_CANDIDATE";

export type ResearchCandidateShadowOutcomeBreakdown = {
  total_shadow_runs: number;
  would_submit_count: number;
  no_signal_count: number;
  risk_rejected_count: number;
  skipped_count: number;
  error_count: number;
  would_submit_rate_pct: string;
  risk_rejection_rate_pct: string;
};

export type ResearchCandidateShadowPerformance = {
  candidate_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  window_start: string;
  window_end: string;
  total_shadow_runs: number;
  would_submit_count: number;
  no_signal_count: number;
  risk_rejected_count: number;
  skipped_count: number;
  error_count: number;
  would_submit_rate_pct: string;
  risk_rejection_rate_pct: string;
  last_shadow_run_at: string | null;
  runner_alignment_current: boolean;
  recommendation: ResearchCandidateShadowPerformanceRecommendation;
  status: ResearchCandidateShadowPerformanceStatus;
  outcome_breakdown: ResearchCandidateShadowOutcomeBreakdown;
  computed_at: string;
};

export type ResearchCandidateShadowRunLink = {
  candidate_id: string;
  shadow_run_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  decision: string;
  status: string;
  signal_id: string | null;
  risk_decision_id: string | null;
  linked_at: string;
  shadow_created_at: string;
  correlation_id: string | null;
};

export type ResearchCandidateShadowPerformanceResponse = {
  performance: ResearchCandidateShadowPerformance;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateShadowRunsResponse = {
  runs: ResearchCandidateShadowRunLink[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchShadowPnlRecommendation =
  | "PROMISING"
  | "WEAK"
  | "NEGATIVE"
  | "INSUFFICIENT_DATA";

export type ResearchShadowPnlStatus =
  | "ATTRIBUTED"
  | "INSUFFICIENT_FORWARD_DATA"
  | "GAP_DETECTED"
  | "EXTREME_PNL";

export type ResearchShadowPnlTradeHoldingWindowResult = {
  holding_window: number;
  status: ResearchShadowPnlStatus;
  attribution_status: ResearchShadowPnlStatus;
  exit_candle_open_time: string | null;
  exit_candle_close_time: string | null;
  exit_price: string | null;
  gross_pnl_pct: string | null;
  fee_bps: string;
  slippage_bps: string;
  net_pnl_pct: string | null;
  fee_drag_pct: string;
  candle_gap_seconds: number | null;
  warning: string | null;
};

export type ResearchShadowPnlAttributionTrade = {
  candidate_id: string;
  shadow_run_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  shadow_created_at: string;
  signal_time: string | null;
  status: ResearchShadowPnlStatus;
  attribution_status: ResearchShadowPnlStatus;
  entry_candle_open_time: string | null;
  entry_candle_close_time: string | null;
  entry_price: string | null;
  holding_windows: ResearchShadowPnlTradeHoldingWindowResult[];
};

export type ResearchShadowPnlHoldingWindowResult = {
  holding_window: number;
  trade_count: number;
  win_rate: string;
  avg_net_pnl_pct: string;
  median_net_pnl_pct: string;
  best_net_pnl_pct: string;
  worst_net_pnl_pct: string;
  total_net_pnl_pct: string;
  fee_drag_pct: string;
  recommendation: ResearchShadowPnlRecommendation;
};

export type ResearchShadowPnlAttributionResult = {
  candidate_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  holding_windows: number[];
  fee_bps: string;
  slippage_bps: string;
  extreme_pnl_threshold_pct: string;
  summary: {
    total_attributed_runs: number;
    extreme_pnl_count: number;
    gap_detected_count: number;
    insufficient_forward_data_count: number;
    negative_all_windows: boolean;
    warnings: string[];
    per_holding_window: ResearchShadowPnlHoldingWindowResult[];
  };
  trades: ResearchShadowPnlAttributionTrade[];
  latest_shadow_pnl_status: ResearchShadowPnlRecommendation;
  best_holding_window: number | null;
  best_avg_net_pnl_pct: string | null;
  computed_at: string;
};

export type ResearchShadowPnlAttributionResponse = {
  attribution: ResearchShadowPnlAttributionResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateQualificationResult = {
  candidate_id: string;
  status: ResearchCandidateQualificationStatus;
  score: number;
  fresh_observation: boolean;
  runner_alignment_valid: boolean;
  latest_readiness_status: ExecutionReadinessStatus | null;
  walk_forward_status: StrategyWalkForwardRobustnessStatus | null;
  walk_forward_run_id: string | null;
  walk_forward_score: string | null;
  walk_forward_consistency_score: string | null;
  walk_forward_recommendation: string | null;
  walk_forward_blockers: string[];
  walk_forward_warnings: string[];
  readiness_penalty_points: number;
  threshold_override_below_default: boolean;
  threshold_override_penalty_points: number;
  score_explanation: string[];
  checks: ResearchCandidateQualificationCheck[];
  blockers: string[];
  warnings: string[];
  recommendations: ResearchCandidateQualificationRecommendation[];
  thresholds: ResearchCandidateQualificationThresholds;
  shadow_performance: ResearchCandidateShadowPerformance | null;
  computed_at: string;
};

export type ResearchCandidateQualificationResponse = {
  qualification: ResearchCandidateQualificationResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateQualificationTrend =
  | "IMPROVING"
  | "STABLE"
  | "DEGRADING"
  | "NEWLY_QUALIFIED"
  | "LOST_QUALIFICATION"
  | "NEEDS_ATTENTION"
  | "INSUFFICIENT_HISTORY";

export type ResearchCandidateWatchlistStatus =
  | "IMPROVING"
  | "STABLE"
  | "DEGRADING"
  | "NEWLY_QUALIFIED"
  | "LOST_QUALIFICATION"
  | "NEEDS_ATTENTION"
  | "INSUFFICIENT_HISTORY";

export type ResearchCandidateQualificationEvaluation = {
  id: string;
  candidate_id: string;
  status: ResearchCandidateQualificationStatus;
  score: number;
  latest_readiness_status: ExecutionReadinessStatus | null;
  total_shadow_runs: number;
  would_submit_count: number;
  risk_rejection_rate_pct: string | null;
  walk_forward_status: StrategyWalkForwardRobustnessStatus | null;
  walk_forward_run_id: string | null;
  walk_forward_score: string | null;
  walk_forward_consistency_score: string | null;
  walk_forward_recommendation: string | null;
  walk_forward_blockers: string[];
  walk_forward_warnings: string[];
  warnings: string[];
  blockers: string[];
  recommendations: ResearchCandidateQualificationRecommendation[];
  thresholds: ResearchCandidateQualificationThresholds;
  evaluated_at: string;
  correlation_id: string | null;
};

export type ResearchCandidateQualificationChange = {
  status_changed: boolean;
  material_score_change: boolean;
  newly_qualified: boolean;
  lost_qualification: boolean;
  previous_status: ResearchCandidateQualificationStatus | null;
  current_status: ResearchCandidateQualificationStatus;
  previous_score: number | null;
  current_score: number;
  score_delta: number;
};

export type ResearchCandidateQualificationHistory = {
  candidate_id: string;
  evaluations: ResearchCandidateQualificationEvaluation[];
  latest_change: ResearchCandidateQualificationChange | null;
  latest_trend: ResearchCandidateQualificationTrend;
};

export type ResearchCandidateWatchlistEntry = {
  candidate_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  candidate_status: ResearchCandidateStatus;
  latest_evaluation: ResearchCandidateQualificationEvaluation | null;
  walk_forward_evidence: ResearchCandidateWalkForwardEvidence | null;
  latest_change: ResearchCandidateQualificationChange | null;
  trend: ResearchCandidateQualificationTrend;
  watchlist_status: ResearchCandidateWatchlistStatus;
};

export type ResearchCandidateQualificationEvaluateResponse = {
  evaluation: ResearchCandidateQualificationEvaluation;
  change: ResearchCandidateQualificationChange | null;
  trend: ResearchCandidateQualificationTrend;
  qualification: ResearchCandidateQualificationResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateQualificationHistoryResponse = {
  history: ResearchCandidateQualificationHistory;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateTestnetReviewStatus =
  | "READY_FOR_REVIEW"
  | "NOT_READY"
  | "NEEDS_MORE_SHADOW_DATA"
  | "NEEDS_OPERATOR_REVIEW"
  | "BLOCKED";

export type ResearchCandidateTestnetReviewSection =
  | "IDENTITY"
  | "QUALIFICATION"
  | "SHADOW_PERFORMANCE"
  | "OBSERVATION"
  | "RUNNER_ALIGNMENT"
  | "READINESS"
  | "PROVENANCE"
  | "WALK_FORWARD"
  | "OPERATOR_REVIEW"
  | "CONTROLS";

export type ResearchCandidateTestnetReviewRecommendation =
  | "REFRESH_OBSERVATION"
  | "GATHER_MORE_SHADOW_DATA"
  | "RE_EVALUATE_QUALIFICATION"
  | "FIX_RUNNER_ALIGNMENT"
  | "CLEAR_READINESS_BLOCKERS"
  | "RECORD_READY_FOR_TESTNET_REVIEW"
  | "REVIEW_PRIVATE_STREAM_FRESHNESS"
  | "VERIFY_EXPERIMENT_PROVENANCE"
  | "MANUAL_OPERATOR_REVIEW";

export type ResearchCandidateTestnetReviewFinding = {
  section: ResearchCandidateTestnetReviewSection;
  code: string;
  summary: string;
  detail?: string | null;
  blocking: boolean;
};

export type ResearchCandidateTestnetReviewChecklist = {
  code: string;
  name: string;
  passed: boolean;
  summary: string;
};

export type ResearchCandidateTestnetReviewEvidence = {
  candidate_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  candidate_status: ResearchCandidateStatus | null;
  latest_review_action: ResearchCandidateReview | null;
  latest_qualification_evaluation: ResearchCandidateQualificationEvaluation | null;
  qualification_trend: ResearchCandidateQualificationTrend;
  shadow_performance_summary: ResearchCandidateShadowPerformance | null;
  latest_observation: StrategyCandidateObservation | null;
  observation_summary: ResearchCandidateObservationSummary | null;
  observation_freshness: ResearchCandidateObservationFreshnessStatus;
  observation_age_seconds: number | null;
  observation_expires_at: string | null;
  runner_alignment: StrategyCandidateRunnerAlignment | null;
  readiness_snapshot: ExecutionReadinessResult | Record<string, unknown> | null;
  source_label: string;
  provenance_available: boolean;
  provenance_notes: string[];
  candidate_score: string | null;
  candidate_pnl_pct: string | null;
  candidate_max_drawdown_pct: string | null;
  candidate_trade_count: number | null;
  candidate_win_rate: string | null;
  candidate_fee_drag: string | null;
  experiment_id: string | null;
  experiment_run_id: string | null;
  walk_forward_evidence: ResearchCandidateWalkForwardEvidence | null;
  operator_report_findings: ResearchCandidateTestnetReviewFinding[];
};

export type ResearchCandidateTestnetReviewDossier = {
  candidate_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  candidate_status: ResearchCandidateStatus | null;
  status: ResearchCandidateTestnetReviewStatus;
  evidence: ResearchCandidateTestnetReviewEvidence;
  checklist: ResearchCandidateTestnetReviewChecklist[];
  findings: ResearchCandidateTestnetReviewFinding[];
  blockers: string[];
  warnings: string[];
  recommendations: ResearchCandidateTestnetReviewRecommendation[];
  generated_at: string;
  correlation_id: string;
};

export type ResearchCandidateTestnetReviewDossierResponse = {
  dossier: ResearchCandidateTestnetReviewDossier;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateWatchlistResponse = {
  watchlist: ResearchCandidateWatchlistEntry[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateReviewAction =
  | "MARK_REVIEWED"
  | "MARK_NEEDS_MORE_OBSERVATION"
  | "MARK_READY_FOR_TESTNET_REVIEW"
  | "MARK_INVESTIGATED"
  | "REJECT_FROM_WATCHLIST"
  | "ARCHIVE_FROM_WATCHLIST";

export type ResearchCandidateReviewStatus =
  | "RECORDED"
  | "CANDIDATE_STATUS_UPDATED";

export type ResearchCandidateReview = {
  id: string;
  candidate_id: string;
  action: ResearchCandidateReviewAction;
  status: ResearchCandidateReviewStatus;
  previous_candidate_status: ResearchCandidateStatus;
  next_candidate_status: ResearchCandidateStatus | null;
  reason: string | null;
  notes: string | null;
  actor_id: string | null;
  created_at: string;
  correlation_id: string | null;
  qualification_evaluation_id: string | null;
};

export type ResearchCandidateReviewRequest = {
  action: ResearchCandidateReviewAction;
  reason?: string | null;
  notes?: string | null;
  qualification_evaluation_id?: string | null;
  correlation_id?: string | null;
};

export type ResearchCandidateReviewResult = {
  review: ResearchCandidateReview;
  candidate_status_before: ResearchCandidateStatus;
  candidate_status_after: ResearchCandidateStatus;
};

export type ResearchCandidateReviewsResponse = {
  reviews: ResearchCandidateReview[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateReviewResponse = {
  result: ResearchCandidateReviewResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateShadowPromotionMode = "PREVIEW_ONLY" | "APPLY";

export type ResearchCandidateShadowPromotionStatus =
  | "READY"
  | "BLOCKED"
  | "NO_CHANGES"
  | "APPLIED";

export type ResearchCandidateShadowPromotionRequest = {
  mode: ResearchCandidateShadowPromotionMode;
  allow_missing_runner_alignment?: boolean;
  confirmation_text?: string | null;
  correlation_id?: string | null;
};

export type ResearchCandidateShadowPromotionPreview = {
  candidate_id: string;
  candidate_status: ResearchCandidateStatus;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  current_runner_config: TestnetShadowRunnerConfig;
  proposed_runner_config: TestnetShadowRunnerConfig;
  changes: string[];
  status: ResearchCandidateShadowPromotionStatus;
  reasons: string[];
  confirmation_required: boolean;
  correlation_id: string;
  mode: ResearchCandidateShadowPromotionMode;
  allow_missing_runner_alignment: boolean;
};

export type ResearchCandidateShadowPromotionResult = {
  candidate_id: string;
  candidate_status: ResearchCandidateStatus;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  current_runner_config: TestnetShadowRunnerConfig;
  proposed_runner_config: TestnetShadowRunnerConfig;
  changes: string[];
  status: ResearchCandidateShadowPromotionStatus;
  reasons: string[];
  confirmation_required: boolean;
  correlation_id: string;
  mode: ResearchCandidateShadowPromotionMode;
  allow_missing_runner_alignment: boolean;
  applied: boolean;
};

export type ResearchCandidateShadowPromotionPreviewResponse = {
  preview: ResearchCandidateShadowPromotionPreview;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ResearchCandidateShadowPromotionResultResponse = {
  result: ResearchCandidateShadowPromotionResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
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
  trend_lookback_candles: number | null;
  momentum_lookback_candles: number | null;
  breakout_lookback_candles: number | null;
  lower_band_pct: string | null;
  upper_band_pct: string | null;
  min_range_width_pct: string | null;
  max_range_width_pct: string | null;
  min_close_above_sma_pct: string | null;
  max_close_above_sma_pct: string | null;
  min_momentum_return_pct: string | null;
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
  trend_lookback_candles?: number | null;
  momentum_lookback_candles?: number | null;
  breakout_lookback_candles?: number | null;
  lower_band_pct?: string | null;
  upper_band_pct?: string | null;
  min_range_width_pct?: string | null;
  max_range_width_pct?: string | null;
  min_close_above_sma_pct?: string | null;
  max_close_above_sma_pct?: string | null;
  min_momentum_return_pct?: string | null;
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

export type StrategyDiagnosticSeverity = "INFO" | "WARN" | "ERROR";

export type StrategyDiagnosticsDecision =
  | "WOULD_SIGNAL"
  | "NO_SIGNAL"
  | "INSUFFICIENT_DATA"
  | "STRATEGY_DISABLED"
  | "INVALID_CONFIG"
  | "STALE_DATA";

export type StrategyNoSignalReason =
  | "MOMENTUM_NOT_STRICTLY_HIGHER_CLOSES"
  | "TREND_CLOSE_NOT_ABOVE_SMA"
  | "TREND_MOMENTUM_NOT_POSITIVE"
  | "BREAKOUT_NOT_ABOVE_RECENT_HIGH"
  | "BREAKOUT_VOLUME_BELOW_AVERAGE"
  | "INSUFFICIENT_DATA"
  | "RANGE_TOO_NARROW"
  | "RANGE_TOO_WIDE"
  | "NOT_NEAR_LOWER_BAND"
  | "NO_REVERSAL_CONFIRMATION"
  | "CONFIDENCE_BELOW_FLOOR"
  | "INSUFFICIENT_CANDLES"
  | "STRATEGY_DISABLED"
  | "INVALID_CONFIG"
  | "STALE_DATA";

export type StrategyDiagnosticCheck = {
  name: string;
  passed: boolean;
  severity: StrategyDiagnosticSeverity;
  message: string;
  actual: string | null;
  expected: string | null;
};

export type StrategyDataHealth = {
  required_lookback_candles: number;
  required_closed_candles: number;
  available_closed_candles: number;
  latest_closed_candle_time: string | null;
  latest_closed_candle_age_ms: number | null;
  stale: boolean;
  latest_closes: string[];
};

export type StrategyDiagnosticsResult = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  strategy_enabled: boolean;
  config_valid: boolean;
  validation_issues: StrategyConfigValidationIssue[];
  data_health: StrategyDataHealth;
  condition_checks: StrategyDiagnosticCheck[];
  final_decision: StrategyDiagnosticsDecision;
  no_signal_reason: StrategyNoSignalReason | null;
  summary: string;
  source_candle_open_time: string | null;
  confidence: string | null;
  correlation_id: string;
  evaluated_at: string;
};

export type StrategyDiagnosticsResponse = {
  result: StrategyDiagnosticsResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyOpportunityStatus =
  | "HEALTHY_OPPORTUNITY"
  | "TOO_RESTRICTIVE"
  | "TOO_LOOSE"
  | "INSUFFICIENT_DATA"
  | "DATA_QUALITY_DEGRADED"
  | "UNKNOWN";

export type StrategyConditionPassRate = {
  condition: string;
  passed_count: number;
  failed_count: number;
  pass_rate_pct: string;
};

export type StrategyConditionFailureBreakdown = {
  condition: string;
  failed_count: number;
  failure_rate_pct: string;
};

export type StrategyOpportunityWindowExample = {
  source_candle_open_time: string;
  source_candle_close_time: string;
  would_signal: boolean;
  blocking_condition: string | null;
  details: unknown;
};

export type StrategyOpportunityAnalysisResult = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  total_closed_candles: number;
  evaluable_windows: number;
  would_signal_count: number;
  no_signal_count: number;
  signal_rate_pct: string;
  top_blocking_conditions: StrategyConditionFailureBreakdown[];
  condition_pass_rates: StrategyConditionPassRate[];
  condition_failure_breakdown: StrategyConditionFailureBreakdown[];
  example_pass_windows: StrategyOpportunityWindowExample[];
  example_fail_windows: StrategyOpportunityWindowExample[];
  distributions: unknown;
  recommendation: {
    status: StrategyOpportunityStatus;
    messages: string[];
  };
  data_quality_status: StrategyOpportunityStatus;
  analyzed_at: string;
};

export type StrategyOpportunityAnalysisResponse = {
  result: StrategyOpportunityAnalysisResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyExitAttributionStatus =
  | "PROMISING"
  | "WEAK"
  | "NEGATIVE"
  | "INSUFFICIENT_DATA"
  | "DATA_QUALITY_DEGRADED";

export type StrategyExitAttributionHoldingWindow = {
  holding_candles: number;
  trade_count: number;
  win_rate: string;
  avg_net_pnl_pct: string;
  median_net_pnl_pct: string;
  total_net_pnl_pct: string;
  best_net_pnl_pct: string;
  worst_net_pnl_pct: string;
  max_drawdown_pct: string | null;
  fee_drag_pct: string;
  recommendation: StrategyExitAttributionStatus;
};

export type StrategyExitAttributionResult = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  total_raw_signals: number;
  total_executable_signals: number;
  suppression_breakdown: ReplaySuppressionCount[];
  per_holding_window: StrategyExitAttributionHoldingWindow[];
  best_holding_window: number | null;
  worst_holding_window: number | null;
  status: StrategyExitAttributionStatus;
  recommendation: StrategyExitAttributionStatus;
  computed_at: string;
};

export type StrategyExitAttributionResponse = {
  result: StrategyExitAttributionResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategySignalFeatureAttributionStatus =
  | "PROMISING_FEATURES_FOUND"
  | "NO_PROMISING_FEATURES"
  | "INSUFFICIENT_DATA"
  | "DATA_QUALITY_DEGRADED";

export type StrategySignalFeatureRecommendation =
  | "PROMISING"
  | "WEAK"
  | "AVOID"
  | "INSUFFICIENT_SAMPLES";

export type StrategySignalFeatureMetric = {
  feature_name: string;
  value: string;
  bucket_label: string;
};

export type StrategySignalFeatureSample = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  signal_time: string;
  entry_candle_open_time: string;
  exit_candle_open_time: string;
  forward_net_pnl_pct: string;
  regime_label: string | null;
  hour_of_day_utc: number;
  day_of_week: string;
  metrics: StrategySignalFeatureMetric[];
};

export type StrategySignalFeatureBucket = {
  feature_name: string;
  bucket_label: string;
  sample_count: number;
  win_rate: string;
  avg_net_pnl_pct: string;
  median_net_pnl_pct: string;
  best_net_pnl_pct: string;
  worst_net_pnl_pct: string;
  total_net_pnl_pct: string;
  recommendation: StrategySignalFeatureRecommendation;
};

export type StrategySignalFeatureAttributionResult = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  holding_window: number;
  total_raw_signals: number;
  executable_signals: number;
  attributed_signals: number;
  insufficient_forward_data_count: number;
  suppression_breakdown: ReplaySuppressionCount[];
  feature_buckets: StrategySignalFeatureBucket[];
  best_buckets: StrategySignalFeatureBucket[];
  worst_buckets: StrategySignalFeatureBucket[];
  recommendations: string[];
  samples: StrategySignalFeatureSample[];
  status: StrategySignalFeatureAttributionStatus;
  computed_at: string;
};

export type StrategySignalFeatureAttributionResponse = {
  result: StrategySignalFeatureAttributionResult;
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

export type TestnetShadowPromotionStatus =
  | "PREVIEWED"
  | "SUBMITTED"
  | "REJECTED"
  | "EXPIRED"
  | "ALREADY_PROMOTED";

export type TestnetShadowPromotionPreview = {
  promotion_id: string;
  shadow_run_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  signal_id: string | null;
  risk_decision_id: string;
  would_submit_payload: TestnetShadowIntent;
  resolved_price: string | null;
  price_source: string | null;
  expires_at: string;
  reasons: string[];
  status: TestnetShadowPromotionStatus;
  correlation_id: string;
  created_at: string;
  submitted_at: string | null;
  testnet_order_id: string | null;
  client_order_id: string | null;
};

export type TestnetShadowPromotionResponse = {
  promotion: TestnetShadowPromotionPreview;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetShadowPromotionsResponse = {
  promotions: TestnetShadowPromotionPreview[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetShadowPromotionSubmitResult = {
  promotion_id: string;
  shadow_run_id: string;
  testnet_order_id: string;
  client_order_id: string;
  execution_state: string;
  correlation_id: string;
};

export type TestnetShadowPromotionSubmitResponse = {
  result: TestnetShadowPromotionSubmitResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetShadowRunnerConfig = {
  id: string;
  enabled: boolean;
  interval_seconds: number;
  strategies: string[];
  symbols: string[];
  timeframe: string;
  max_runs_per_tick: number;
  stale_feed_policy: string;
  notes: string | null;
  updated_by: string | null;
  updated_at: string;
};

export type TestnetShadowRunnerState = {
  id: string;
  status: string;
  last_tick_at: string | null;
  last_success_at: string | null;
  last_error: string | null;
  total_ticks: number;
  total_runs: number;
  updated_at: string;
};

export type TestnetShadowRunnerTickResult = {
  status: string;
  started_at: string;
  completed_at: string;
  scheduled: boolean;
  attempted_runs: number;
  completed_runs: number;
  failed_runs: number;
  correlation_id: string;
  message: string | null;
};

export type TestnetShadowRunnerStatusResponse = {
  config: TestnetShadowRunnerConfig;
  state: TestnetShadowRunnerState;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetShadowRunnerConfigResponse = {
  config: TestnetShadowRunnerConfig;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type TestnetShadowRunnerControlResponse = {
  state: TestnetShadowRunnerState;
  tick: TestnetShadowRunnerTickResult | null;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ScheduledResearchJobStatus =
  | "DISABLED"
  | "ENABLED"
  | "PAUSED"
  | "RUNNING"
  | "ERROR";

export type ScheduledResearchJobKind =
  | "PROVIDER_HEALTH"
  | "MARKET_DATA_QUALITY"
  | "AGGREGATION_STATUS"
  | "RESEARCH_BATCH"
  | "RESEARCH_CAMPAIGN"
  | "REGIME_DISCOVERY"
  | "ROBUSTNESS_MATRIX"
  | "OPERATOR_REPORT";

export type ScheduledResearchJobRunStatus =
  | "COMPLETED"
  | "FAILED"
  | "SKIPPED"
  | "PARTIAL_SUCCESS";

export type ScheduledResearchJob = {
  id: string;
  name: string;
  kind: ScheduledResearchJobKind;
  enabled: boolean;
  interval_seconds: number;
  request: Record<string, unknown>;
  max_runs_per_tick: number;
  last_run_at: string | null;
  next_run_at: string | null;
  status: ScheduledResearchJobStatus;
  created_at: string;
  updated_at: string;
};

export type ScheduledResearchJobRun = {
  id: string;
  job_id: string;
  status: ScheduledResearchJobRunStatus;
  started_at: string;
  completed_at: string | null;
  result: Record<string, unknown>;
  error: string | null;
  created_artifact_type: string | null;
  created_artifact_id: string | null;
  correlation_id: string | null;
};

export type ScheduledResearchJobRequest = {
  name: string;
  kind: ScheduledResearchJobKind;
  enabled: boolean;
  interval_seconds: number;
  request: Record<string, unknown>;
  max_runs_per_tick: number;
  next_run_at?: string | null;
};

export type ScheduledResearchJobsResponse = {
  jobs: ScheduledResearchJob[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ScheduledResearchJobResponse = {
  job: ScheduledResearchJob;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ScheduledResearchJobRunsResponse = {
  runs: ScheduledResearchJobRun[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type ScheduledResearchJobRunResponse = {
  run: ScheduledResearchJobRun;
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
  raw_signal_count?: number;
  cooldown_suppressed_count?: number;
  open_position_suppressed_count?: number;
  executed_trade_count?: number;
  suppression_breakdown?: ReplaySuppressionCount[];
  last_signal_time?: string | null;
  last_executed_entry_time?: string | null;
  correlation_id: string | null;
};

export type ReplaySuppressionReason =
  | "COOLDOWN_ACTIVE"
  | "POSITION_ALREADY_OPEN"
  | "INSUFFICIENT_FORWARD_DATA"
  | "INVALID_SIGNAL"
  | "NONE";

export type ReplaySuppressionCount = {
  reason: ReplaySuppressionReason;
  count: number;
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
  raw_signal_count?: number;
  cooldown_suppressed_count?: number;
  open_position_suppressed_count?: number;
  executed_trade_count?: number;
  suppression_breakdown?: ReplaySuppressionCount[];
  last_signal_time?: string | null;
  last_executed_entry_time?: string | null;
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

export type StrategyExperimentMetric =
  | "net_pnl_pct"
  | "max_drawdown_pct"
  | "trade_count"
  | "win_rate"
  | "fee_slippage_drag_pct"
  | "risk_adjusted_score";

export type StrategyExperimentStatus =
  | "PENDING"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED";

export type StrategyExperimentCandidate = {
  lookback_candles: number;
  trend_lookback_candles: number | null;
  momentum_lookback_candles: number | null;
  breakout_lookback_candles: number | null;
  holding_candles: number | null;
  stop_loss_pct: string | null;
  take_profit_pct: string | null;
  max_signal_age_ms: number | null;
};

export type StrategyExperimentRun = {
  id: string;
  experiment_id: string;
  rank: number;
  candidate: StrategyExperimentCandidate;
  final_equity: string;
  pnl: string;
  pnl_pct: string;
  max_drawdown_pct: string;
  win_rate: string;
  trade_count: number;
  fee_paid: string;
  slippage_cost: string;
  fee_slippage_drag_pct: string;
  raw_signal_count?: number;
  cooldown_suppressed_count?: number;
  open_position_suppressed_count?: number;
  executed_trade_count?: number;
  suppression_breakdown?: ReplaySuppressionCount[];
  last_signal_time?: string | null;
  last_executed_entry_time?: string | null;
  score: string;
  status: StrategyExperimentStatus;
  warnings: string[];
  created_at: string;
};

export type StrategyExperimentComparison = {
  ranking_metric: StrategyExperimentMetric;
  best_run_id: string | null;
  worst_run_id: string | null;
  ranked_run_ids: string[];
};

export type StrategyExperimentResult = {
  experiment_id: string;
  experiment_group_id: string | null;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  max_signal_age_ms: number | null;
  max_runs: number | null;
  status: StrategyExperimentStatus;
  run_count: number;
  comparison: StrategyExperimentComparison;
  best_run: StrategyExperimentRun | null;
  worst_run: StrategyExperimentRun | null;
  candle_count: number | null;
  warnings: string[];
  skipped_reason: string | null;
  created_at: string;
  correlation_id: string | null;
};

export type StrategyExperimentRequest = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  start_time: string;
  end_time: string;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  lookback_candidates: number[];
  trend_lookback_candidates?: number[] | null;
  momentum_lookback_candidates?: number[] | null;
  breakout_lookback_candidates?: number[] | null;
  lower_band_pct_candidates?: string[] | null;
  upper_band_pct_candidates?: string[] | null;
  min_range_width_pct_candidates?: string[] | null;
  max_range_width_pct_candidates?: string[] | null;
  min_close_above_sma_pct_candidates?: string[] | null;
  max_close_above_sma_pct_candidates?: string[] | null;
  min_momentum_return_pct_candidates?: string[] | null;
  holding_candles_candidates?: number[] | null;
  stop_loss_pct_candidates?: string[] | null;
  take_profit_pct_candidates?: string[] | null;
  max_signal_age_ms?: number | null;
  max_runs?: number | null;
  correlation_id?: string | null;
};

export type StrategyMultiTimeframeExperimentRequest = {
  strategy_id: string;
  symbol: string;
  timeframes: string[];
  start_time: string;
  end_time: string;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  lookback_candidates: number[];
  trend_lookback_candidates?: number[] | null;
  momentum_lookback_candidates?: number[] | null;
  breakout_lookback_candidates?: number[] | null;
  lower_band_pct_candidates?: string[] | null;
  upper_band_pct_candidates?: string[] | null;
  min_range_width_pct_candidates?: string[] | null;
  max_range_width_pct_candidates?: string[] | null;
  min_close_above_sma_pct_candidates?: string[] | null;
  max_close_above_sma_pct_candidates?: string[] | null;
  min_momentum_return_pct_candidates?: string[] | null;
  holding_candles_candidates?: number[] | null;
  stop_loss_pct_candidates?: string[] | null;
  take_profit_pct_candidates?: string[] | null;
  max_signal_age_ms?: number | null;
  max_runs?: number | null;
  correlation_id?: string | null;
};

export type StrategyExperimentRunAcceptedResponse = {
  experiment: StrategyExperimentResult;
  runs: StrategyExperimentRun[];
};

export type StrategyTimeframeCandidate = {
  timeframe: string;
  candle_count: number;
  required_candles: number;
};

export type StrategyTimeframeComparison = {
  candidate: StrategyTimeframeCandidate;
  experiment_id: string | null;
  status: StrategyExperimentStatus;
  run_count: number;
  best_run: StrategyExperimentRun | null;
  skipped_reason: string | null;
  warnings: string[];
};

export type StrategyExperimentGlobalRankingEntry = {
  timeframe: string;
  experiment_id: string;
  candle_count: number;
  required_candles: number;
  insufficient_data_penalty: string;
  overtrading_penalty: string;
  run: StrategyExperimentRun;
  warnings: string[];
};

export type StrategyExperimentGlobalRanking = {
  ranking_metric: StrategyExperimentMetric;
  best_run_id: string | null;
  ranked_runs: StrategyExperimentGlobalRankingEntry[];
};

export type StrategyMultiTimeframeExperimentResult = {
  experiment_group_id: string;
  strategy_id: string;
  symbol: string;
  requested_timeframes: string[];
  start_time: string;
  end_time: string;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  max_signal_age_ms: number | null;
  max_runs: number | null;
  status: StrategyExperimentStatus;
  timeframe_comparisons: StrategyTimeframeComparison[];
  global_ranking: StrategyExperimentGlobalRanking;
  warnings: string[];
  created_at: string;
  correlation_id: string | null;
};

export type StrategyMultiTimeframeExperimentAcceptedResponse = {
  comparison: StrategyMultiTimeframeExperimentResult;
};

export type StrategyExperimentsResponse = {
  experiments: StrategyExperimentResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyExperimentResponse = {
  experiment: StrategyExperimentResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyExperimentRunsResponse = {
  runs: StrategyExperimentRun[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyMultiTimeframeExperimentResponse = {
  comparison: StrategyMultiTimeframeExperimentResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyWalkForwardStatus =
  | "PENDING"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "SKIPPED";

export type StrategyWalkForwardRobustnessStatus =
  | "ROBUST"
  | "WEAK"
  | "OVERFIT_RISK"
  | "INSUFFICIENT_DATA"
  | "FAILED";

export type StrategyWalkForwardCandidate = {
  lookback_candles: number;
  trend_lookback_candles?: number | null;
  momentum_lookback_candles?: number | null;
  breakout_lookback_candles?: number | null;
  holding_candles: number | null;
  stop_loss_pct: string | null;
  take_profit_pct: string | null;
  max_signal_age_ms: number | null;
};

export type StrategyWalkForwardRequest = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  config?: Record<string, unknown> | null;
  experiment_run_id?: string | null;
  start_time: string;
  end_time: string;
  window_train_size_hours: number;
  window_test_size_hours: number;
  step_size_hours: number;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  candidate_config: StrategyWalkForwardCandidate;
  min_required_test_windows?: number | null;
  correlation_id?: string | null;
};

export type StrategyWalkForwardWindow = {
  window_index: number;
  train_start: string;
  train_end: string;
  test_start: string;
  test_end: string;
};

export type StrategyWalkForwardRobustnessSummary = {
  profitable_window_pct: string;
  total_trade_count: number;
  avg_trades_per_completed_window: string;
  avg_fee_slippage_drag_pct: string;
  skipped_window_pct: string;
  dominant_winner_share_pct: string;
  recommendation?: StrategyWalkForwardRecommendation;
};

export type StrategyWalkForwardRecommendation = {
  action: string;
  reason: string;
};

export type StrategyWalkForwardWindowResult = {
  id: string;
  walk_forward_id: string;
  window: StrategyWalkForwardWindow;
  status: StrategyWalkForwardStatus;
  skip_reason: string | null;
  trade_count: number;
  pnl: string;
  pnl_pct: string;
  max_drawdown_pct: string;
  win_rate: string;
  fee_paid: string;
  slippage_cost: string;
  raw_signal_count?: number;
  cooldown_suppressed_count?: number;
  open_position_suppressed_count?: number;
  executed_trade_count?: number;
  suppression_breakdown?: ReplaySuppressionCount[];
  last_signal_time?: string | null;
  last_executed_entry_time?: string | null;
  result: Record<string, unknown>;
  created_at: string;
};

export type StrategyWalkForwardResult = {
  walk_forward_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  total_windows: number;
  completed_windows: number;
  failed_windows?: number;
  skipped_windows: number;
  profitable_test_windows: number;
  profitable_windows?: number;
  losing_test_windows: number;
  losing_windows?: number;
  avg_test_pnl_pct: string;
  avg_pnl_pct?: string;
  median_test_pnl_pct: string;
  median_pnl_pct?: string;
  worst_test_pnl_pct: string;
  worst_pnl_pct?: string;
  best_test_pnl_pct: string;
  best_pnl_pct?: string;
  avg_max_drawdown_pct: string;
  max_drawdown_pct?: string;
  avg_trade_count?: string;
  robustness_score: string;
  consistency_score?: string;
  status: StrategyWalkForwardStatus;
  robustness_status: StrategyWalkForwardRobustnessStatus;
  robustness_summary: StrategyWalkForwardRobustnessSummary;
  recommendation: StrategyWalkForwardRecommendation;
  warnings: string[];
  created_at: string;
  correlation_id: string | null;
};

export type StrategyWalkForwardAcceptedResponse = {
  walk_forward: StrategyWalkForwardResult;
  windows: StrategyWalkForwardWindowResult[];
};

export type StrategyWalkForwardRunsResponse = {
  walk_forwards: StrategyWalkForwardResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyWalkForwardResponse = {
  walk_forward: StrategyWalkForwardResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyWalkForwardWindowsResponse = {
  windows: StrategyWalkForwardWindowResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyRobustnessMatrixStatus =
  | "ROBUST"
  | "PROMISING_BUT_WEAK"
  | "MIXED"
  | "OVERFIT_RISK"
  | "NEGATIVE"
  | "INSUFFICIENT_DATA"
  | "FAILED";

export type StrategyRobustnessMatrixFinding = {
  severity: string;
  code: string;
  message: string;
};

export type StrategyRobustnessMatrixRecommendation = {
  priority: string;
  code: string;
  message: string;
};

export type StrategyRobustnessMatrixRequest = {
  strategy_ids: string[];
  symbols: string[];
  timeframes: string[];
  windows?: { start_time: string; end_time: string }[];
  start_time?: string | null;
  end_time?: string | null;
  window_hours?: number | null;
  step_hours?: number | null;
  config_json_by_strategy?: Record<string, unknown> | null;
  experiment_run_id?: string | null;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  holding_candles?: number | null;
  min_trades_per_cell?: number;
  min_profitable_window_ratio?: string;
};

export type StrategyRobustnessMatrixCell = {
  id: string;
  matrix_run_id: string;
  strategy_id: string;
  symbol: string;
  timeframe: string;
  window_start: string;
  window_end: string;
  regime_label: ResearchRegimeLabel;
  data_quality_status: string;
  status: StrategyRobustnessMatrixStatus;
  pnl_pct: string;
  trade_count: number;
  raw_signal_count: number;
  executed_trade_count: number;
  cooldown_suppressed_count: number;
  win_rate: string;
  max_drawdown_pct: string;
  fee_drag: string;
  findings: StrategyRobustnessMatrixFinding[];
  created_at: string;
};

export type StrategyRobustnessMatrixStrategySummary = {
  strategy_id: string;
  status: StrategyRobustnessMatrixStatus;
  profitable_window_ratio: string;
  avg_pnl_pct: string;
  median_pnl_pct: string;
  worst_window_pnl_pct: string;
  best_window_pnl_pct: string;
  avg_trade_count: string;
  regime_consistency: string;
  data_quality_penalty: string;
  robustness_score: string;
  completed_cells: number;
  insufficient_data_cells: number;
  failed_cells: number;
  best_symbol: string | null;
  worst_symbol: string | null;
  best_regime: ResearchRegimeLabel | null;
  worst_regime: ResearchRegimeLabel | null;
  findings: StrategyRobustnessMatrixFinding[];
  recommendations: StrategyRobustnessMatrixRecommendation[];
};

export type StrategyRobustnessMatrixResult = {
  run_id: string;
  status: StrategyRobustnessMatrixStatus;
  request: StrategyRobustnessMatrixRequest;
  strategy_rankings: StrategyRobustnessMatrixStrategySummary[];
  findings: StrategyRobustnessMatrixFinding[];
  recommendations: StrategyRobustnessMatrixRecommendation[];
  cell_count: number;
  created_at: string;
};

export type StrategyRobustnessMatrixAcceptedResponse = {
  matrix: StrategyRobustnessMatrixResult;
  cells: StrategyRobustnessMatrixCell[];
};

export type StrategyRobustnessMatrixRunsResponse = {
  matrices: StrategyRobustnessMatrixResult[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyRobustnessMatrixResponse = {
  matrix: StrategyRobustnessMatrixResult;
  request_id: string;
  correlation_id: string;
  timestamp: string;
};

export type StrategyRobustnessMatrixCellsResponse = {
  cells: StrategyRobustnessMatrixCell[];
  request_id: string;
  correlation_id: string;
  timestamp: string;
};
