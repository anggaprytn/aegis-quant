"use client";

import {
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

import { api, getApiErrorPayload, getErrorMessage } from "@/lib/api";
import type {
  AuthUser,
  BacktestRequest,
  BacktestResult,
  BacktestRunAcceptedResponse,
  CandleAggregationRequest,
  CandleAggregationResult,
  CandleBackfillRequest,
  CandleBackfillResult,
  CandleCoverageSummary,
  MarketDataQualityReport,
  MarketDataQualityRequest,
  MarketDataRepairPlan,
  MarketDataRepairRunResult,
  ExecutionReadinessRequest,
  ExecutionReadinessResult,
  ExecutionReadinessSnapshot,
  ExecutionReadinessTarget,
  MarketFeedStatusRecord,
  OrderRecord,
  OperatorReport,
  OperatorReportRequest,
  OperatorReportListItem,
  PaperPositionRecord,
  ResearchDataCoverageResult,
  ResearchBatchRequest,
  ResearchBatchCandidateSummary,
  ResearchBatchResult,
  ResearchBatchTriage,
  ResearchCampaignBatchResult,
  ResearchCampaignFailureAttribution,
  ResearchCampaignRequest,
  ResearchCampaignResult,
  ResearchHypothesis,
  ResearchHypothesisPriority,
  ResearchHypothesisStatus,
  ResearchDatasetBuildRequest,
  ResearchDatasetBuildResult,
  ResearchRegimeCalibrationRequest,
  ResearchRegimeCalibrationResult,
  ResearchRegimeDatasetFromDiscoveryRequest,
  ResearchRegimeDatasetRequest,
  ResearchRegimeDatasetResult,
  ResearchRegimeDiscoveryCandidateWindow,
  ResearchRegimeDiscoveryRequest,
  ResearchRegimeDiscoveryResult,
  ResearchRegimeStrategyLeaderboard,
  ResearchRegimeStrategyStatus,
  ResearchRegimeWindow,
  ResearchCandidateObservationHistoryItem,
  ResearchCandidateQualificationResult,
  ResearchCandidateObservationSummary,
  ResearchCandidateShadowPerformance,
  ResearchCandidate as StrategyResearchCandidate,
  ResearchCandidateLifecycleEvent,
  ResearchCandidateQualificationHistory,
  ResearchCandidateQualificationTrend,
  ResearchCandidateQualificationChange,
  ResearchCandidateQualificationEvaluation,
  ResearchCandidateReview,
  ResearchCandidateReviewAction,
  ResearchCandidateTestnetReviewDossier,
  ResearchCandidateShadowPromotionPreview,
  ResearchCandidateShadowPromotionResult,
  ResearchCandidateShadowRunLink,
  ResearchCandidateWatchlistEntry,
  ResearchShadowPnlAttributionResult,
  ResearchCandidateStatus as StrategyResearchCandidateStatus,
  RiskConfig,
  RiskDecisionRecord,
  StrategyComparisonSummary,
  StrategyCandidateObservation,
  StrategyDiagnosticsResult,
  StrategyExperimentResult,
  StrategyExperimentRun,
  StrategyExitAttributionResult,
  StrategyMultiTimeframeExperimentAcceptedResponse,
  StrategyMultiTimeframeExperimentRequest,
  StrategyMultiTimeframeExperimentResult,
  StrategyOpportunityAnalysisResult,
  StrategySignalFeatureBucket,
  StrategySignalFeatureAttributionResult,
  StrategyWalkForwardAcceptedResponse,
  StrategyWalkForwardRequest,
  StrategyWalkForwardResult,
  StrategyWalkForwardWindowResult,
  StrategyRobustnessMatrixAcceptedResponse,
  StrategyRobustnessMatrixCell,
  StrategyRobustnessMatrixRequest,
  StrategyRobustnessMatrixResult,
  StrategyConfigUpdateRequest,
  StrategyDecisionBreakdown,
  StrategyPerformanceSummary,
  StrategyPnlBreakdown,
  StrategyStatusView,
  SystemEventRecord,
  TestnetPromotionFunnelRow,
  TestnetPromotionFunnelSummary,
  TestnetShadowRunResult,
  TestnetShadowRunnerConfig,
} from "@/lib/types";
import {
  cn,
  formatDateTime,
  formatNumber,
  formatRelativeAge,
  shortenId,
  toTitleCase,
} from "@/lib/utils";

type SectionId =
  | "command-center"
  | "market-data"
  | "strategies"
  | "risk"
  | "orders"
  | "analytics"
  | "reports"
  | "backtests"
  | "experiments"
  | "events"
  | "settings";

const SECTIONS: Array<{ id: SectionId; label: string }> = [
  { id: "command-center", label: "Command Center" },
  { id: "market-data", label: "Market Data" },
  { id: "strategies", label: "Strategies" },
  { id: "risk", label: "Risk" },
  { id: "orders", label: "Orders" },
  { id: "analytics", label: "Analytics" },
  { id: "reports", label: "Reports" },
  { id: "backtests", label: "Backtests" },
  { id: "experiments", label: "Experiments" },
  { id: "events", label: "Logs / Events" },
  { id: "settings", label: "Settings" },
];

const DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"];
const TIMEFRAME_OPTIONS = ["1m", "5m", "15m", "1h"];
const AGGREGATION_TARGET_OPTIONS = ["5m", "15m", "1h"];

const DEFAULT_BACKTEST_FORM: BacktestRequest = {
  strategy_id: "momentum_v1",
  symbol: "BTCUSDT",
  timeframe: "1m",
  start_time: "2026-05-01T00:00:00Z",
  end_time: "2026-05-02T00:00:00Z",
  initial_capital: "1000000",
  fee_bps: "10",
  slippage_bps: "5",
  holding_candles: 3,
};

function observationAgeSeconds(observation: StrategyCandidateObservation | null) {
  if (!observation) {
    return null;
  }
  return Math.max(
    0,
    Math.floor((Date.now() - new Date(observation.last_observed_at).getTime()) / 1000),
  );
}

function observationFreshnessState(observation: StrategyCandidateObservation | null) {
  if (!observation) {
    return "NOT_OBSERVED" as const;
  }
  const maxAge = observation.observation_max_age_seconds;
  const age = observationAgeSeconds(observation);
  if (maxAge === null || age === null) {
    return "UNKNOWN" as const;
  }
  return age <= maxAge ? "FRESH" as const : "STALE" as const;
}

function shadowRecommendationLabel(
  recommendation: ResearchCandidateShadowPerformance["recommendation"] | null | undefined,
) {
  switch (recommendation) {
    case "KEEP_OBSERVING":
      return "keep observing";
    case "NEEDS_REVIEW":
      return "needs review";
    case "INSUFFICIENT_DATA":
      return "insufficient data";
    case "PROMOTE_TO_SHADOW_CONFIG":
      return "promote to shadow runner config";
    case "CANDIDATE_NOT_COVERED_BY_RUNNER":
      return "candidate not covered by runner";
    case "REJECT_CANDIDATE":
      return "reject candidate";
    default:
      return "unknown";
  }
}

function qualificationRecommendationLabel(
  recommendation: ResearchCandidateQualificationResult["recommendations"][number],
) {
  switch (recommendation) {
    case "REFRESH_CANDIDATE_OBSERVATION":
      return "Refresh candidate observation";
    case "FIX_RUNNER_ALIGNMENT":
      return "Fix runner alignment";
    case "EXPAND_SHADOW_RUNNER_COVERAGE":
      return "Expand shadow runner coverage";
    case "GATHER_MORE_SHADOW_RUNS":
      return "Gather more shadow runs";
    case "GENERATE_MORE_WOULD_SUBMIT_EVIDENCE":
      return "Generate more WOULD_SUBMIT evidence";
    case "REVIEW_RISK_REJECTIONS":
      return "Review risk rejections";
    case "REDUCE_SHADOW_ERRORS_OR_SKIPS":
      return "Reduce shadow errors or skips";
    case "RESTORE_TESTNET_SHADOW_READINESS":
      return "Restore TESTNET_SHADOW readiness";
    case "RE_ACCEPT_CANDIDATE_FOR_SHADOW":
      return "Re-accept candidate for shadow";
    case "READY_FOR_TESTNET_PROMOTION_CONSIDERATION":
      return "Ready for testnet promotion consideration";
    default:
      return recommendation;
  }
}

function qualificationTrendLabel(trend: ResearchCandidateQualificationTrend): string {
  return trend.replaceAll("_", " ");
}

type StrategyExperimentFormState = {
  strategy_id: string;
  symbol: string;
  timeframes: string;
  start_time: string;
  end_time: string;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  lookbacks: string;
  trend_lookbacks: string;
  momentum_lookbacks: string;
  breakout_lookbacks: string;
  lower_band_pct: string;
  min_range_width_pct: string;
  max_range_width_pct: string;
  min_close_above_sma_pct: string;
  max_close_above_sma_pct: string;
  min_momentum_return_pct: string;
  holding_candles: string;
  stop_loss_pct: string;
  take_profit_pct: string;
  max_signal_age_ms: string;
  max_runs: string;
};

type StrategyWalkForwardFormState = {
  strategy_id: string;
  symbol: string;
  timeframe: string;
  experiment_run_id: string;
  config_json: string;
  start_time: string;
  end_time: string;
  train_hours: string;
  test_hours: string;
  step_hours: string;
  initial_capital: string;
  fee_bps: string;
  slippage_bps: string;
  lookback_candles: string;
  trend_lookback: string;
  momentum_lookback: string;
  breakout_lookback: string;
  holding_candles: string;
  stop_loss_pct: string;
  take_profit_pct: string;
  max_signal_age_ms: string;
  min_required_test_windows: string;
};

const DEFAULT_STRATEGY_EXPERIMENT_FORM: StrategyExperimentFormState = {
  strategy_id: "trend_filter_momentum_v2",
  symbol: "BTCUSDT",
  timeframes: "5m,15m",
  start_time: "2026-05-01T00:00:00Z",
  end_time: "2026-05-02T00:00:00Z",
  initial_capital: "1000000",
  fee_bps: "10",
  slippage_bps: "5",
  lookbacks: "10,20,50",
  trend_lookbacks: "10,20,50",
  momentum_lookbacks: "2,3,5",
  breakout_lookbacks: "",
  lower_band_pct: "10,20,30",
  min_range_width_pct: "0.15",
  max_range_width_pct: "3.0",
  min_close_above_sma_pct: "0",
  max_close_above_sma_pct: "0.5,1.0,1.5",
  min_momentum_return_pct: "0,0.1,0.2",
  holding_candles: "3,5,10",
  stop_loss_pct: "",
  take_profit_pct: "",
  max_signal_age_ms: "180000",
  max_runs: "12",
};

const DEFAULT_STRATEGY_WALK_FORWARD_FORM: StrategyWalkForwardFormState = {
  strategy_id: "trend_filter_momentum_v1",
  symbol: "BTCUSDT",
  timeframe: "15m",
  experiment_run_id: "",
  config_json: "",
  start_time: "2026-05-01T00:00:00Z",
  end_time: "2026-05-24T00:00:00Z",
  train_hours: "0",
  test_hours: "6",
  step_hours: "6",
  initial_capital: "1000000",
  fee_bps: "10",
  slippage_bps: "5",
  lookback_candles: "50",
  trend_lookback: "50",
  momentum_lookback: "2",
  breakout_lookback: "",
  holding_candles: "3",
  stop_loss_pct: "",
  take_profit_pct: "",
  max_signal_age_ms: "",
  min_required_test_windows: "3",
};

const DEFAULT_BACKFILL_FORM: CandleBackfillRequest = {
  exchange: "binance",
  symbol: "BTCUSDT",
  interval: "1m",
  start_time: "2026-05-01T00:00:00Z",
  end_time: "2026-05-02T00:00:00Z",
  limit_per_request: 1000,
};

const DEFAULT_AGGREGATION_FORM: CandleAggregationRequest = {
  exchange: "binance",
  symbol: "BTCUSDT",
  source_interval: "1m",
  target_interval: "5m",
  start_time: "2026-05-23T00:00:00Z",
  end_time: "2026-05-24T00:00:00Z",
};

const DEFAULT_MARKET_DATA_QUALITY_FORM: MarketDataQualityRequest = {
  exchange: "binance",
  symbol: "BTCUSDT",
  interval: "15m",
  start_time: "2024-05-01T00:00:00Z",
  end_time: "2024-05-06T00:00:00Z",
};

const DEFAULT_RESEARCH_DATA_FORM: ResearchDatasetBuildRequest = {
  exchange: "binance",
  symbol: "BTCUSDT",
  intervals: ["1m", "5m", "15m", "1h"],
  start_time: "2026-05-17T00:00:00Z",
  end_time: "2026-05-24T00:00:00Z",
  required_coverage_pct: "95",
};

const DEFAULT_RESEARCH_BATCH_FORM: ResearchBatchRequest = {
  strategy_id: "trend_filter_momentum_v2",
  symbol: "BTCUSDT",
  base_interval: "1m",
  target_intervals: ["5m", "15m", "1h"],
  start_time: "2026-05-23T00:00:00Z",
  end_time: "2026-05-24T00:00:00Z",
  initial_capital: "10000",
  fee_bps: "10",
  slippage_bps: "5",
  experiment_timeframes: ["5m", "15m"],
  lookback_candidates: [10, 20, 50],
  momentum_lookback_candidates: [2, 3, 5],
  min_close_above_sma_pct_candidates: ["0"],
  max_close_above_sma_pct_candidates: ["0.5", "1.0", "1.5"],
  min_momentum_return_pct_candidates: ["0", "0.1", "0.2"],
  holding_candles_candidates: [3, 5, 10],
  walk_forward_top_n: 3,
  repair_degraded_data: true,
  create_candidates: true,
  max_candidates: 3,
};

const DEFAULT_RESEARCH_CAMPAIGN_FORM: ResearchCampaignRequest = {
  strategies: ["trend_filter_momentum_v1", "trend_filter_momentum_v2", "range_reversion_v1"],
  symbols: ["BTCUSDT", "ETHUSDT"],
  experiment_timeframes: ["5m", "15m"],
  campaign_start: "2024-05-01T00:00:00Z",
  campaign_end: "2024-05-03T00:00:00Z",
  window_hours: 24,
  step_hours: 24,
  initial_capital: "1000000",
  fee_bps: "10",
  slippage_bps: "5",
  max_candidates_per_batch: 2,
  repair_degraded_data: true,
  walk_forward_top_n: 3,
  base_interval: "1m",
  lookback_candidates: [10, 20, 50],
  momentum_lookback_candidates: [2, 3, 5],
  min_close_above_sma_pct_candidates: ["0"],
  max_close_above_sma_pct_candidates: ["0.5", "1.0", "1.5"],
  min_momentum_return_pct_candidates: ["0", "0.1", "0.2"],
  lower_band_pct_candidates: ["10", "20", "30"],
  min_range_width_pct_candidates: ["0.15"],
  max_range_width_pct_candidates: ["3.0"],
};

const DEFAULT_RESEARCH_REGIME_DATASET_FORM: ResearchRegimeDatasetRequest = {
  symbol: "BTCUSDT",
  timeframe: "15m",
  start_time: "2024-01-01T00:00:00Z",
  end_time: "2024-02-01T00:00:00Z",
  window_hours: 24,
  step_hours: 12,
  min_candles_per_window: 80,
  target_regimes: ["TREND_UP", "TREND_DOWN", "RANGE", "HIGH_VOLATILITY", "LOW_VOLATILITY"],
  max_windows_per_regime: 20,
  require_good_data_quality: true,
};

const DEFAULT_RESEARCH_REGIME_DISCOVERY_FORM: ResearchRegimeDiscoveryRequest = {
  symbol: "BTCUSDT",
  timeframe: "15m",
  scan_start: "2024-01-01T00:00:00Z",
  scan_end: "2025-01-01T00:00:00Z",
  window_hours: 24,
  step_hours: 12,
  target_regimes: ["TREND_UP", "TREND_DOWN", "RANGE", "HIGH_VOLATILITY", "LOW_VOLATILITY"],
  max_windows_per_regime: 10,
  min_confidence: null,
  require_existing_candles: true,
  auto_backfill_missing: false,
};

const DEFAULT_RESEARCH_REGIME_CALIBRATION_FORM: ResearchRegimeCalibrationRequest = {
  symbol: "BTCUSDT",
  timeframe: "15m",
  scan_start: "2024-01-01T00:00:00Z",
  scan_end: "2025-01-01T00:00:00Z",
  window_hours: 24,
  step_hours: 12,
  threshold_candidates: null,
  target_min_windows_per_regime: 5,
};

const DEFAULT_REPORT_FORM: OperatorReportRequest = {
  start_time: "2026-05-24T00:00:00Z",
  end_time: "2026-05-24T23:59:59Z",
  symbol: "BTCUSDT",
  interval: "15m",
  strategy_id: "momentum_v1",
  format: "MARKDOWN",
  persist: false,
};

const DEFAULT_READINESS_FORM: ExecutionReadinessRequest = {
  target: "TESTNET_SUBMIT",
  symbol: "BTCUSDT",
  strategy_id: "momentum_v1",
  timeframe: "1m",
  persist: false,
};

function strategyConfigFormFromStatus(
  strategy?: StrategyStatusView,
): StrategyConfigUpdateRequest {
  return {
    strategy_id: strategy?.strategy_id ?? "momentum_v1",
    enabled: strategy?.enabled ?? true,
    mode: strategy?.mode ?? "paper",
    symbols: strategy?.symbols ?? ["BTCUSDT"],
    timeframe: strategy?.timeframe ?? "1m",
    suggested_notional: strategy?.suggested_notional ?? "100000",
    max_signal_age_ms: strategy?.max_signal_age_ms ?? 180000,
    cooldown_seconds: strategy?.cooldown_seconds ?? 900,
    lookback_candles: strategy?.lookback_candles ?? 3,
    trend_lookback_candles: strategy?.trend_lookback_candles ?? null,
    momentum_lookback_candles: strategy?.momentum_lookback_candles ?? null,
    breakout_lookback_candles: strategy?.breakout_lookback_candles ?? null,
    lower_band_pct: strategy?.lower_band_pct ?? null,
    upper_band_pct: strategy?.upper_band_pct ?? null,
    min_range_width_pct: strategy?.min_range_width_pct ?? null,
    max_range_width_pct: strategy?.max_range_width_pct ?? null,
    min_close_above_sma_pct: strategy?.min_close_above_sma_pct ?? null,
    max_close_above_sma_pct: strategy?.max_close_above_sma_pct ?? null,
    min_momentum_return_pct: strategy?.min_momentum_return_pct ?? null,
    confidence_floor: strategy?.confidence_floor ?? null,
    stop_loss_pct: strategy?.stop_loss_pct ?? null,
    take_profit_pct: strategy?.take_profit_pct ?? null,
    holding_candles: strategy?.holding_candles ?? 3,
    notes: strategy?.notes ?? "",
  };
}

function strategyDiagnosticsFormFromStatus(strategy?: StrategyStatusView) {
  return {
    symbol: strategy?.symbols[0] ?? "BTCUSDT",
    timeframe: strategy?.timeframe ?? "1m",
    limit: 20,
  };
}

function strategyOpportunityFormFromStatus(strategy?: StrategyStatusView) {
  const end = new Date();
  const start = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000);
  return {
    symbol: strategy?.symbols[0] ?? "BTCUSDT",
    timeframe: strategy?.timeframe ?? "15m",
    start_time: start.toISOString(),
    end_time: end.toISOString(),
    limit_samples: 5,
  };
}

function strategyExitAttributionFormFromStatus(strategy?: StrategyStatusView) {
  const end = new Date();
  const start = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000);
  return {
    symbol: strategy?.symbols[0] ?? "BTCUSDT",
    timeframe: strategy?.timeframe ?? "15m",
    start_time: start.toISOString(),
    end_time: end.toISOString(),
    experiment_run_id: "",
    holding_windows: "1,3,5,10,20",
    fee_bps: "10",
    slippage_bps: "5",
  };
}

function strategySignalFeatureAttributionFormFromStatus(strategy?: StrategyStatusView) {
  const end = new Date();
  const start = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000);
  return {
    symbol: strategy?.symbols[0] ?? "BTCUSDT",
    timeframe: strategy?.timeframe ?? "15m",
    start_time: start.toISOString(),
    end_time: end.toISOString(),
    experiment_run_id: "",
    holding_window: "5",
    fee_bps: "10",
    slippage_bps: "5",
    min_samples_per_bucket: "5",
  };
}

function parseIntegerList(value: string) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => Number(entry))
    .filter((entry) => Number.isFinite(entry) && entry > 0);
}

function parseStringList(value: string) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseDecimalList(value: string) {
  const values = value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  return values.length ? values : null;
}

function buildStrategyExperimentRequest(
  form: StrategyExperimentFormState,
): StrategyMultiTimeframeExperimentRequest {
  const holding = parseIntegerList(form.holding_candles);
  const request: StrategyMultiTimeframeExperimentRequest = {
    strategy_id: form.strategy_id,
    symbol: form.symbol,
    timeframes: form.timeframes
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean),
    start_time: form.start_time,
    end_time: form.end_time,
    initial_capital: form.initial_capital,
    fee_bps: form.fee_bps,
    slippage_bps: form.slippage_bps,
    lookback_candidates: parseIntegerList(form.lookbacks),
    trend_lookback_candidates: parseIntegerList(form.trend_lookbacks),
    momentum_lookback_candidates: parseIntegerList(form.momentum_lookbacks),
    breakout_lookback_candidates: parseIntegerList(form.breakout_lookbacks),
    lower_band_pct_candidates: parseDecimalList(form.lower_band_pct),
    min_range_width_pct_candidates: parseDecimalList(form.min_range_width_pct),
    max_range_width_pct_candidates: parseDecimalList(form.max_range_width_pct),
    min_close_above_sma_pct_candidates: parseDecimalList(form.min_close_above_sma_pct),
    max_close_above_sma_pct_candidates: parseDecimalList(form.max_close_above_sma_pct),
    min_momentum_return_pct_candidates: parseDecimalList(form.min_momentum_return_pct),
    holding_candles_candidates: holding.length ? holding : null,
    stop_loss_pct_candidates: parseDecimalList(form.stop_loss_pct),
    take_profit_pct_candidates: parseDecimalList(form.take_profit_pct),
    max_signal_age_ms: form.max_signal_age_ms ? Number(form.max_signal_age_ms) : null,
    max_runs: form.max_runs ? Number(form.max_runs) : null,
  };
  return request;
}

function buildStrategyWalkForwardRequest(
  form: StrategyWalkForwardFormState,
): StrategyWalkForwardRequest {
  return {
    strategy_id: form.strategy_id,
    symbol: form.symbol,
    timeframe: form.timeframe,
    config: form.config_json ? JSON.parse(form.config_json) : null,
    experiment_run_id: form.experiment_run_id || null,
    start_time: form.start_time,
    end_time: form.end_time,
    window_train_size_hours: Number(form.train_hours),
    window_test_size_hours: Number(form.test_hours),
    step_size_hours: Number(form.step_hours),
    initial_capital: form.initial_capital,
    fee_bps: form.fee_bps,
    slippage_bps: form.slippage_bps,
    candidate_config: {
      lookback_candles: Number(form.lookback_candles),
      trend_lookback_candles: form.trend_lookback ? Number(form.trend_lookback) : null,
      momentum_lookback_candles: form.momentum_lookback ? Number(form.momentum_lookback) : null,
      breakout_lookback_candles: form.breakout_lookback ? Number(form.breakout_lookback) : null,
      holding_candles: form.holding_candles ? Number(form.holding_candles) : null,
      stop_loss_pct: form.stop_loss_pct || null,
      take_profit_pct: form.take_profit_pct || null,
      max_signal_age_ms: form.max_signal_age_ms ? Number(form.max_signal_age_ms) : null,
    },
    min_required_test_windows: form.min_required_test_windows
      ? Number(form.min_required_test_windows)
      : null,
  };
}

function riskConfigFormFromView(config?: RiskConfig): RiskConfig {
  return {
    max_open_positions: config?.max_open_positions ?? 2,
    max_daily_loss_pct: config?.max_daily_loss_pct ?? "2",
    max_weekly_loss_pct: config?.max_weekly_loss_pct ?? "5",
    max_position_notional: config?.max_position_notional ?? "150000",
    max_slippage_pct: config?.max_slippage_pct ?? "1",
    max_consecutive_losses: config?.max_consecutive_losses ?? 3,
    cooldown_seconds: config?.cooldown_seconds ?? 900,
    max_signal_age_ms: config?.max_signal_age_ms ?? 5000,
    stale_feed_threshold_seconds: config?.stale_feed_threshold_seconds ?? 10,
  };
}

function shadowRunnerConfigFormFromView(
  config?: TestnetShadowRunnerConfig,
): Record<string, unknown> {
  return {
    enabled: config?.enabled ?? false,
    interval_seconds: config?.interval_seconds ?? 60,
    strategies: config?.strategies ?? ["momentum_v1"],
    symbols: config?.symbols ?? ["BTCUSDT"],
    timeframe: config?.timeframe ?? "1m",
    max_runs_per_tick: config?.max_runs_per_tick ?? 1,
    stale_feed_policy: config?.stale_feed_policy ?? "SKIP",
    notes: config?.notes ?? "",
  };
}

type TelemetrySnapshot = {
  reachable: boolean;
  killSwitchActive?: string;
  openPositions?: string;
  paperEquity?: string;
  maxFeedAgeSeconds?: string;
  raw: string;
};

function readMetricValue(metricsText: string, metricName: string) {
  const line = metricsText
    .split("\n")
    .find((entry) => entry.startsWith(`${metricName} `) && !entry.startsWith("#"));
  if (!line) {
    return undefined;
  }

  const parts = line.trim().split(/\s+/);
  return parts[parts.length - 1];
}

function sumMetricValues(metricsText: string, metricName: string) {
  const lines = metricsText
    .split("\n")
    .filter((entry) => entry.startsWith(`${metricName}{`));
  if (!lines.length) {
    return undefined;
  }

  let total = 0;
  for (const line of lines) {
    const parts = line.trim().split(/\s+/);
    const value = Number(parts[parts.length - 1]);
    if (Number.isFinite(value)) {
      total += value;
    }
  }

  return String(total);
}

function readMaxFeedAgeSeconds(metricsText: string) {
  const lines = metricsText
    .split("\n")
    .filter((entry) => entry.startsWith("aegis_market_feed_last_event_age_seconds{"));
  if (!lines.length) {
    return undefined;
  }

  let maxValue: number | undefined;
  for (const line of lines) {
    const parts = line.trim().split(/\s+/);
    const value = Number(parts[parts.length - 1]);
    if (Number.isFinite(value)) {
      maxValue = maxValue === undefined ? value : Math.max(maxValue, value);
    }
  }

  return maxValue === undefined ? undefined : String(maxValue);
}

export function DashboardApp() {
  const queryClient = useQueryClient();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const authQuery = useQuery({
    queryKey: ["auth-me"],
    queryFn: api.me,
    retry: false,
  });

  useEffect(() => {
    if (authQuery.error) {
      api.setAccessToken(null);
    }
  }, [authQuery.error]);

  const loginMutation = useMutation({
    mutationFn: api.login,
    onSuccess: (response) => {
      api.setAccessToken(response.access_token);
      queryClient.setQueryData(["auth-me"], { user: response.user });
      setPassword("");
    },
  });

  const logoutMutation = useMutation({
    mutationFn: api.logout,
    onSettled: async () => {
      api.setAccessToken(null);
      await queryClient.invalidateQueries({ queryKey: ["auth-me"] });
    },
  });

  if (authQuery.isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-transparent text-slate-100">
        <div className="rounded-2xl border border-border bg-panel/90 px-6 py-4 shadow-panel">
          Authenticating...
        </div>
      </div>
    );
  }

  if (!authQuery.data?.user) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-transparent px-4 text-slate-100">
        <form
          className="w-full max-w-md rounded-3xl border border-border bg-panel/95 p-6 shadow-panel"
          onSubmit={(event) => {
            event.preventDefault();
            loginMutation.mutate({ email, password });
          }}
        >
          <div className="text-xs uppercase tracking-[0.24em] text-muted">Aegis Quant</div>
          <h1 className="mt-3 text-2xl font-semibold">Operator Login</h1>
          <p className="mt-2 text-sm text-slate-300">
            Dashboard access requires an authenticated local operator session.
          </p>
          <div className="mt-6 space-y-4">
            <label className="block text-sm">
              <span className="mb-2 block text-slate-300">Email</span>
              <input
                className="w-full rounded-xl border border-border bg-surface/70 px-3 py-2 outline-none"
                type="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                required
              />
            </label>
            <label className="block text-sm">
              <span className="mb-2 block text-slate-300">Password</span>
              <input
                className="w-full rounded-xl border border-border bg-surface/70 px-3 py-2 outline-none"
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                required
                minLength={12}
              />
            </label>
          </div>
          {loginMutation.error ? (
            <div className="mt-4 rounded-xl border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200">
              {getErrorMessage(loginMutation.error)}
            </div>
          ) : null}
          <button
            className="mt-6 w-full rounded-xl border border-accent bg-accent/15 px-4 py-2 font-medium text-white transition hover:bg-accent/25 disabled:opacity-50"
            type="submit"
            disabled={loginMutation.isPending}
          >
            {loginMutation.isPending ? "Signing in..." : "Sign In"}
          </button>
        </form>
      </div>
    );
  }

  return (
    <AuthenticatedDashboard
      user={authQuery.data.user}
      onLogout={() => logoutMutation.mutate()}
      isLoggingOut={logoutMutation.isPending}
    />
  );
}

function AuthenticatedDashboard({
  user,
  onLogout,
  isLoggingOut,
}: {
  user: AuthUser;
  onLogout: () => void;
  isLoggingOut: boolean;
}) {
  const queryClient = useQueryClient();
  const [section, setSection] = useState<SectionId>("command-center");
  const [selectedSymbol, setSelectedSymbol] = useState("BTCUSDT");
  const [selectedStrategyId, setSelectedStrategyId] = useState("momentum_v1");
  const [pipelineStrategyId, setPipelineStrategyId] = useState("momentum_v1");
  const [pipelineSymbol, setPipelineSymbol] = useState("BTCUSDT");
  const [pipelineTimeframe, setPipelineTimeframe] = useState("1m");
  const [paperPositionStatus, setPaperPositionStatus] = useState("OPEN");
  const [closeTarget, setCloseTarget] = useState<PaperPositionRecord | null>(null);
  const [closeConfirmation, setCloseConfirmation] = useState("");
  const [closeReason, setCloseReason] = useState("manual_operator_exit");
  const [killSwitchReason, setKillSwitchReason] = useState("");
  const [resumeReason, setResumeReason] = useState("");
  const [resumeConfirmation, setResumeConfirmation] = useState("");
  const [testnetConfirmation, setTestnetConfirmation] = useState("");
  const [testnetRepairAction, setTestnetRepairAction] = useState("MANUAL_RECHECK");
  const [testnetRepairConfirmation, setTestnetRepairConfirmation] = useState("");
  const [testnetRepairReason, setTestnetRepairReason] = useState("");
  const [testnetRepairForce, setTestnetRepairForce] = useState(false);
  const [testnetSymbol, setTestnetSymbol] = useState("BTCUSDT");
  const [testnetSide, setTestnetSide] = useState("BUY");
  const [testnetOrderType, setTestnetOrderType] = useState("MARKET");
  const [testnetQuoteNotional, setTestnetQuoteNotional] = useState("10");
  const [testnetQuantity, setTestnetQuantity] = useState("");
  const [testnetLimitPrice, setTestnetLimitPrice] = useState("");
  const [testnetRiskDecisionId, setTestnetRiskDecisionId] = useState("");
  const [testnetPipelineRiskDecisionId, setTestnetPipelineRiskDecisionId] = useState("");
  const [testnetPipelineConfirmation, setTestnetPipelineConfirmation] = useState("");
  const [selectedTestnetOrderId, setSelectedTestnetOrderId] = useState<string | null>(null);
  const [testnetShadowStrategyId, setTestnetShadowStrategyId] = useState("momentum_v1");
  const [testnetShadowSymbol, setTestnetShadowSymbol] = useState("BTCUSDT");
  const [testnetShadowTimeframe, setTestnetShadowTimeframe] = useState("1m");
  const [shadowRunnerConfigForm, setShadowRunnerConfigForm] =
    useState<Record<string, unknown>>(shadowRunnerConfigFormFromView());
  const [selectedShadowRunId, setSelectedShadowRunId] = useState<string | null>(null);
  const [selectedShadowPromotionId, setSelectedShadowPromotionId] = useState<string | null>(null);
  const [shadowPromotionConfirmation, setShadowPromotionConfirmation] = useState("");
  const [privateStreamListenKey, setPrivateStreamListenKey] = useState("");
  const [eventTypeFilter, setEventTypeFilter] = useState("");
  const [eventSourceFilter, setEventSourceFilter] = useState("");
  const [eventCorrelationFilter, setEventCorrelationFilter] = useState("");
  const [selectedOrderId, setSelectedOrderId] = useState<string | null>(null);
  const [selectedRiskDecisionId, setSelectedRiskDecisionId] = useState<string | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedReconciliationRunId, setSelectedReconciliationRunId] = useState<string | null>(
    null,
  );
  const [reportForm, setReportForm] = useState<OperatorReportRequest>(DEFAULT_REPORT_FORM);
  const [readinessForm, setReadinessForm] =
    useState<ExecutionReadinessRequest>(DEFAULT_READINESS_FORM);
  const [lastReadinessResult, setLastReadinessResult] =
    useState<ExecutionReadinessResult | null>(null);
  const [generatedReport, setGeneratedReport] = useState<OperatorReport | null>(null);
  const [selectedReportId, setSelectedReportId] = useState<string | null>(null);
  const [backtestForm, setBacktestForm] =
    useState<BacktestRequest>(DEFAULT_BACKTEST_FORM);
  const [lastBacktestResult, setLastBacktestResult] =
    useState<BacktestRunAcceptedResponse | null>(null);
  const [strategyExperimentForm, setStrategyExperimentForm] =
    useState<StrategyExperimentFormState>(DEFAULT_STRATEGY_EXPERIMENT_FORM);
  const [lastStrategyExperimentResult, setLastStrategyExperimentResult] =
    useState<StrategyMultiTimeframeExperimentAcceptedResponse | null>(null);
  const [selectedExperimentId, setSelectedExperimentId] = useState<string | null>(null);
  const [strategyWalkForwardForm, setStrategyWalkForwardForm] =
    useState<StrategyWalkForwardFormState>(DEFAULT_STRATEGY_WALK_FORWARD_FORM);
  const [lastStrategyWalkForwardResult, setLastStrategyWalkForwardResult] =
    useState<StrategyWalkForwardAcceptedResponse | null>(null);
  const [selectedWalkForwardId, setSelectedWalkForwardId] = useState<string | null>(null);
  const [selectedResearchCandidateId, setSelectedResearchCandidateId] = useState<string | null>(
    null,
  );
  const [lastResearchCandidateObservation, setLastResearchCandidateObservation] =
    useState<StrategyCandidateObservation | null>(null);
  const [researchCandidateStrategyFilter, setResearchCandidateStrategyFilter] =
    useState("");
  const [researchCandidateSymbolFilter, setResearchCandidateSymbolFilter] =
    useState("");
  const [researchCandidateTimeframeFilter, setResearchCandidateTimeframeFilter] = useState("");
  const [researchCandidateStatusFilter, setResearchCandidateStatusFilter] =
    useState<StrategyResearchCandidateStatus | "">("");
  const [researchCandidateDecisionReason, setResearchCandidateDecisionReason] = useState("");
  const [researchCandidateReviewReason, setResearchCandidateReviewReason] = useState("");
  const [researchCandidateReviewNotes, setResearchCandidateReviewNotes] = useState("");
  const [researchShadowPnlHoldingWindows, setResearchShadowPnlHoldingWindows] =
    useState("1,3,5,10");
  const [researchCandidateShadowPromotionPreview, setResearchCandidateShadowPromotionPreview] =
    useState<ResearchCandidateShadowPromotionPreview | null>(null);
  const [researchCandidateShadowPromotionResult, setResearchCandidateShadowPromotionResult] =
    useState<ResearchCandidateShadowPromotionResult | null>(null);
  const [researchCandidateShadowPromotionConfirmation, setResearchCandidateShadowPromotionConfirmation] =
    useState("");
  const [researchCandidateAllowMissingRunnerAlignment, setResearchCandidateAllowMissingRunnerAlignment] =
    useState(false);
  const [backfillForm, setBackfillForm] =
    useState<CandleBackfillRequest>(DEFAULT_BACKFILL_FORM);
  const [aggregationForm, setAggregationForm] =
    useState<CandleAggregationRequest>(DEFAULT_AGGREGATION_FORM);
  const [marketDataQualityForm, setMarketDataQualityForm] =
    useState<MarketDataQualityRequest>(DEFAULT_MARKET_DATA_QUALITY_FORM);
  const [selectedBackfillRunId, setSelectedBackfillRunId] = useState<string | null>(null);
  const [lastBackfillResult, setLastBackfillResult] =
    useState<CandleBackfillResult | null>(null);
  const [lastAggregationResult, setLastAggregationResult] =
    useState<CandleAggregationResult | null>(null);
  const [lastMarketDataQualityReport, setLastMarketDataQualityReport] =
    useState<MarketDataQualityReport | null>(null);
  const [lastMarketDataRepairPlan, setLastMarketDataRepairPlan] =
    useState<MarketDataRepairPlan | null>(null);
  const [lastMarketDataRepairRun, setLastMarketDataRepairRun] =
    useState<MarketDataRepairRunResult | null>(null);
  const [researchDataForm, setResearchDataForm] =
    useState<ResearchDatasetBuildRequest>(DEFAULT_RESEARCH_DATA_FORM);
  const [selectedResearchBuildId, setSelectedResearchBuildId] = useState<string | null>(null);
  const [lastResearchCoverage, setLastResearchCoverage] =
    useState<ResearchDataCoverageResult | null>(null);
  const [lastResearchBuild, setLastResearchBuild] =
    useState<ResearchDatasetBuildResult | null>(null);
  const [researchRegimeDatasetForm, setResearchRegimeDatasetForm] =
    useState<ResearchRegimeDatasetRequest>(DEFAULT_RESEARCH_REGIME_DATASET_FORM);
  const [selectedResearchRegimeDatasetId, setSelectedResearchRegimeDatasetId] =
    useState<string | null>(null);
  const [lastResearchRegimeDataset, setLastResearchRegimeDataset] =
    useState<ResearchRegimeDatasetResult | null>(null);
  const [researchRegimeDiscoveryForm, setResearchRegimeDiscoveryForm] =
    useState<ResearchRegimeDiscoveryRequest>(DEFAULT_RESEARCH_REGIME_DISCOVERY_FORM);
  const [selectedResearchRegimeDiscoveryId, setSelectedResearchRegimeDiscoveryId] =
    useState<string | null>(null);
  const [lastResearchRegimeDiscovery, setLastResearchRegimeDiscovery] =
    useState<ResearchRegimeDiscoveryResult | null>(null);
  const [researchRegimeCalibrationForm, setResearchRegimeCalibrationForm] =
    useState<ResearchRegimeCalibrationRequest>(DEFAULT_RESEARCH_REGIME_CALIBRATION_FORM);
  const [lastResearchRegimeCalibration, setLastResearchRegimeCalibration] =
    useState<ResearchRegimeCalibrationResult | null>(null);
  const [selectedResearchRegimeCalibrationId, setSelectedResearchRegimeCalibrationId] =
    useState<string | null>(null);
  const [researchBatchForm, setResearchBatchForm] =
    useState<ResearchBatchRequest>(DEFAULT_RESEARCH_BATCH_FORM);
  const [selectedResearchBatchId, setSelectedResearchBatchId] = useState<string | null>(null);
  const [lastResearchBatch, setLastResearchBatch] = useState<ResearchBatchResult | null>(null);
  const [researchCampaignForm, setResearchCampaignForm] =
    useState<ResearchCampaignRequest>(DEFAULT_RESEARCH_CAMPAIGN_FORM);
  const [selectedResearchCampaignId, setSelectedResearchCampaignId] = useState<string | null>(null);
  const [lastResearchCampaign, setLastResearchCampaign] =
    useState<ResearchCampaignResult | null>(null);
  const [selectedResearchHypothesisId, setSelectedResearchHypothesisId] = useState<string | null>(null);
  const [researchHypothesisPriorityFilter, setResearchHypothesisPriorityFilter] =
    useState<ResearchHypothesisPriority | "ALL">("ALL");
  const [researchHypothesisStatusFilter, setResearchHypothesisStatusFilter] =
    useState<ResearchHypothesisStatus | "ALL">("ALL");
  const [strategyConfigForm, setStrategyConfigForm] =
    useState<StrategyConfigUpdateRequest>(strategyConfigFormFromStatus());
  const [strategyDiagnosticsForm, setStrategyDiagnosticsForm] = useState(
    strategyDiagnosticsFormFromStatus(),
  );
  const [strategyDiagnosticsResult, setStrategyDiagnosticsResult] =
    useState<StrategyDiagnosticsResult | null>(null);
  const [strategyOpportunityForm, setStrategyOpportunityForm] = useState(
    strategyOpportunityFormFromStatus(),
  );
  const [strategyOpportunityResult, setStrategyOpportunityResult] =
    useState<StrategyOpportunityAnalysisResult | null>(null);
  const [strategyExitAttributionForm, setStrategyExitAttributionForm] = useState(
    strategyExitAttributionFormFromStatus(),
  );
  const [strategyExitAttributionResult, setStrategyExitAttributionResult] =
    useState<StrategyExitAttributionResult | null>(null);
  const [strategySignalFeatureAttributionForm, setStrategySignalFeatureAttributionForm] = useState(
    strategySignalFeatureAttributionFormFromStatus(),
  );
  const [strategySignalFeatureAttributionResult, setStrategySignalFeatureAttributionResult] =
    useState<StrategySignalFeatureAttributionResult | null>(null);
  const [riskConfigForm, setRiskConfigForm] = useState<RiskConfig>(riskConfigFormFromView());

  useEffect(() => {
    setLastResearchCandidateObservation(null);
    setResearchCandidateShadowPromotionPreview(null);
    setResearchCandidateShadowPromotionResult(null);
    setResearchCandidateShadowPromotionConfirmation("");
    setResearchCandidateAllowMissingRunnerAlignment(false);
  }, [selectedResearchCandidateId]);

  const healthQuery = useQuery({
    queryKey: ["system-health"],
    queryFn: api.getSystemHealth,
    refetchInterval: 10_000,
  });
  const providerHealthQuery = useQuery({
    queryKey: ["market-provider-health"],
    queryFn: api.getMarketProviderHealth,
    refetchInterval: 30_000,
  });
  const statusQuery = useQuery({
    queryKey: ["system-status"],
    queryFn: api.getSystemStatus,
    refetchInterval: 10_000,
  });
  const riskQuery = useQuery({
    queryKey: ["risk-status"],
    queryFn: api.getRiskStatus,
    refetchInterval: 5_000,
  });
  const exchangeTestnetStatusQuery = useQuery({
    queryKey: ["exchange-testnet-status"],
    queryFn: api.getExchangeTestnetStatus,
    refetchInterval: 15_000,
  });
  const exchangePrivateStreamStatusQuery = useQuery({
    queryKey: ["exchange-private-stream-status"],
    queryFn: api.getExchangeTestnetPrivateStreamStatus,
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 10_000,
  });
  const exchangePrivateStreamEventsQuery = useQuery({
    queryKey: ["exchange-private-stream-events"],
    queryFn: () => api.getExchangeTestnetPrivateStreamEvents(20),
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 10_000,
  });
  const exchangeTestnetSymbolsQuery = useQuery({
    queryKey: ["exchange-testnet-symbols"],
    queryFn: api.getExchangeTestnetSymbols,
    refetchInterval: 60_000,
  });
  const exchangeTestnetBalancesQuery = useQuery({
    queryKey: ["exchange-testnet-balances"],
    queryFn: api.getExchangeTestnetBalances,
    enabled: user.role === "OWNER" || user.role === "OPERATOR",
    refetchInterval: 15_000,
  });
  const exchangeTestnetOrdersQuery = useQuery({
    queryKey: ["exchange-testnet-orders"],
    queryFn: () => api.getExchangeTestnetOrders(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR",
    refetchInterval: 10_000,
  });
  const exchangeTestnetShadowRunsQuery = useQuery({
    queryKey: ["exchange-testnet-shadow-runs"],
    queryFn: () => api.getExchangeTestnetShadowRuns(20),
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 10_000,
  });
  const exchangeTestnetShadowRunQuery = useQuery({
    queryKey: ["exchange-testnet-shadow-run", selectedShadowRunId],
    queryFn: () => api.getExchangeTestnetShadowRun(selectedShadowRunId ?? ""),
    enabled:
      (user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER") &&
      Boolean(selectedShadowRunId),
    refetchInterval: 10_000,
  });
  const exchangeTestnetShadowPromotionsQuery = useQuery({
    queryKey: ["exchange-testnet-shadow-promotions"],
    queryFn: () => api.getExchangeTestnetShadowPromotions(50),
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 10_000,
  });
  const exchangeTestnetShadowPromotionQuery = useQuery({
    queryKey: ["exchange-testnet-shadow-promotion", selectedShadowPromotionId],
    queryFn: () => api.getExchangeTestnetShadowPromotion(selectedShadowPromotionId ?? ""),
    enabled:
      (user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER") &&
      Boolean(selectedShadowPromotionId),
    refetchInterval: 10_000,
  });
  const exchangeTestnetShadowRunnerStatusQuery = useQuery({
    queryKey: ["exchange-testnet-shadow-runner-status"],
    queryFn: api.getExchangeTestnetShadowRunnerStatus,
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 10_000,
  });
  const exchangeTestnetShadowRunnerConfigQuery = useQuery({
    queryKey: ["exchange-testnet-shadow-runner-config"],
    queryFn: api.getExchangeTestnetShadowRunnerConfig,
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 10_000,
  });
  const exchangeTestnetLifecycleQuery = useQuery({
    queryKey: ["exchange-testnet-order-lifecycle", selectedTestnetOrderId],
    queryFn: () => api.getExchangeTestnetOrderLifecycle(selectedTestnetOrderId ?? ""),
    enabled:
      (user.role === "OWNER" || user.role === "OPERATOR") &&
      Boolean(selectedTestnetOrderId),
    refetchInterval: 10_000,
  });
  const exchangeTestnetRepairsQuery = useQuery({
    queryKey: ["exchange-testnet-order-repairs", selectedTestnetOrderId],
    queryFn: () => api.getExchangeTestnetOrderRepairs(selectedTestnetOrderId ?? ""),
    enabled: Boolean(selectedTestnetOrderId),
    refetchInterval: 10_000,
  });
  const exchangeReconciliationRunsQuery = useQuery({
    queryKey: ["exchange-reconciliation-runs"],
    queryFn: () => api.getExchangeReconciliationRuns(10),
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 10_000,
  });
  const symbolsQuery = useQuery({
    queryKey: ["market-symbols"],
    queryFn: api.getMarketSymbols,
    refetchInterval: 30_000,
  });
  const feedQuery = useQuery({
    queryKey: ["feed-status"],
    queryFn: api.getMarketFeedStatus,
    refetchInterval: 5_000,
  });
  const metricsQuery = useQuery({
    queryKey: ["metrics-text"],
    queryFn: api.getMetricsText,
    refetchInterval: 15_000,
  });
  const strategiesQuery = useQuery({
    queryKey: ["strategies"],
    queryFn: api.getStrategyList,
    refetchInterval: 10_000,
  });
  const signalsQuery = useQuery({
    queryKey: ["signals", selectedSymbol],
    queryFn: () => api.getRecentSignals(selectedSymbol, 20),
    refetchInterval: 10_000,
  });
  const ordersQuery = useQuery({
    queryKey: ["orders"],
    queryFn: api.getOrders,
    refetchInterval: 10_000,
  });
  const paperAccountQuery = useQuery({
    queryKey: ["paper-account"],
    queryFn: api.getPaperAccount,
    refetchInterval: 10_000,
  });
  const paperPositionsQuery = useQuery({
    queryKey: ["paper-positions", paperPositionStatus],
    queryFn: () => api.getPaperPositions(50, paperPositionStatus),
    refetchInterval: 10_000,
  });
  const paperPnlQuery = useQuery({
    queryKey: ["paper-pnl"],
    queryFn: api.getPaperPnl,
    refetchInterval: 10_000,
  });
  const paperEquityQuery = useQuery({
    queryKey: ["paper-equity"],
    queryFn: () => api.getPaperEquity(50),
    refetchInterval: 15_000,
  });
  const paperJournalQuery = useQuery({
    queryKey: ["paper-journal"],
    queryFn: () => api.getPaperTradeJournal(50),
    refetchInterval: 15_000,
  });
  const riskDecisionsQuery = useQuery({
    queryKey: ["risk-decisions", selectedSymbol],
    queryFn: () => api.getRiskDecisions(selectedSymbol, 50),
    refetchInterval: 10_000,
  });
  const latestRiskDecisionsQuery = useQuery({
    queryKey: ["risk-decisions-latest"],
    queryFn: () => api.getRiskDecisions(undefined, 20),
    refetchInterval: 10_000,
  });
  const riskConfigQuery = useQuery({
    queryKey: ["risk-config"],
    queryFn: api.getRiskConfig,
    refetchInterval: 10_000,
  });
  const riskConfigVersionsQuery = useQuery({
    queryKey: ["risk-config-versions"],
    queryFn: api.getRiskConfigVersions,
    refetchInterval: 10_000,
  });
  const riskConfigAuditQuery = useQuery({
    queryKey: ["risk-config-audit"],
    queryFn: api.getRiskConfigAudit,
    refetchInterval: 10_000,
  });
  const backtestRunsQuery = useQuery({
    queryKey: ["backtest-runs"],
    queryFn: () => api.getBacktestRuns(20),
    refetchInterval: 15_000,
  });
  const strategyExperimentsQuery = useQuery({
    queryKey: ["strategy-experiments"],
    queryFn: () => api.getStrategyExperiments(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const strategyWalkForwardsQuery = useQuery({
    queryKey: ["strategy-walk-forwards"],
    queryFn: () => api.getStrategyWalkForwards(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const researchCandidatesQuery = useQuery({
    queryKey: [
      "research-candidates",
      researchCandidateStrategyFilter,
      researchCandidateSymbolFilter,
      researchCandidateTimeframeFilter,
      researchCandidateStatusFilter,
    ],
    queryFn: () =>
      api.listResearchCandidates({
        strategy_id: researchCandidateStrategyFilter || undefined,
        symbol: researchCandidateSymbolFilter || undefined,
        timeframe: researchCandidateTimeframeFilter || undefined,
        status: researchCandidateStatusFilter || undefined,
        limit: 50,
      }),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateQuery = useQuery({
    queryKey: ["research-candidate", selectedResearchCandidateId],
    queryFn: () => api.getResearchCandidate(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
  });
  const selectedResearchCandidateEventsQuery = useQuery({
    queryKey: ["research-candidate-events", selectedResearchCandidateId],
    queryFn: () => api.listResearchCandidateEvents(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateReviewsQuery = useQuery({
    queryKey: ["research-candidate-reviews", selectedResearchCandidateId],
    queryFn: () => api.getResearchCandidateReviews(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateObservationQuery = useQuery({
    queryKey: ["research-candidate-observations", selectedResearchCandidateId],
    queryFn: () => api.listResearchCandidateObservations(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateObservationSummaryQuery = useQuery({
    queryKey: ["research-candidate-observation-summary", selectedResearchCandidateId],
    queryFn: () => api.getResearchCandidateObservationSummary(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateQualificationQuery = useQuery({
    queryKey: ["research-candidate-qualification", selectedResearchCandidateId],
    queryFn: () => api.getResearchCandidateQualification(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const researchCandidateWatchlistQuery = useQuery({
    queryKey: ["research-candidate-watchlist"],
    queryFn: () => api.getResearchCandidateWatchlist(50),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateQualificationHistoryQuery = useQuery({
    queryKey: ["research-candidate-qualification-history", selectedResearchCandidateId],
    queryFn: () =>
      api.getResearchCandidateQualificationHistory(selectedResearchCandidateId ?? "", 20),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateTestnetReviewDossierQuery = useQuery({
    queryKey: ["research-candidate-testnet-review-dossier", selectedResearchCandidateId],
    queryFn: () => api.getResearchCandidateTestnetReviewDossier(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateWalkForwardQuery = useQuery({
    queryKey: ["research-candidate-walk-forward", selectedResearchCandidateId],
    queryFn: () => api.getResearchCandidateWalkForward(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateShadowPerformanceQuery = useQuery({
    queryKey: ["research-candidate-shadow-performance", selectedResearchCandidateId],
    queryFn: () => api.getResearchCandidateShadowPerformance(selectedResearchCandidateId ?? ""),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateShadowPnlQuery = useQuery({
    queryKey: [
      "research-candidate-shadow-pnl-attribution",
      selectedResearchCandidateId,
      researchShadowPnlHoldingWindows,
    ],
    queryFn: () =>
      api.getResearchCandidateShadowPnlAttribution(selectedResearchCandidateId ?? "", {
        holding_windows: researchShadowPnlHoldingWindows,
        fee_bps: "10",
        slippage_bps: "5",
        limit: 100,
      }),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const selectedResearchCandidateShadowRunsQuery = useQuery({
    queryKey: ["research-candidate-shadow-runs", selectedResearchCandidateId],
    queryFn: () =>
      api.getResearchCandidateShadowRuns(selectedResearchCandidateId ?? "", { limit: 50 }),
    enabled: Boolean(selectedResearchCandidateId),
    refetchInterval: 15_000,
  });
  const eventsQuery = useQuery({
    queryKey: ["events", eventTypeFilter, eventSourceFilter, eventCorrelationFilter],
    queryFn: () =>
      api.getRecentEvents({
        limit: 100,
        event_type: eventTypeFilter || undefined,
        source: eventSourceFilter || undefined,
        correlation_id: eventCorrelationFilter || undefined,
      }),
    refetchInterval: 10_000,
  });

  const tickQueries = useQueries({
    queries: DEFAULT_SYMBOLS.map((symbol) => ({
      queryKey: ["latest-tick", symbol],
      queryFn: () => api.getLatestTick(symbol),
      refetchInterval: 5_000,
    })),
  });

  const candlesQuery = useQuery({
    queryKey: ["candles", selectedSymbol],
    queryFn: () => api.getMarketCandles(selectedSymbol, "1m", 25),
    refetchInterval: 10_000,
  });
  const candleCoverageQuery = useQuery({
    queryKey: ["candle-coverage", selectedSymbol],
    queryFn: () => api.getMarketCandleCoverage(selectedSymbol),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const researchBuildsQuery = useQuery({
    queryKey: ["research-builds"],
    queryFn: () => api.listResearchDatasetBuilds(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedResearchBuildQuery = useQuery({
    queryKey: ["research-build", selectedResearchBuildId],
    queryFn: () => api.getResearchDatasetBuild(selectedResearchBuildId ?? ""),
    enabled: Boolean(selectedResearchBuildId),
  });
  const researchRegimeDatasetsQuery = useQuery({
    queryKey: ["research-regime-datasets"],
    queryFn: () => api.listResearchRegimeDatasets(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedResearchRegimeDatasetQuery = useQuery({
    queryKey: ["research-regime-dataset", selectedResearchRegimeDatasetId],
    queryFn: () => api.getResearchRegimeDataset(selectedResearchRegimeDatasetId ?? ""),
    enabled: Boolean(selectedResearchRegimeDatasetId),
  });
  const selectedResearchRegimeDatasetWindowsQuery = useQuery({
    queryKey: ["research-regime-dataset-windows", selectedResearchRegimeDatasetId],
    queryFn: () => api.getResearchRegimeDatasetWindows(selectedResearchRegimeDatasetId ?? ""),
    enabled: Boolean(selectedResearchRegimeDatasetId),
  });
  const researchRegimeDiscoveriesQuery = useQuery({
    queryKey: ["research-regime-discoveries"],
    queryFn: () => api.listResearchRegimeDiscoveries(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const researchRegimeCalibrationsQuery = useQuery({
    queryKey: ["research-regime-calibrations"],
    queryFn: () => api.listResearchRegimeCalibrations(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedResearchRegimeCalibrationQuery = useQuery({
    queryKey: ["research-regime-calibration", selectedResearchRegimeCalibrationId],
    queryFn: () => api.getResearchRegimeCalibration(selectedResearchRegimeCalibrationId ?? ""),
    enabled: Boolean(selectedResearchRegimeCalibrationId),
  });
  const selectedResearchRegimeCalibrationCandidatesQuery = useQuery({
    queryKey: ["research-regime-calibration-candidates", selectedResearchRegimeCalibrationId],
    queryFn: () =>
      api.getResearchRegimeCalibrationCandidates(selectedResearchRegimeCalibrationId ?? ""),
    enabled: Boolean(selectedResearchRegimeCalibrationId),
  });
  const selectedResearchRegimeDiscoveryQuery = useQuery({
    queryKey: ["research-regime-discovery", selectedResearchRegimeDiscoveryId],
    queryFn: () => api.getResearchRegimeDiscovery(selectedResearchRegimeDiscoveryId ?? ""),
    enabled: Boolean(selectedResearchRegimeDiscoveryId),
  });
  const selectedResearchRegimeDiscoveryWindowsQuery = useQuery({
    queryKey: ["research-regime-discovery-windows", selectedResearchRegimeDiscoveryId],
    queryFn: () => api.getResearchRegimeDiscoveryWindows(selectedResearchRegimeDiscoveryId ?? ""),
    enabled: Boolean(selectedResearchRegimeDiscoveryId),
  });
  const researchBatchesQuery = useQuery({
    queryKey: ["research-batches"],
    queryFn: () => api.listResearchBatches(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedResearchBatchQuery = useQuery({
    queryKey: ["research-batch", selectedResearchBatchId],
    queryFn: () => api.getResearchBatch(selectedResearchBatchId ?? ""),
    enabled: Boolean(selectedResearchBatchId),
  });
  const selectedResearchBatchTriageQuery = useQuery({
    queryKey: ["research-batch-triage", selectedResearchBatchId],
    queryFn: () => api.getResearchBatchTriage(selectedResearchBatchId ?? ""),
    enabled: Boolean(selectedResearchBatchId),
  });
  const researchCampaignsQuery = useQuery({
    queryKey: ["research-campaigns"],
    queryFn: () => api.listResearchCampaigns(20),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedResearchCampaignQuery = useQuery({
    queryKey: ["research-campaign", selectedResearchCampaignId],
    queryFn: () => api.getResearchCampaign(selectedResearchCampaignId ?? ""),
    enabled: Boolean(selectedResearchCampaignId),
  });
  const selectedResearchCampaignFailureAttributionQuery = useQuery({
    queryKey: ["research-campaign-failure-attribution", selectedResearchCampaignId],
    queryFn: () => api.getResearchCampaignFailureAttribution(selectedResearchCampaignId ?? ""),
    enabled: Boolean(selectedResearchCampaignId),
  });
  const selectedResearchCampaignRegimeLeaderboardQuery = useQuery({
    queryKey: ["research-campaign-regime-leaderboard", selectedResearchCampaignId],
    queryFn: () => api.getResearchCampaignRegimeLeaderboard(selectedResearchCampaignId ?? ""),
    enabled: Boolean(selectedResearchCampaignId),
  });
  const researchHypothesesQuery = useQuery({
    queryKey: ["research-hypotheses"],
    queryFn: () => api.listResearchHypotheses(50),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const generateResearchHypothesesMutation = useMutation({
    mutationFn: () =>
      api.generateResearchHypotheses({
        campaign_id: selectedResearchCampaignId ?? undefined,
        persist: true,
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["research-hypotheses"] });
    },
  });
  const decideResearchHypothesisMutation = useMutation({
    mutationFn: ({ id, decision }: { id: string; decision: ResearchHypothesisStatus }) =>
      api.decideResearchHypothesis(id, { decision, reason: "dashboard review" }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["research-hypotheses"] });
    },
  });
  const backfillRunsQuery = useQuery({
    queryKey: ["backfill-runs"],
    queryFn: () => api.getMarketBackfillRuns(20),
    refetchInterval: 15_000,
  });
  const repairRunsQuery = useQuery({
    queryKey: ["market-data-repair-runs"],
    queryFn: () => api.getMarketDataRepairRuns(10),
    enabled: user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedBackfillRunQuery = useQuery({
    queryKey: ["backfill-run", selectedBackfillRunId],
    queryFn: () => api.getMarketBackfillRun(selectedBackfillRunId ?? ""),
    enabled: Boolean(selectedBackfillRunId),
  });

  const selectedStrategyStatusQuery = useQuery({
    queryKey: ["strategy-status", selectedStrategyId],
    queryFn: () => api.getStrategyConfig(selectedStrategyId),
    enabled: Boolean(selectedStrategyId),
  });
  const selectedAnalyticsTimeframe =
    selectedStrategyStatusQuery.data?.strategy.timeframe ?? "1m";
  const strategyPerformanceQuery = useQuery({
    queryKey: [
      "analytics-strategy-performance",
      selectedStrategyId,
      selectedSymbol,
      selectedAnalyticsTimeframe,
    ],
    queryFn: () =>
      api.getStrategyPerformance(
        "COMBINED",
        selectedStrategyId,
        selectedSymbol,
        selectedAnalyticsTimeframe,
      ),
    enabled: Boolean(selectedStrategyId),
    refetchInterval: 15_000,
  });
  const strategyRankingsQuery = useQuery({
    queryKey: ["analytics-strategy-rankings", selectedAnalyticsTimeframe],
    queryFn: () =>
      api.getStrategyPerformanceRankings(
        "COMBINED",
        undefined,
        selectedAnalyticsTimeframe,
        20,
      ),
    refetchInterval: 15_000,
  });
  const shadowRankingsQuery = useQuery({
    queryKey: ["analytics-shadow-rankings", selectedAnalyticsTimeframe],
    queryFn: () =>
      api.getStrategyPerformanceRankings(
        "SHADOW",
        undefined,
        selectedAnalyticsTimeframe,
        20,
      ),
    refetchInterval: 15_000,
  });
  const strategyDecisionBreakdownQuery = useQuery({
    queryKey: [
      "analytics-decision-breakdown",
      selectedStrategyId,
      selectedSymbol,
      selectedAnalyticsTimeframe,
    ],
    queryFn: () =>
      api.getStrategyDecisionBreakdown(
        selectedStrategyId,
        selectedSymbol,
        selectedAnalyticsTimeframe,
      ),
    enabled: Boolean(selectedStrategyId),
    refetchInterval: 15_000,
  });
  const strategyPaperPnlBreakdownQuery = useQuery({
    queryKey: [
      "analytics-paper-pnl-breakdown",
      selectedStrategyId,
      selectedSymbol,
      selectedAnalyticsTimeframe,
    ],
    queryFn: () =>
      api.getStrategyPaperPnlBreakdown(
        selectedStrategyId,
        selectedSymbol,
        selectedAnalyticsTimeframe,
      ),
    enabled: Boolean(selectedStrategyId),
    refetchInterval: 15_000,
  });
  const strategyBacktestBreakdownQuery = useQuery({
    queryKey: [
      "analytics-backtest-breakdown",
      selectedStrategyId,
      selectedSymbol,
      selectedAnalyticsTimeframe,
    ],
    queryFn: () =>
      api.getStrategyBacktestBreakdown(
        selectedStrategyId,
        selectedSymbol,
        selectedAnalyticsTimeframe,
      ),
    enabled: Boolean(selectedStrategyId),
    refetchInterval: 15_000,
  });
  const testnetPromotionFunnelQuery = useQuery({
    queryKey: [
      "analytics-testnet-promotion-funnel",
      selectedStrategyId,
      selectedSymbol,
      selectedAnalyticsTimeframe,
    ],
    queryFn: () =>
      api.getTestnetPromotionFunnel(
        selectedStrategyId,
        selectedSymbol,
        selectedAnalyticsTimeframe,
      ),
    enabled: Boolean(selectedStrategyId),
    refetchInterval: 15_000,
  });
  const testnetPromotionRowsQuery = useQuery({
    queryKey: [
      "analytics-testnet-promotion-rows",
      selectedStrategyId,
      selectedSymbol,
      selectedAnalyticsTimeframe,
    ],
    queryFn: () =>
      api.getTestnetPromotionRows(
        selectedStrategyId,
        selectedSymbol,
        selectedAnalyticsTimeframe,
        undefined,
        undefined,
        50,
      ),
    enabled: Boolean(selectedStrategyId),
    refetchInterval: 15_000,
  });
  const strategyConfigVersionsQuery = useQuery({
    queryKey: ["strategy-config-versions", selectedStrategyId],
    queryFn: () => api.getStrategyConfigVersions(selectedStrategyId),
    enabled: Boolean(selectedStrategyId),
  });
  const strategyConfigAuditQuery = useQuery({
    queryKey: ["strategy-config-audit", selectedStrategyId],
    queryFn: () => api.getStrategyConfigAudit(selectedStrategyId),
    enabled: Boolean(selectedStrategyId),
  });

  const selectedOrderQuery = useQuery({
    queryKey: ["order", selectedOrderId],
    queryFn: () => api.getOrder(selectedOrderId ?? ""),
    enabled: Boolean(selectedOrderId),
  });
  const operatorReportsQuery = useQuery({
    queryKey: ["operator-reports"],
    queryFn: () => api.getOperatorReports(20),
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });
  const selectedOperatorReportQuery = useQuery({
    queryKey: ["operator-report", selectedReportId],
    queryFn: () => api.getOperatorReport(selectedReportId ?? ""),
    enabled:
      (user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER") &&
      Boolean(selectedReportId),
  });
  const readinessSnapshotsQuery = useQuery({
    queryKey: ["execution-readiness-snapshots"],
    queryFn: () => api.getExecutionReadinessSnapshots(20),
    enabled:
      user.role === "OWNER" || user.role === "OPERATOR" || user.role === "VIEWER",
    refetchInterval: 15_000,
  });

  const selectedExchangeReconciliationRunQuery = useQuery({
    queryKey: ["exchange-reconciliation-run", selectedReconciliationRunId],
    queryFn: () => api.getExchangeReconciliationRun(selectedReconciliationRunId ?? ""),
    enabled: Boolean(selectedReconciliationRunId),
  });

  const selectedExchangeReconciliationMismatchesQuery = useQuery({
    queryKey: ["exchange-reconciliation-mismatches", selectedReconciliationRunId],
    queryFn: () =>
      api.getExchangeReconciliationMismatches(selectedReconciliationRunId ?? ""),
    enabled: Boolean(selectedReconciliationRunId),
  });

  const selectedRiskDecisionQuery = useQuery({
    queryKey: ["risk-decision", selectedRiskDecisionId],
    queryFn: () => api.getRiskDecision(selectedRiskDecisionId ?? ""),
    enabled: Boolean(selectedRiskDecisionId),
  });

  const selectedRunQuery = useQuery({
    queryKey: ["backtest-run", selectedRunId],
    queryFn: () => api.getBacktestRun(selectedRunId ?? ""),
    enabled: Boolean(selectedRunId),
  });
  const selectedExperimentQuery = useQuery({
    queryKey: ["strategy-experiment", selectedExperimentId],
    queryFn: () => api.getStrategyExperiment(selectedExperimentId ?? ""),
    enabled: Boolean(selectedExperimentId),
  });
  const selectedExperimentRunsQuery = useQuery({
    queryKey: ["strategy-experiment-runs", selectedExperimentId],
    queryFn: () => api.getStrategyExperimentRuns(selectedExperimentId ?? ""),
    enabled: Boolean(selectedExperimentId),
  });
  const selectedExperimentComparisonQuery = useQuery({
    queryKey: ["strategy-experiment-comparison", selectedExperimentQuery.data?.experiment?.experiment_group_id],
    queryFn: () =>
      api.getStrategyMultiTimeframeComparison(
        selectedExperimentQuery.data?.experiment?.experiment_group_id ?? "",
      ),
    enabled: Boolean(selectedExperimentQuery.data?.experiment?.experiment_group_id),
  });
  const selectedWalkForwardQuery = useQuery({
    queryKey: ["strategy-walk-forward", selectedWalkForwardId],
    queryFn: () => api.getStrategyWalkForward(selectedWalkForwardId ?? ""),
    enabled: Boolean(selectedWalkForwardId),
  });
  const selectedWalkForwardWindowsQuery = useQuery({
    queryKey: ["strategy-walk-forward-windows", selectedWalkForwardId],
    queryFn: () => api.getStrategyWalkForwardWindows(selectedWalkForwardId ?? ""),
    enabled: Boolean(selectedWalkForwardId),
  });

  const selectedRunTradesQuery = useQuery({
    queryKey: ["backtest-trades", selectedRunId],
    queryFn: () => api.getBacktestTrades(selectedRunId ?? ""),
    enabled: Boolean(selectedRunId),
  });

  const selectedRunEquityQuery = useQuery({
    queryKey: ["backtest-equity", selectedRunId],
    queryFn: () => api.getBacktestEquity(selectedRunId ?? ""),
    enabled: Boolean(selectedRunId),
  });

  useEffect(() => {
    const symbols = symbolsQuery.data?.symbols;
    if (symbols?.length) {
      if (!symbols.includes(selectedSymbol)) {
        setSelectedSymbol(symbols[0]);
      }
      if (!symbols.includes(pipelineSymbol)) {
        setPipelineSymbol(symbols[0]);
      }
      if (!symbols.includes(backfillForm.symbol)) {
        setBackfillForm((current) => ({ ...current, symbol: symbols[0] }));
      }
      if (!symbols.includes(aggregationForm.symbol)) {
        setAggregationForm((current) => ({ ...current, symbol: symbols[0] }));
      }
    }
  }, [
    aggregationForm.symbol,
    backfillForm.symbol,
    pipelineSymbol,
    selectedSymbol,
    symbolsQuery.data?.symbols,
  ]);

  useEffect(() => {
    const strategies = strategiesQuery.data?.strategies;
    if (strategies?.length) {
      if (!strategies.some((item) => item.strategy_id === selectedStrategyId)) {
        setSelectedStrategyId(strategies[0].strategy_id);
      }
      if (!strategies.some((item) => item.strategy_id === pipelineStrategyId)) {
        setPipelineStrategyId(strategies[0].strategy_id);
      }
    }
  }, [
    pipelineStrategyId,
    selectedStrategyId,
    strategiesQuery.data?.strategies,
  ]);

  useEffect(() => {
    if (selectedStrategyStatusQuery.data?.strategy) {
      setStrategyConfigForm(
        strategyConfigFormFromStatus(selectedStrategyStatusQuery.data.strategy),
      );
      setStrategyDiagnosticsForm(
        strategyDiagnosticsFormFromStatus(selectedStrategyStatusQuery.data.strategy),
      );
      setStrategyOpportunityForm(
        strategyOpportunityFormFromStatus(selectedStrategyStatusQuery.data.strategy),
      );
      setStrategyExitAttributionForm(
        strategyExitAttributionFormFromStatus(selectedStrategyStatusQuery.data.strategy),
      );
      setStrategySignalFeatureAttributionForm(
        strategySignalFeatureAttributionFormFromStatus(selectedStrategyStatusQuery.data.strategy),
      );
      setStrategyDiagnosticsResult(null);
      setStrategyOpportunityResult(null);
      setStrategySignalFeatureAttributionResult(null);
      setStrategyExitAttributionResult(null);
    }
  }, [selectedStrategyStatusQuery.data?.strategy]);

  useEffect(() => {
    if (riskConfigQuery.data?.config) {
      setRiskConfigForm(riskConfigFormFromView(riskConfigQuery.data.config));
    }
  }, [riskConfigQuery.data?.config]);

  useEffect(() => {
    if (exchangeTestnetShadowRunnerConfigQuery.data?.config) {
      setShadowRunnerConfigForm(
        shadowRunnerConfigFormFromView(exchangeTestnetShadowRunnerConfigQuery.data.config),
      );
    }
  }, [exchangeTestnetShadowRunnerConfigQuery.data?.config]);

  useEffect(() => {
    if (!selectedOrderId && ordersQuery.data?.orders[0]) {
      setSelectedOrderId(ordersQuery.data.orders[0].order_id);
    }
  }, [ordersQuery.data?.orders, selectedOrderId]);

  useEffect(() => {
    if (!selectedReconciliationRunId && exchangeReconciliationRunsQuery.data?.runs[0]) {
      setSelectedReconciliationRunId(exchangeReconciliationRunsQuery.data.runs[0].id);
    }
  }, [exchangeReconciliationRunsQuery.data?.runs, selectedReconciliationRunId]);

  useEffect(() => {
    if (!selectedRiskDecisionId && riskDecisionsQuery.data?.decisions[0]) {
      setSelectedRiskDecisionId(riskDecisionsQuery.data.decisions[0].id);
    }
  }, [riskDecisionsQuery.data?.decisions, selectedRiskDecisionId]);

  useEffect(() => {
    if (!selectedRunId && backtestRunsQuery.data?.runs[0]) {
      setSelectedRunId(backtestRunsQuery.data.runs[0].run_id);
    }
  }, [backtestRunsQuery.data?.runs, selectedRunId]);

  useEffect(() => {
    if (!selectedExperimentId && strategyExperimentsQuery.data?.experiments[0]) {
      setSelectedExperimentId(strategyExperimentsQuery.data.experiments[0].experiment_id);
    }
  }, [selectedExperimentId, strategyExperimentsQuery.data?.experiments]);

  useEffect(() => {
    if (!selectedWalkForwardId && strategyWalkForwardsQuery.data?.walk_forwards[0]) {
      setSelectedWalkForwardId(
        strategyWalkForwardsQuery.data.walk_forwards[0].walk_forward_id,
      );
    }
  }, [selectedWalkForwardId, strategyWalkForwardsQuery.data?.walk_forwards]);

  useEffect(() => {
    if (!selectedResearchCandidateId && researchCandidatesQuery.data?.candidates[0]) {
      setSelectedResearchCandidateId(researchCandidatesQuery.data.candidates[0].id);
    }
  }, [researchCandidatesQuery.data?.candidates, selectedResearchCandidateId]);

  useEffect(() => {
    if (!selectedBackfillRunId && backfillRunsQuery.data?.runs[0]) {
      setSelectedBackfillRunId(backfillRunsQuery.data.runs[0].run_id);
    }
  }, [backfillRunsQuery.data?.runs, selectedBackfillRunId]);

  useEffect(() => {
    if (!selectedResearchBuildId && researchBuildsQuery.data?.builds[0]) {
      setSelectedResearchBuildId(researchBuildsQuery.data.builds[0].build_id);
    }
  }, [researchBuildsQuery.data?.builds, selectedResearchBuildId]);

  useEffect(() => {
    if (!selectedResearchRegimeDatasetId && researchRegimeDatasetsQuery.data?.datasets[0]) {
      setSelectedResearchRegimeDatasetId(
        researchRegimeDatasetsQuery.data.datasets[0].dataset_id,
      );
    }
  }, [researchRegimeDatasetsQuery.data?.datasets, selectedResearchRegimeDatasetId]);

  useEffect(() => {
    if (!selectedResearchRegimeDiscoveryId && researchRegimeDiscoveriesQuery.data?.discoveries[0]) {
      setSelectedResearchRegimeDiscoveryId(
        researchRegimeDiscoveriesQuery.data.discoveries[0].discovery_id,
      );
    }
  }, [researchRegimeDiscoveriesQuery.data?.discoveries, selectedResearchRegimeDiscoveryId]);

  useEffect(() => {
    if (!selectedResearchRegimeCalibrationId && researchRegimeCalibrationsQuery.data?.calibrations[0]) {
      setSelectedResearchRegimeCalibrationId(
        researchRegimeCalibrationsQuery.data.calibrations[0].calibration_id,
      );
    }
  }, [researchRegimeCalibrationsQuery.data?.calibrations, selectedResearchRegimeCalibrationId]);

  useEffect(() => {
    if (!selectedResearchBatchId && researchBatchesQuery.data?.batches[0]) {
      setSelectedResearchBatchId(researchBatchesQuery.data.batches[0].batch_id);
    }
  }, [researchBatchesQuery.data?.batches, selectedResearchBatchId]);

  useEffect(() => {
    if (!selectedResearchCampaignId && researchCampaignsQuery.data?.campaigns[0]) {
      setSelectedResearchCampaignId(researchCampaignsQuery.data.campaigns[0].campaign_id);
    }
  }, [researchCampaignsQuery.data?.campaigns, selectedResearchCampaignId]);

  const refreshOperationalData = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["risk-status"] }),
      queryClient.invalidateQueries({ queryKey: ["risk-config"] }),
      queryClient.invalidateQueries({ queryKey: ["risk-config-versions"] }),
      queryClient.invalidateQueries({ queryKey: ["risk-config-audit"] }),
      queryClient.invalidateQueries({ queryKey: ["risk-decisions"] }),
      queryClient.invalidateQueries({ queryKey: ["orders"] }),
      queryClient.invalidateQueries({ queryKey: ["signals"] }),
      queryClient.invalidateQueries({ queryKey: ["backtest-runs"] }),
      queryClient.invalidateQueries({ queryKey: ["analytics-strategy-performance"] }),
      queryClient.invalidateQueries({ queryKey: ["analytics-strategy-rankings"] }),
      queryClient.invalidateQueries({ queryKey: ["analytics-shadow-rankings"] }),
      queryClient.invalidateQueries({ queryKey: ["analytics-decision-breakdown"] }),
      queryClient.invalidateQueries({ queryKey: ["analytics-paper-pnl-breakdown"] }),
      queryClient.invalidateQueries({ queryKey: ["analytics-backtest-breakdown"] }),
      queryClient.invalidateQueries({ queryKey: ["analytics-testnet-promotion-funnel"] }),
      queryClient.invalidateQueries({ queryKey: ["analytics-testnet-promotion-rows"] }),
      queryClient.invalidateQueries({ queryKey: ["events"] }),
      queryClient.invalidateQueries({ queryKey: ["feed-status"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-status"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-private-stream-status"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-private-stream-events"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-symbols"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-balances"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-orders"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-shadow-runs"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-shadow-run"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-shadow-promotions"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-shadow-promotion"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-shadow-runner-status"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-shadow-runner-config"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-order-lifecycle"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-testnet-order-repairs"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-reconciliation-runs"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-reconciliation-run"] }),
      queryClient.invalidateQueries({ queryKey: ["exchange-reconciliation-mismatches"] }),
      queryClient.invalidateQueries({ queryKey: ["backfill-runs"] }),
      queryClient.invalidateQueries({ queryKey: ["backfill-run"] }),
      queryClient.invalidateQueries({ queryKey: ["latest-tick"] }),
      queryClient.invalidateQueries({ queryKey: ["strategy-status"] }),
      queryClient.invalidateQueries({ queryKey: ["strategies"] }),
      queryClient.invalidateQueries({ queryKey: ["research-candidate-observations"] }),
    ]);
  };

  const selectedTestnetOrder =
    (exchangeTestnetOrdersQuery.data?.orders ?? []).find(
      (item) => item.client_order_id === selectedTestnetOrderId,
    ) ?? null;
  const selectedShadowRun: TestnetShadowRunResult | null =
    exchangeTestnetShadowRunQuery.data?.run ??
    (exchangeTestnetShadowRunsQuery.data?.runs ?? []).find(
      (item) => item.run_id === selectedShadowRunId,
    ) ??
    null;
  const selectedShadowPromotion =
    exchangeTestnetShadowPromotionQuery.data?.promotion ??
    (exchangeTestnetShadowPromotionsQuery.data?.promotions ?? []).find(
      (item) => item.promotion_id === selectedShadowPromotionId,
    ) ??
    null;
  const selectedTestnetOrderRepairable =
    selectedTestnetOrder !== null &&
    [
      "RECONCILIATION_REQUIRED",
      "UNKNOWN_EXCHANGE_STATE",
      "CANCEL_REQUESTED",
      "FAILED",
    ].includes(selectedTestnetOrder.execution_state);
  const repairConfirmationText =
    selectedTestnetOrder === null
      ? ""
      : testnetRepairAction === "SAFE_CANCEL_REQUEST"
        ? `CANCEL TESTNET ${selectedTestnetOrder.client_order_id}`
        : `REPAIR TESTNET ${selectedTestnetOrder.client_order_id}`;

  const killSwitchMutation = useMutation({
    mutationFn: () => api.activateKillSwitch(killSwitchReason || undefined),
    onSuccess: async () => {
      setKillSwitchReason("");
      await refreshOperationalData();
    },
  });

  const resumeMutation = useMutation({
    mutationFn: () =>
      api.resumeTrading(resumeConfirmation, resumeReason || undefined),
    onSuccess: async () => {
      setResumeReason("");
      setResumeConfirmation("");
      await refreshOperationalData();
    },
  });

  const pipelineMutation = useMutation({
    mutationFn: () =>
      api.runPaperPipeline({
        strategy_id: pipelineStrategyId,
        symbol: pipelineSymbol,
        timeframe: pipelineTimeframe,
      }),
    onSuccess: refreshOperationalData,
  });

  const evaluateMutation = useMutation({
    mutationFn: ({ strategyId, symbol }: { strategyId: string; symbol?: string }) =>
      api.evaluateStrategy(strategyId, symbol),
    onSuccess: refreshOperationalData,
  });

  const toggleStrategyMutation = useMutation({
    mutationFn: ({
      strategyId,
      enabled,
    }: {
      strategyId: string;
      enabled: boolean;
    }) =>
      enabled ? api.enableStrategy(strategyId) : api.disableStrategy(strategyId),
    onSuccess: refreshOperationalData,
  });

  const validateStrategyConfigMutation = useMutation({
    mutationFn: () => api.validateStrategyConfig(selectedStrategyId, strategyConfigForm),
  });

  const updateStrategyConfigMutation = useMutation({
    mutationFn: () => api.updateStrategyConfig(selectedStrategyId, strategyConfigForm),
    onSuccess: async () => {
      await refreshOperationalData();
      await queryClient.invalidateQueries({ queryKey: ["strategy-status", selectedStrategyId] });
      await queryClient.invalidateQueries({
        queryKey: ["strategy-config-versions", selectedStrategyId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["strategy-config-audit", selectedStrategyId],
      });
    },
  });

  const strategyDryRunMutation = useMutation({
    mutationFn: () =>
      api.dryRunStrategy(selectedStrategyId, {
        symbol: strategyConfigForm.symbols[0],
        timeframe: strategyConfigForm.timeframe,
        config_override: strategyConfigForm,
      }),
  });

  const strategyDiagnosticsMutation = useMutation({
    mutationFn: () =>
      api.getStrategyDiagnostics(selectedStrategyId, {
        symbol: strategyDiagnosticsForm.symbol,
        timeframe: strategyDiagnosticsForm.timeframe,
        limit: strategyDiagnosticsForm.limit,
      }),
    onSuccess: (response) => {
      setStrategyDiagnosticsResult(response.result);
    },
  });

  const strategyOpportunityMutation = useMutation({
    mutationFn: () =>
      api.getStrategyOpportunityAnalysis(selectedStrategyId, {
        symbol: strategyOpportunityForm.symbol,
        timeframe: strategyOpportunityForm.timeframe,
        start_time: strategyOpportunityForm.start_time,
        end_time: strategyOpportunityForm.end_time,
        limit_samples: strategyOpportunityForm.limit_samples,
        include_examples: "true",
      }),
    onSuccess: (response) => {
      setStrategyOpportunityResult(response.result);
    },
  });

  const strategyExitAttributionMutation = useMutation({
    mutationFn: () =>
      api.getStrategyExitAttribution(selectedStrategyId, {
        symbol: strategyExitAttributionForm.symbol,
        timeframe: strategyExitAttributionForm.timeframe,
        start_time: strategyExitAttributionForm.start_time,
        end_time: strategyExitAttributionForm.end_time,
        experiment_run_id: strategyExitAttributionForm.experiment_run_id || undefined,
        holding_windows: strategyExitAttributionForm.holding_windows,
        fee_bps: strategyExitAttributionForm.fee_bps,
        slippage_bps: strategyExitAttributionForm.slippage_bps,
      }),
    onSuccess: (response) => {
      setStrategyExitAttributionResult(response.result);
    },
  });

  const strategySignalFeatureAttributionMutation = useMutation({
    mutationFn: () =>
      api.getStrategySignalFeatureAttribution(selectedStrategyId, {
        symbol: strategySignalFeatureAttributionForm.symbol,
        timeframe: strategySignalFeatureAttributionForm.timeframe,
        start_time: strategySignalFeatureAttributionForm.start_time,
        end_time: strategySignalFeatureAttributionForm.end_time,
        experiment_run_id: strategySignalFeatureAttributionForm.experiment_run_id || undefined,
        holding_window: strategySignalFeatureAttributionForm.holding_window,
        fee_bps: strategySignalFeatureAttributionForm.fee_bps,
        slippage_bps: strategySignalFeatureAttributionForm.slippage_bps,
        min_samples_per_bucket: strategySignalFeatureAttributionForm.min_samples_per_bucket,
      }),
    onSuccess: (response) => {
      setStrategySignalFeatureAttributionResult(response.result);
    },
  });

  const validateRiskConfigMutation = useMutation({
    mutationFn: () => api.validateRiskConfig(riskConfigForm),
  });

  const updateRiskConfigMutation = useMutation({
    mutationFn: () => api.updateRiskConfig(riskConfigForm),
    onSuccess: async () => {
      await refreshOperationalData();
      await queryClient.invalidateQueries({ queryKey: ["risk-config"] });
      await queryClient.invalidateQueries({ queryKey: ["risk-config-versions"] });
      await queryClient.invalidateQueries({ queryKey: ["risk-config-audit"] });
    },
  });

  const runBacktestMutation = useMutation({
    mutationFn: () => api.runBacktest(backtestForm),
    onSuccess: async (result) => {
      setLastBacktestResult(result);
      setSelectedRunId(result.run_id);
      await refreshOperationalData();
    },
  });
  const runStrategyExperimentMutation = useMutation({
    mutationFn: () =>
      api.runMultiTimeframeStrategyExperiment(
        buildStrategyExperimentRequest(strategyExperimentForm),
      ),
    onSuccess: async (response) => {
      setLastStrategyExperimentResult(response);
      setSelectedExperimentId(
        response.comparison.timeframe_comparisons.find((item) => item.experiment_id)?.experiment_id ??
          null,
      );
      await queryClient.invalidateQueries({ queryKey: ["strategy-experiments"] });
      await queryClient.invalidateQueries({
        queryKey: ["strategy-experiment-comparison", response.comparison.experiment_group_id],
      });
    },
  });
  const runStrategyWalkForwardMutation = useMutation({
    mutationFn: () =>
      api.runStrategyWalkForward(
        buildStrategyWalkForwardRequest(strategyWalkForwardForm),
      ),
    onSuccess: async (response) => {
      setLastStrategyWalkForwardResult(response);
      setSelectedWalkForwardId(response.walk_forward.walk_forward_id);
      await queryClient.invalidateQueries({ queryKey: ["strategy-walk-forwards"] });
      await queryClient.invalidateQueries({
        queryKey: ["strategy-walk-forward", response.walk_forward.walk_forward_id],
      });
      await queryClient.invalidateQueries({
        queryKey: ["strategy-walk-forward-windows", response.walk_forward.walk_forward_id],
      });
    },
  });
  const linkResearchCandidateWalkForwardMutation = useMutation({
    mutationFn: async () => {
      if (!selectedResearchCandidateId || !selectedWalkForward?.walk_forward_id) {
        throw new Error("Select a candidate and walk-forward run first.");
      }
      return api.linkResearchCandidateWalkForward(
        selectedResearchCandidateId,
        selectedWalkForward.walk_forward_id,
      );
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-walk-forward", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-qualification", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-testnet-review-dossier", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({ queryKey: ["research-candidate-watchlist"] });
    },
  });
  const decideResearchCandidateMutation = useMutation({
    mutationFn: async (decision: "ACCEPT_FOR_SHADOW" | "REJECT" | "ARCHIVE" | "REOPEN") => {
      if (!selectedResearchCandidateId) {
        throw new Error("Select a candidate first.");
      }
      return api.decideResearchCandidate(selectedResearchCandidateId, {
        decision,
        reason: researchCandidateDecisionReason || undefined,
        acknowledge_runner_mismatch: decision === "ACCEPT_FOR_SHADOW",
        acknowledge_overfit_risk: false,
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["research-candidates"] });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-events", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-observations", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-observation-summary", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-qualification", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-shadow-performance", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-shadow-runs", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-testnet-review-dossier", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-walk-forward", selectedResearchCandidateId],
      });
      setResearchCandidateDecisionReason("");
    },
  });
  const reviewResearchCandidateMutation = useMutation({
    mutationFn: async (action: ResearchCandidateReviewAction) => {
      if (!selectedResearchCandidateId) {
        throw new Error("Select a candidate first.");
      }
      return api.createResearchCandidateReview(selectedResearchCandidateId, {
        action,
        reason: researchCandidateReviewReason || undefined,
        notes: researchCandidateReviewNotes || undefined,
        qualification_evaluation_id: latestQualificationEvaluation?.id ?? undefined,
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["research-candidates"] });
      await queryClient.invalidateQueries({ queryKey: ["research-candidate-watchlist"] });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-events", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-reviews", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-qualification", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-qualification-history", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-testnet-review-dossier", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({ queryKey: ["operator-reports"] });
      setResearchCandidateReviewReason("");
      setResearchCandidateReviewNotes("");
    },
  });
  const observeResearchCandidateMutation = useMutation({
    mutationFn: async () => {
      if (!selectedResearchCandidateId) {
        throw new Error("Select a candidate first.");
      }
      return api.observeResearchCandidate(selectedResearchCandidateId);
    },
    onSuccess: async (response) => {
      setLastResearchCandidateObservation(response.observation);
      await queryClient.invalidateQueries({ queryKey: ["research-candidates"] });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-events", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-observations", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-observation-summary", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-qualification", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-shadow-performance", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-shadow-runs", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-testnet-review-dossier", selectedResearchCandidateId],
      });
    },
  });
  const evaluateResearchCandidateQualificationMutation = useMutation({
    mutationFn: async () => {
      if (!selectedResearchCandidateId) {
        throw new Error("Select a candidate first.");
      }
      return api.evaluateResearchCandidateQualification(selectedResearchCandidateId);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-qualification", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-qualification-history", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-testnet-review-dossier", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({ queryKey: ["research-candidate-watchlist"] });
    },
  });
  const previewResearchCandidateShadowPromotionMutation = useMutation({
    mutationFn: async () => {
      if (!selectedResearchCandidateId) {
        throw new Error("Select a candidate first.");
      }
      return api.previewResearchCandidateShadowPromotion(selectedResearchCandidateId, {
        mode: "PREVIEW_ONLY",
        allow_missing_runner_alignment: researchCandidateAllowMissingRunnerAlignment,
      });
    },
    onSuccess: async (response) => {
      setResearchCandidateShadowPromotionPreview(response.preview);
      setResearchCandidateShadowPromotionResult(null);
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-observations", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-observation-summary", selectedResearchCandidateId],
      });
    },
  });
  const applyResearchCandidateShadowPromotionMutation = useMutation({
    mutationFn: async () => {
      if (!selectedResearchCandidateId) {
        throw new Error("Select a candidate first.");
      }
      return api.applyResearchCandidateShadowPromotion(selectedResearchCandidateId, {
        mode: "APPLY",
        allow_missing_runner_alignment: researchCandidateAllowMissingRunnerAlignment,
        confirmation_text: researchCandidateShadowPromotionConfirmation,
      });
    },
    onSuccess: async (response) => {
      setResearchCandidateShadowPromotionResult(response.result);
      setResearchCandidateShadowPromotionPreview({
        candidate_id: response.result.candidate_id,
        candidate_status: response.result.candidate_status,
        strategy_id: response.result.strategy_id,
        symbol: response.result.symbol,
        timeframe: response.result.timeframe,
        current_runner_config: response.result.current_runner_config,
        proposed_runner_config: response.result.proposed_runner_config,
        changes: response.result.changes,
        status: response.result.status,
        reasons: response.result.reasons,
        confirmation_required: response.result.confirmation_required,
        correlation_id: response.result.correlation_id,
        mode: response.result.mode,
        allow_missing_runner_alignment: response.result.allow_missing_runner_alignment,
      });
      await queryClient.invalidateQueries({ queryKey: ["research-candidates"] });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-events", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-observations", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-observation-summary", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-qualification", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-shadow-performance", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["research-candidate-shadow-runs", selectedResearchCandidateId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["exchange-testnet-shadow-runner-config"],
      });
      await queryClient.invalidateQueries({
        queryKey: ["exchange-testnet-shadow-runner-status"],
      });
    },
  });
  const operatorReportMutation = useMutation({
    mutationFn: () => api.generateOperatorReport(reportForm),
    onSuccess: async (response) => {
      setGeneratedReport(response.report);
      if (response.report.persisted) {
        setSelectedReportId(response.report.report_id);
      }
      await queryClient.invalidateQueries({ queryKey: ["operator-reports"] });
    },
  });
  const readinessCheckMutation = useMutation({
    mutationFn: () => api.checkExecutionReadiness(readinessForm),
    onSuccess: async (response) => {
      setLastReadinessResult(response.readiness);
      await queryClient.invalidateQueries({
        queryKey: ["execution-readiness-snapshots"],
      });
    },
  });

  const backfillMutation = useMutation({
    mutationFn: () => api.backfillMarketCandles(backfillForm),
    onSuccess: async (result) => {
      setLastBackfillResult(result);
      setSelectedBackfillRunId(result.run_id);
      await refreshOperationalData();
      await queryClient.invalidateQueries({ queryKey: ["candles"] });
    },
  });
  const aggregateCandlesMutation = useMutation({
    mutationFn: () => api.aggregateMarketCandles(aggregationForm),
    onSuccess: async (result) => {
      setLastAggregationResult(result);
      await queryClient.invalidateQueries({ queryKey: ["candles"] });
      await queryClient.invalidateQueries({ queryKey: ["candle-coverage"] });
    },
  });
  const marketDataQualityMutation = useMutation({
    mutationFn: () => api.getMarketCandleQuality(marketDataQualityForm),
    onSuccess: (response) => {
      setLastMarketDataQualityReport(response.report);
    },
  });
  const marketDataRepairPlanMutation = useMutation({
    mutationFn: () =>
      api.planMarketDataRepair({
        ...marketDataQualityForm,
        repair_mode: "PLAN_ONLY",
        max_ranges: 100,
        reaggregate_derived_intervals: true,
      }),
    onSuccess: (response) => {
      setLastMarketDataRepairPlan(response.plan);
    },
  });
  const marketDataRepairRunMutation = useMutation({
    mutationFn: () =>
      api.runMarketDataRepair({
        ...marketDataQualityForm,
        repair_mode: "REPAIR",
        max_ranges: 100,
        reaggregate_derived_intervals: true,
      }),
    onSuccess: async (response) => {
      setLastMarketDataRepairRun(response.run);
      setLastMarketDataRepairPlan(response.run.plan);
      await queryClient.invalidateQueries({ queryKey: ["market-data-repair-runs"] });
      await queryClient.invalidateQueries({ queryKey: ["candles"] });
      await queryClient.invalidateQueries({ queryKey: ["candle-coverage"] });
    },
  });
  const researchCoverageMutation = useMutation({
    mutationFn: () =>
      api.getResearchDataCoverage({
        exchange: researchDataForm.exchange,
        symbol: researchDataForm.symbol,
        intervals: researchDataForm.intervals.join(","),
        start_time: researchDataForm.start_time,
        end_time: researchDataForm.end_time,
        required_coverage_pct: researchDataForm.required_coverage_pct,
      }),
    onSuccess: (response) => {
      setLastResearchCoverage(response.coverage);
    },
  });
  const researchBuildMutation = useMutation({
    mutationFn: () => api.buildResearchDataset(researchDataForm),
    onSuccess: async (response) => {
      setLastResearchBuild(response.build);
      setLastResearchCoverage(response.build.coverage_after);
      setSelectedResearchBuildId(response.build.build_id);
      await queryClient.invalidateQueries({ queryKey: ["research-builds"] });
      await queryClient.invalidateQueries({ queryKey: ["research-build"] });
      await queryClient.invalidateQueries({ queryKey: ["candle-coverage"] });
    },
  });
  const researchBatchMutation = useMutation({
    mutationFn: () => api.runResearchBatch(researchBatchForm),
    onSuccess: async (response) => {
      setLastResearchBatch(response.batch);
      setSelectedResearchBatchId(response.batch.batch_id);
      await queryClient.invalidateQueries({ queryKey: ["research-batches"] });
      await queryClient.invalidateQueries({ queryKey: ["research-batch"] });
      await queryClient.invalidateQueries({ queryKey: ["research-candidates"] });
      await queryClient.invalidateQueries({ queryKey: ["strategy-experiments"] });
    },
  });
  const researchRegimeDatasetMutation = useMutation({
    mutationFn: () => api.buildResearchRegimeDataset(researchRegimeDatasetForm),
    onSuccess: async (response) => {
      setLastResearchRegimeDataset(response.dataset);
      setSelectedResearchRegimeDatasetId(response.dataset.dataset_id);
      await queryClient.invalidateQueries({ queryKey: ["research-regime-datasets"] });
      await queryClient.invalidateQueries({ queryKey: ["research-regime-dataset"] });
      await queryClient.invalidateQueries({ queryKey: ["research-regime-dataset-windows"] });
    },
  });
  const researchRegimeDiscoveryMutation = useMutation({
    mutationFn: () => api.runResearchRegimeDiscovery(researchRegimeDiscoveryForm),
    onSuccess: async (response) => {
      setLastResearchRegimeDiscovery(response.discovery);
      setSelectedResearchRegimeDiscoveryId(response.discovery.discovery_id);
      await queryClient.invalidateQueries({ queryKey: ["research-regime-discoveries"] });
      await queryClient.invalidateQueries({ queryKey: ["research-regime-discovery"] });
      await queryClient.invalidateQueries({ queryKey: ["research-regime-discovery-windows"] });
    },
  });
  const researchRegimeCalibrationMutation = useMutation({
    mutationFn: () => api.runResearchRegimeCalibration(researchRegimeCalibrationForm),
    onSuccess: async (response) => {
      setLastResearchRegimeCalibration(response.calibration);
      setSelectedResearchRegimeCalibrationId(response.calibration.calibration_id);
      if (response.calibration.recommended_config) {
        setResearchRegimeDiscoveryForm((current) => ({
          ...current,
          symbol: response.calibration.request.symbol,
          timeframe: response.calibration.request.timeframe,
          scan_start: response.calibration.request.scan_start,
          scan_end: response.calibration.request.scan_end,
          window_hours: response.calibration.request.window_hours,
          step_hours: response.calibration.request.step_hours,
          classifier_config: null,
          calibration_id: response.calibration.calibration_id,
        }));
      }
      await queryClient.invalidateQueries({ queryKey: ["research-regime-calibrations"] });
      await queryClient.invalidateQueries({ queryKey: ["research-regime-calibration"] });
      await queryClient.invalidateQueries({ queryKey: ["research-regime-calibration-candidates"] });
    },
  });
  const researchRegimeDatasetFromDiscoveryMutation = useMutation({
    mutationFn: () => {
      if (!selectedResearchRegimeDiscoveryId) {
        throw new Error("Select a regime discovery first.");
      }
      const payload: ResearchRegimeDatasetFromDiscoveryRequest = {
        discovery_id: selectedResearchRegimeDiscoveryId,
      };
      return api.buildResearchRegimeDatasetFromDiscovery(payload);
    },
    onSuccess: async (response) => {
      setLastResearchRegimeDataset(response.dataset);
      setSelectedResearchRegimeDatasetId(response.dataset.dataset_id);
      await queryClient.invalidateQueries({ queryKey: ["research-regime-datasets"] });
      await queryClient.invalidateQueries({ queryKey: ["research-regime-dataset"] });
      await queryClient.invalidateQueries({ queryKey: ["research-regime-dataset-windows"] });
    },
  });
  const researchCampaignMutation = useMutation({
    mutationFn: () => api.runResearchCampaign(researchCampaignForm),
    onSuccess: async (response) => {
      setLastResearchCampaign(response.campaign);
      setSelectedResearchCampaignId(response.campaign.campaign_id);
      await queryClient.invalidateQueries({ queryKey: ["research-campaigns"] });
      await queryClient.invalidateQueries({ queryKey: ["research-campaign"] });
      await queryClient.invalidateQueries({ queryKey: ["research-batches"] });
      await queryClient.invalidateQueries({ queryKey: ["research-candidates"] });
      await queryClient.invalidateQueries({ queryKey: ["strategy-experiments"] });
    },
  });
  const paperMarkMutation = useMutation({
    mutationFn: api.markPaperToMarket,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["paper-account"] });
      queryClient.invalidateQueries({ queryKey: ["paper-positions"] });
      queryClient.invalidateQueries({ queryKey: ["paper-pnl"] });
      queryClient.invalidateQueries({ queryKey: ["paper-equity"] });
      queryClient.invalidateQueries({ queryKey: ["paper-journal"] });
    },
  });
  const paperCloseMutation = useMutation({
    mutationFn: async () => {
      if (!closeTarget) {
        throw new Error("No paper position selected for simulated close.");
      }
      return api.closePaperPosition(closeTarget.id, {
        confirmation_text: closeConfirmation,
        reason: closeReason,
        close_mode: "MARKET_SIMULATED",
      });
    },
    onSuccess: () => {
      setCloseConfirmation("");
      setCloseTarget(null);
      queryClient.invalidateQueries({ queryKey: ["paper-account"] });
      queryClient.invalidateQueries({ queryKey: ["paper-positions"] });
      queryClient.invalidateQueries({ queryKey: ["paper-pnl"] });
      queryClient.invalidateQueries({ queryKey: ["paper-equity"] });
      queryClient.invalidateQueries({ queryKey: ["paper-journal"] });
      queryClient.invalidateQueries({ queryKey: ["metrics-text"] });
    },
  });
  const exchangeTestnetSubmitMutation = useMutation({
    mutationFn: () =>
      api.submitExchangeTestnetOrder({
        symbol: testnetSymbol,
        side: testnetSide,
        order_type: testnetOrderType,
        quote_notional:
          testnetOrderType === "MARKET" && testnetQuoteNotional ? testnetQuoteNotional : undefined,
        quantity: testnetQuantity || undefined,
        limit_price: testnetLimitPrice || undefined,
        risk_decision_id: testnetRiskDecisionId || undefined,
        confirmation_text: testnetConfirmation,
      }),
    onSuccess: async () => {
      setTestnetConfirmation("");
      await refreshOperationalData();
    },
  });
  const exchangeTestnetPipelinePreviewMutation = useMutation({
    mutationFn: () =>
      api.previewExchangeTestnetPipeline({
        risk_decision_id: testnetPipelineRiskDecisionId,
      }),
    onSuccess: (response) => {
      setTestnetPipelineConfirmation(response.preview.confirmation_text);
    },
  });
  const exchangeTestnetPipelineSubmitMutation = useMutation({
    mutationFn: () =>
      api.submitExchangeTestnetPipeline({
        risk_decision_id: testnetPipelineRiskDecisionId,
        confirmation_text: testnetPipelineConfirmation,
      }),
    onSuccess: async () => {
      setTestnetPipelineConfirmation("");
      await refreshOperationalData();
    },
  });
  const exchangeTestnetShadowRunMutation = useMutation({
    mutationFn: () =>
      api.runExchangeTestnetShadow({
        strategy_id: testnetShadowStrategyId,
        symbol: testnetShadowSymbol,
        timeframe: testnetShadowTimeframe,
      }),
    onSuccess: async (response) => {
      setSelectedShadowRunId(response.run.run_id);
      await refreshOperationalData();
    },
  });
  const exchangeTestnetShadowPromotionPreviewMutation = useMutation({
    mutationFn: (shadowRunId: string) =>
      api.previewExchangeTestnetShadowPromotion({ shadow_run_id: shadowRunId }),
    onSuccess: async (response) => {
      setSelectedShadowPromotionId(response.promotion.promotion_id);
      await refreshOperationalData();
    },
  });
  const exchangeTestnetShadowPromotionSubmitMutation = useMutation({
    mutationFn: (promotionId: string) =>
      api.submitExchangeTestnetShadowPromotion(promotionId, {
        confirmation_text: shadowPromotionConfirmation,
      }),
    onSuccess: async () => {
      setShadowPromotionConfirmation("");
      await refreshOperationalData();
    },
  });
  const exchangeTestnetShadowRunnerConfigUpdateMutation = useMutation({
    mutationFn: () => api.updateExchangeTestnetShadowRunnerConfig(shadowRunnerConfigForm),
    onSuccess: refreshOperationalData,
  });
  const exchangeTestnetShadowRunnerControlMutation = useMutation({
    mutationFn: (action: string) => api.controlExchangeTestnetShadowRunner({ action }),
    onSuccess: async (response) => {
      if (response.tick?.correlation_id) {
        setSelectedShadowRunId(null);
      }
      await refreshOperationalData();
    },
  });
  const exchangeTestnetCancelMutation = useMutation({
    mutationFn: (clientOrderId: string) =>
      api.cancelExchangeTestnetOrder(clientOrderId, testnetConfirmation),
    onSuccess: async () => {
      setTestnetConfirmation("");
      await refreshOperationalData();
    },
  });
  const exchangeTestnetRepairMutation = useMutation({
    mutationFn: (clientOrderId: string) =>
      api.repairExchangeTestnetOrder(clientOrderId, {
        action: testnetRepairAction,
        confirmation_text: testnetRepairConfirmation,
        reason: testnetRepairReason || undefined,
        force: testnetRepairForce,
      }),
    onSuccess: async () => {
      setTestnetRepairConfirmation("");
      setTestnetRepairReason("");
      setTestnetRepairForce(false);
      await refreshOperationalData();
    },
  });
  const exchangeReconcileMutation = useMutation({
    mutationFn: () =>
      api.reconcileExchangeTestnetOrders({
        limit: 50,
        status_filter: ["ACKED", "NEW", "PARTIALLY_FILLED"],
      }),
    onSuccess: async (response) => {
      setSelectedReconciliationRunId(response.result.run_id);
      await refreshOperationalData();
    },
  });
  const exchangePrivateStreamCreateListenKeyMutation = useMutation({
    mutationFn: api.createExchangeTestnetPrivateStreamListenKey,
    onSuccess: async () => {
      await refreshOperationalData();
    },
  });
  const exchangePrivateStreamKeepaliveMutation = useMutation({
    mutationFn: () => api.keepaliveExchangeTestnetPrivateStreamListenKey(privateStreamListenKey),
    onSuccess: async () => {
      await refreshOperationalData();
    },
  });
  const exchangePrivateStreamCloseMutation = useMutation({
    mutationFn: () => api.closeExchangeTestnetPrivateStreamListenKey(privateStreamListenKey),
    onSuccess: async () => {
      setPrivateStreamListenKey("");
      await refreshOperationalData();
    },
  });

  const strategies = strategiesQuery.data?.strategies ?? [];
  const orders = ordersQuery.data?.orders ?? [];
  const paperAccount = paperAccountQuery.data?.account;
  const paperPositions = paperPositionsQuery.data?.positions ?? [];
  const paperPnl = paperPnlQuery.data?.pnl;
  const riskDecisions = riskDecisionsQuery.data?.decisions ?? [];
  const latestRiskDecisions = latestRiskDecisionsQuery.data?.decisions ?? [];
  const events = eventsQuery.data?.events ?? [];
  const recentSignals = signalsQuery.data?.signals ?? [];
  const backtestRuns = backtestRunsQuery.data?.runs ?? [];
  const strategyExperiments = strategyExperimentsQuery.data?.experiments ?? [];
  const strategyWalkForwards = strategyWalkForwardsQuery.data?.walk_forwards ?? [];
  const researchBatches = researchBatchesQuery.data?.batches ?? [];
  const researchRegimeDatasets = researchRegimeDatasetsQuery.data?.datasets ?? [];
  const selectedResearchRegimeDataset: ResearchRegimeDatasetResult | null =
    selectedResearchRegimeDatasetQuery.data?.dataset ?? lastResearchRegimeDataset;
  const selectedResearchRegimeWindows: ResearchRegimeWindow[] =
    selectedResearchRegimeDatasetWindowsQuery.data?.windows ??
    selectedResearchRegimeDataset?.windows ??
    [];
  const researchRegimeDiscoveries = researchRegimeDiscoveriesQuery.data?.discoveries ?? [];
  const researchRegimeCalibrations = researchRegimeCalibrationsQuery.data?.calibrations ?? [];
  const selectedResearchRegimeCalibration: ResearchRegimeCalibrationResult | null =
    selectedResearchRegimeCalibrationQuery.data?.calibration ?? lastResearchRegimeCalibration;
  const selectedResearchRegimeCalibrationCandidates =
    selectedResearchRegimeCalibrationCandidatesQuery.data?.candidates ??
    selectedResearchRegimeCalibration?.candidates ??
    [];
  const selectedResearchRegimeDiscovery: ResearchRegimeDiscoveryResult | null =
    selectedResearchRegimeDiscoveryQuery.data?.discovery ?? lastResearchRegimeDiscovery;
  const selectedResearchRegimeDiscoveryWindows: ResearchRegimeDiscoveryCandidateWindow[] =
    selectedResearchRegimeDiscoveryWindowsQuery.data?.windows ??
    selectedResearchRegimeDiscovery?.selected_windows ??
    [];
  const selectedResearchBatch: ResearchBatchResult | null =
    selectedResearchBatchQuery.data?.batch ?? lastResearchBatch;
  const researchCampaigns = researchCampaignsQuery.data?.campaigns ?? [];
  const selectedResearchCampaign: ResearchCampaignResult | null =
    selectedResearchCampaignQuery.data?.campaign ?? lastResearchCampaign;
  const selectedResearchCampaignFailureAttribution =
    selectedResearchCampaignFailureAttributionQuery.data?.attribution ?? null;
  const selectedResearchCampaignRegimeLeaderboard =
    selectedResearchCampaignRegimeLeaderboardQuery.data?.leaderboard ?? null;
  const researchHypotheses = researchHypothesesQuery.data?.hypotheses ?? [];
  const filteredResearchHypotheses = researchHypotheses.filter((hypothesis) => {
    const priorityMatches =
      researchHypothesisPriorityFilter === "ALL" ||
      hypothesis.priority === researchHypothesisPriorityFilter;
    const statusMatches =
      researchHypothesisStatusFilter === "ALL" ||
      hypothesis.status === researchHypothesisStatusFilter;
    return priorityMatches && statusMatches;
  });
  const selectedResearchHypothesis =
    researchHypotheses.find((hypothesis) => hypothesis.id === selectedResearchHypothesisId) ??
    filteredResearchHypotheses[0] ??
    null;
  const selectedExperiment =
    selectedExperimentQuery.data?.experiment ?? null;
  const strategyExperimentRuns =
    selectedExperimentRunsQuery.data?.runs ?? [];
  const selectedExperimentComparison: StrategyMultiTimeframeExperimentResult | null =
    selectedExperimentComparisonQuery.data?.comparison ??
    lastStrategyExperimentResult?.comparison ??
    null;
  const selectedWalkForward: StrategyWalkForwardResult | null =
    selectedWalkForwardQuery.data?.walk_forward ??
    lastStrategyWalkForwardResult?.walk_forward ??
    null;
  const selectedWalkForwardWindows: StrategyWalkForwardWindowResult[] =
    selectedWalkForwardWindowsQuery.data?.windows ??
    lastStrategyWalkForwardResult?.windows ??
    [];
  const researchCandidates = researchCandidatesQuery.data?.candidates ?? [];
  const selectedResearchCandidate: StrategyResearchCandidate | null =
    selectedResearchCandidateQuery.data?.candidate ?? null;
  const researchCandidateEvents: ResearchCandidateLifecycleEvent[] =
    selectedResearchCandidateEventsQuery.data?.events ?? [];
  const researchCandidateObservationHistory: ResearchCandidateObservationHistoryItem[] =
    selectedResearchCandidateObservationQuery.data?.history ?? [];
  const researchCandidateObservationSummary: ResearchCandidateObservationSummary | null =
    selectedResearchCandidateObservationSummaryQuery.data?.summary ?? null;
  const researchCandidateQualification: ResearchCandidateQualificationResult | null =
    selectedResearchCandidateQualificationQuery.data?.qualification ?? null;
  const researchCandidateQualificationHistory: ResearchCandidateQualificationHistory | null =
    selectedResearchCandidateQualificationHistoryQuery.data?.history ?? null;
  const researchCandidateTestnetReviewDossier: ResearchCandidateTestnetReviewDossier | null =
    selectedResearchCandidateTestnetReviewDossierQuery.data?.dossier ?? null;
  const researchCandidateWalkForwardEvidence =
    selectedResearchCandidateWalkForwardQuery.data?.latest ??
    selectedResearchCandidateQuery.data?.walk_forward_evidence ??
    null;
  const researchCandidateWatchlist: ResearchCandidateWatchlistEntry[] =
    researchCandidateWatchlistQuery.data?.watchlist ?? [];
  const researchCandidateReviews: ResearchCandidateReview[] =
    selectedResearchCandidateReviewsQuery.data?.reviews ?? [];
  const researchCandidateShadowPerformance: ResearchCandidateShadowPerformance | null =
    selectedResearchCandidateShadowPerformanceQuery.data?.performance ?? null;
  const researchCandidateShadowPnl: ResearchShadowPnlAttributionResult | null =
    selectedResearchCandidateShadowPnlQuery.data?.attribution ?? null;
  const researchCandidateShadowRuns: ResearchCandidateShadowRunLink[] =
    selectedResearchCandidateShadowRunsQuery.data?.runs ?? [];
  const latestResearchCandidateObservation: StrategyCandidateObservation | null =
    observeResearchCandidateMutation.data?.observation ??
    lastResearchCandidateObservation ??
    selectedResearchCandidateObservationQuery.data?.observations?.[0] ??
    null;
  const latestResearchCandidateRunnerAlignment =
    latestResearchCandidateObservation?.runner_alignment ??
    latestResearchCandidateObservation?.summary.runner_alignment ??
    null;
  const researchCandidateObservationFreshness =
    observationFreshnessState(latestResearchCandidateObservation);
  const researchCandidateObservationAgeSeconds =
    observationAgeSeconds(latestResearchCandidateObservation);
  const acceptForShadowBlockedByStale =
    researchCandidateObservationFreshness === "STALE";
  const latestEligibilityLabel = researchCandidateObservationSummary
    ? researchCandidateObservationSummary.current_accept_for_shadow_eligible
      ? "Eligible"
      : "Not eligible"
    : "Unknown";
  const qualificationNeedsMoreData =
    researchCandidateQualification?.status === "NEEDS_MORE_DATA";
  const shadowPerformanceRecommendationLabel = shadowRecommendationLabel(
    researchCandidateShadowPerformance?.recommendation,
  );
  const shadowPromotionPreview =
    researchCandidateShadowPromotionPreview ??
    (researchCandidateShadowPromotionResult
      ? {
          candidate_id: researchCandidateShadowPromotionResult.candidate_id,
          candidate_status: researchCandidateShadowPromotionResult.candidate_status,
          strategy_id: researchCandidateShadowPromotionResult.strategy_id,
          symbol: researchCandidateShadowPromotionResult.symbol,
          timeframe: researchCandidateShadowPromotionResult.timeframe,
          current_runner_config: researchCandidateShadowPromotionResult.current_runner_config,
          proposed_runner_config: researchCandidateShadowPromotionResult.proposed_runner_config,
          changes: researchCandidateShadowPromotionResult.changes,
          status: researchCandidateShadowPromotionResult.status,
          reasons: researchCandidateShadowPromotionResult.reasons,
          confirmation_required:
            researchCandidateShadowPromotionResult.confirmation_required,
          correlation_id: researchCandidateShadowPromotionResult.correlation_id,
          mode: researchCandidateShadowPromotionResult.mode,
          allow_missing_runner_alignment:
            researchCandidateShadowPromotionResult.allow_missing_runner_alignment,
        }
      : null);
  const expectedShadowPromotionConfirmation = selectedResearchCandidate
    ? `PROMOTE CANDIDATE ${selectedResearchCandidate.id} TO SHADOW`
    : "";
  const latestQualificationEvaluation: ResearchCandidateQualificationEvaluation | null =
    researchCandidateQualificationHistory?.evaluations[0] ?? null;
  const latestQualificationChange: ResearchCandidateQualificationChange | null =
    researchCandidateQualificationHistory?.latest_change ?? null;
  const canApplyShadowPromotion =
    user.role === "OWNER" &&
    Boolean(selectedResearchCandidate) &&
    shadowPromotionPreview !== null &&
    (shadowPromotionPreview.status === "READY" || shadowPromotionPreview.status === "NO_CHANGES") &&
    (selectedResearchCandidate?.status === "ACCEPTED_FOR_SHADOW" ||
      selectedResearchCandidate?.status === "PROMOTED_TO_SHADOW_CONFIG") &&
    researchCandidateObservationFreshness === "FRESH";
  const decideResearchCandidateErrorPayload = getApiErrorPayload(
    decideResearchCandidateMutation.error,
  );
  const feeds = feedQuery.data?.feeds ?? [];
  const dataSymbols = symbolsQuery.data?.symbols ?? DEFAULT_SYMBOLS;
  const telemetrySnapshot = useMemo<TelemetrySnapshot>(
    () => ({
      reachable: Boolean(metricsQuery.data),
      killSwitchActive: metricsQuery.data
        ? readMetricValue(metricsQuery.data, "aegis_kill_switch_active")
        : undefined,
      openPositions: metricsQuery.data
        ? sumMetricValues(metricsQuery.data, "aegis_paper_positions_open")
        : undefined,
      paperEquity: metricsQuery.data
        ? readMetricValue(metricsQuery.data, "aegis_paper_equity")
        : undefined,
      maxFeedAgeSeconds: metricsQuery.data
        ? readMaxFeedAgeSeconds(metricsQuery.data)
        : undefined,
      raw: metricsQuery.data ?? "",
    }),
    [metricsQuery.data],
  );

  const latestTicks = DEFAULT_SYMBOLS.map((symbol, index) => ({
    symbol,
    data: tickQueries[index]?.data?.tick,
    error: tickQueries[index]?.error,
    isLoading: tickQueries[index]?.isLoading,
  }));

  const headerFeedState = summarizeFeedState(feeds);
  const headerDataAge = computeDataAge(feeds, latestTicks.map((item) => item.data?.trade_time));
  const riskEvents = useMemo(
    () =>
      events.filter(
        (event) =>
          event.event_type.startsWith("risk.") ||
          event.event_type.startsWith("system.kill_switch"),
      ),
    [events],
  );
  const latestRiskRejection = useMemo(
    () =>
      latestRiskDecisions.find((decision) => decision.decision === "REJECTED") ?? null,
    [latestRiskDecisions],
  );
  const activeOperatorReport =
    selectedOperatorReportQuery.data?.report ?? generatedReport;

  return (
    <div className="min-h-screen bg-transparent text-slate-100">
      <div className="mx-auto flex min-h-screen max-w-[1700px] gap-4 px-3 py-3 lg:px-4">
        <aside className="hidden w-64 shrink-0 rounded-2xl border border-border bg-panel/90 p-4 shadow-panel lg:block">
          <div className="mb-6">
            <div className="text-xs uppercase tracking-[0.24em] text-muted">
              Aegis Quant
            </div>
            <div className="mt-2 text-xl font-semibold">Operational Cockpit</div>
          </div>
          <nav className="space-y-2">
            {SECTIONS.map((item) => (
              <button
                key={item.id}
                className={cn(
                  "flex w-full items-center justify-between rounded-xl border px-3 py-2 text-left text-sm transition",
                  section === item.id
                    ? "border-accent bg-accent/10 text-white"
                    : "border-border bg-surface/50 text-slate-300 hover:border-slate-500 hover:text-white",
                )}
                onClick={() => setSection(item.id)}
              >
                <span>{item.label}</span>
              </button>
            ))}
          </nav>
        </aside>

        <main className="flex min-w-0 flex-1 flex-col gap-4">
          <header className="sticky top-3 z-20 rounded-2xl border border-border bg-panel/95 p-3 shadow-panel backdrop-blur">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3 rounded-xl border border-border bg-surface/50 px-3 py-2">
              <div>
                <div className="text-sm font-medium text-white">{user.email}</div>
                <div className="text-xs uppercase tracking-[0.2em] text-muted">
                  Role {user.role}
                </div>
              </div>
              <button
                className="rounded-xl border border-border bg-surface/70 px-3 py-2 text-sm text-slate-200 transition hover:border-slate-400 hover:text-white disabled:opacity-50"
                onClick={onLogout}
                disabled={isLoggingOut}
              >
                {isLoggingOut ? "Signing out..." : "Logout"}
              </button>
            </div>
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
              <HeaderStat
                label="Mode"
                value={statusQuery.data?.market_mode?.toUpperCase() ?? "UNKNOWN"}
                tone="neutral"
              />
              <HeaderStat
                label="Kill Switch"
                value={riskQuery.data?.kill_switch.enabled ? "ON" : "OFF"}
                tone={riskQuery.data?.kill_switch.enabled ? "danger" : "ok"}
              />
              <HeaderStat
                label="Feed Status"
                value={headerFeedState.label}
                tone={headerFeedState.tone}
              />
              <HeaderStat
                label="Data Age"
                value={headerDataAge}
                tone={headerFeedState.tone}
              />
              <HeaderStat label="Daily PnL" value="N/A" tone="neutral" />
              <HeaderStat
                label="API Health"
                value={healthQuery.data?.status?.toUpperCase() ?? "UNKNOWN"}
                tone={healthQuery.data?.status === "ok" ? "ok" : "danger"}
              />
            </div>
          </header>

          <div className="rounded-2xl border border-border bg-panel/90 p-3 shadow-panel lg:hidden">
            <div className="flex gap-2 overflow-x-auto">
              {SECTIONS.map((item) => (
                <button
                  key={item.id}
                  className={cn(
                    "whitespace-nowrap rounded-lg border px-3 py-2 text-sm",
                    section === item.id
                      ? "border-accent bg-accent/10"
                      : "border-border bg-surface/60",
                  )}
                  onClick={() => setSection(item.id)}
                >
                  {item.label}
                </button>
              ))}
            </div>
          </div>

          {section === "command-center" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-4" title="System Health">
                <KeyValue
                  items={[
                    ["Status", healthQuery.data?.status ?? "N/A"],
                    ["Service", healthQuery.data?.service ?? "N/A"],
                    ["Environment", healthQuery.data?.environment ?? "N/A"],
                    ["Timestamp", formatDateTime(healthQuery.data?.timestamp)],
                  ]}
                  loading={healthQuery.isLoading}
                  error={getErrorMessage(healthQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="Market Provider">
                <KeyValue
                  items={[
                    ["Status", providerHealthQuery.data?.health.status ?? "N/A"],
                    ["Provider", providerHealthQuery.data?.health.provider ?? "N/A"],
                    ["Base URL", providerHealthQuery.data?.health.base_url ?? "N/A"],
                    [
                      "Latency",
                      providerHealthQuery.data?.health.latency_ms !== null &&
                      providerHealthQuery.data?.health.latency_ms !== undefined
                        ? `${providerHealthQuery.data.health.latency_ms} ms`
                        : "N/A",
                    ],
                    [
                      "Recommendation",
                      providerHealthQuery.data?.health.recommendation ?? "N/A",
                    ],
                  ]}
                  loading={providerHealthQuery.isLoading}
                  error={getErrorMessage(providerHealthQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="System Status">
                <KeyValue
                  items={[
                    ["Mode", statusQuery.data?.market_mode ?? "N/A"],
                    ["Started", formatDateTime(statusQuery.data?.started_at)],
                    [
                      "Database",
                      statusQuery.data?.dependencies.database.status ?? "N/A",
                    ],
                    [
                      "Event Bus",
                      statusQuery.data?.dependencies.event_bus.status ?? "N/A",
                    ],
                    [
                      "Execution",
                      statusQuery.data?.dependencies.exchange_execution.status ?? "N/A",
                    ],
                  ]}
                  loading={statusQuery.isLoading}
                  error={getErrorMessage(statusQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="Kill Switch Status">
                <KeyValue
                  items={[
                    [
                      "State",
                      riskQuery.data?.kill_switch.enabled ? "ACTIVE" : "DISABLED",
                    ],
                    ["Reason", riskQuery.data?.kill_switch.reason ?? "N/A"],
                    [
                      "Updated",
                      formatDateTime(riskQuery.data?.kill_switch.updated_at),
                    ],
                    [
                      "Updated By",
                      riskQuery.data?.kill_switch.updated_by.actor ?? "N/A",
                    ],
                  ]}
                  loading={riskQuery.isLoading}
                  error={getErrorMessage(riskQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="Feed Status">
                <FeedTable feeds={feeds} loading={feedQuery.isLoading} error={getErrorMessage(feedQuery.error)} />
              </Panel>

              <Panel className="xl:col-span-8" title="Telemetry">
                <KeyValue
                  items={[
                    ["Metrics Reachable", telemetrySnapshot.reachable ? "YES" : "NO"],
                    ["Kill Switch Active", telemetrySnapshot.killSwitchActive ?? "N/A"],
                    ["Open Positions", telemetrySnapshot.openPositions ?? "N/A"],
                    ["Paper Equity", telemetrySnapshot.paperEquity ?? "N/A"],
                    ["Max Feed Age Seconds", telemetrySnapshot.maxFeedAgeSeconds ?? "N/A"],
                  ]}
                  loading={metricsQuery.isLoading}
                  error={getErrorMessage(metricsQuery.error)}
                />
                <pre className="mt-3 max-h-64 overflow-auto rounded-xl border border-border bg-surface/80 p-3 text-xs text-copy/80">
                  {telemetrySnapshot.raw || "# metrics unavailable"}
                </pre>
              </Panel>

              <Panel className="xl:col-span-12" title="Execution Readiness">
                <div className="grid gap-3 md:grid-cols-5">
                  <select
                    className="rounded-xl border border-border bg-surface/70 px-3 py-2 text-sm text-white"
                    value={readinessForm.target}
                    onChange={(event) =>
                      setReadinessForm((current) => ({
                        ...current,
                        target: event.target.value as ExecutionReadinessTarget,
                      }))
                    }
                  >
                    {[
                      "PAPER_PIPELINE",
                      "TESTNET_SHADOW",
                      "TESTNET_PROMOTION",
                      "TESTNET_SUBMIT",
                    ].map((target) => (
                      <option key={target} value={target}>
                        {target}
                      </option>
                    ))}
                  </select>
                  <input
                    className="rounded-xl border border-border bg-surface/70 px-3 py-2 text-sm text-white"
                    value={readinessForm.symbol ?? ""}
                    onChange={(event) =>
                      setReadinessForm((current) => ({
                        ...current,
                        symbol: event.target.value,
                      }))
                    }
                    placeholder="Symbol"
                  />
                  <input
                    className="rounded-xl border border-border bg-surface/70 px-3 py-2 text-sm text-white"
                    value={readinessForm.strategy_id ?? ""}
                    onChange={(event) =>
                      setReadinessForm((current) => ({
                        ...current,
                        strategy_id: event.target.value,
                      }))
                    }
                    placeholder="Strategy"
                  />
                  <input
                    className="rounded-xl border border-border bg-surface/70 px-3 py-2 text-sm text-white"
                    value={readinessForm.timeframe ?? ""}
                    onChange={(event) =>
                      setReadinessForm((current) => ({
                        ...current,
                        timeframe: event.target.value,
                      }))
                    }
                    placeholder="Timeframe"
                  />
                  <ActionButton
                    label="Check Readiness"
                    onClick={() => readinessCheckMutation.mutate()}
                    busy={readinessCheckMutation.isPending}
                  />
                </div>
                <div className="mt-4 grid gap-4 md:grid-cols-3">
                  <HeaderStat
                    label="Status"
                    value={lastReadinessResult?.status ?? "UNKNOWN"}
                    tone={
                      lastReadinessResult?.status === "READY"
                        ? "ok"
                        : lastReadinessResult?.status === "DEGRADED"
                          ? "warning"
                          : "danger"
                    }
                  />
                  <HeaderStat
                    label="Score"
                    value={String(lastReadinessResult?.score ?? "N/A")}
                    tone="neutral"
                  />
                  <HeaderStat
                    label="Snapshots"
                    value={String(readinessSnapshotsQuery.data?.snapshots.length ?? 0)}
                    tone="neutral"
                  />
                </div>
                <InlineStatus
                  error={getErrorMessage(readinessCheckMutation.error)}
                  success={
                    lastReadinessResult
                      ? `Computed ${lastReadinessResult.target} readiness`
                      : undefined
                  }
                />
                <div className="mt-4 grid gap-4 xl:grid-cols-3">
                  <SimpleStringTable
                    title="Blockers"
                    values={lastReadinessResult?.blocking_reasons ?? []}
                  />
                  <SimpleStringTable
                    title="Recommendations"
                    values={lastReadinessResult?.recommendations ?? []}
                  />
                  <SimpleStringTable
                    title="Snapshots"
                    values={
                      readinessSnapshotsQuery.data?.snapshots.map(
                        (snapshot) =>
                          `${snapshot.target} ${snapshot.status} ${snapshot.score}`,
                      ) ?? []
                    }
                  />
                </div>
              </Panel>

              {latestTicks.map((tickCard) => (
                <Panel
                  key={tickCard.symbol}
                  className="xl:col-span-4"
                  title={`Latest ${tickCard.symbol} Tick`}
                >
                  <KeyValue
                    items={[
                      ["Price", formatNumber(tickCard.data?.price)],
                      ["Quantity", formatNumber(tickCard.data?.quantity)],
                      ["Trade Time", formatDateTime(tickCard.data?.trade_time)],
                      ["Age", formatRelativeAge(tickCard.data?.trade_time)],
                    ]}
                    loading={tickCard.isLoading}
                    error={getErrorMessage(tickCard.error)}
                  />
                </Panel>
              ))}

              <Panel className="xl:col-span-6" title="Recent Signals">
                <SignalsTable signals={recentSignals.slice(0, 8)} />
              </Panel>

              <Panel className="xl:col-span-6" title="Recent Orders">
                <OrdersTable
                  orders={orders.slice(0, 8)}
                  onSelect={setSelectedOrderId}
                  selectedId={selectedOrderId}
                />
                <InlineStatus error={getErrorMessage(ordersQuery.error)} />
              </Panel>

              <Panel className="xl:col-span-6" title="Paper Account">
                <KeyValue
                  items={[
                    ["Equity", formatNumber(paperAccount?.current_equity)],
                    ["Realized PnL", formatNumber(paperAccount?.realized_pnl)],
                    ["Unrealized PnL", formatNumber(paperAccount?.unrealized_pnl)],
                    ["Daily PnL", formatNumber(paperPnl?.daily_pnl)],
                    ["Open Positions", String(paperPnl?.open_positions_count ?? 0)],
                    ["Drawdown %", formatNumber(paperPnl?.drawdown_pct)],
                  ]}
                  loading={paperAccountQuery.isLoading || paperPnlQuery.isLoading}
                  error={getErrorMessage(paperAccountQuery.error) ?? getErrorMessage(paperPnlQuery.error)}
                />
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Mark To Market"
                    onClick={() => paperMarkMutation.mutate()}
                    busy={paperMarkMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(paperMarkMutation.error)}
                    success={paperMarkMutation.data ? "Paper equity refreshed" : undefined}
                  />
                </div>
              </Panel>

              <Panel className="xl:col-span-12" title="Open Paper Positions">
                <PaperPositionsTable
                  positions={paperPositions.filter((position) => position.status === "open")}
                  onClose={setCloseTarget}
                />
                <InlineStatus error={getErrorMessage(paperPositionsQuery.error)} />
              </Panel>

              <Panel className="xl:col-span-6" title="Recent Backtest Runs">
                <BacktestRunsTable runs={backtestRuns.slice(0, 8)} onSelect={setSelectedRunId} />
              </Panel>

              <Panel className="xl:col-span-6" title="Latest Risk Rejection">
                <RiskRejectionSummary
                  decision={latestRiskRejection}
                  loading={latestRiskDecisionsQuery.isLoading}
                  error={getErrorMessage(latestRiskDecisionsQuery.error)}
                  onOpen={() => {
                    if (latestRiskRejection) {
                      setSelectedRiskDecisionId(latestRiskRejection.id);
                      setSection("risk");
                    }
                  }}
                />
              </Panel>

              <Panel className="xl:col-span-6" title="Recent System Events">
                <EventsTable
                  events={events.slice(0, 8)}
                  loading={eventsQuery.isLoading}
                  error={getErrorMessage(eventsQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-6" title="Paper Pipeline Run">
                <div className="grid gap-3 md:grid-cols-3">
                  <Field
                    label="Strategy"
                    as="select"
                    value={pipelineStrategyId}
                    onChange={setPipelineStrategyId}
                    options={strategies.map((strategy) => strategy.strategy_id)}
                  />
                  <Field
                    label="Symbol"
                    as="select"
                    value={pipelineSymbol}
                    onChange={setPipelineSymbol}
                    options={dataSymbols}
                  />
                  <Field
                    label="Timeframe"
                    value={pipelineTimeframe}
                    onChange={setPipelineTimeframe}
                  />
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Run Paper Pipeline"
                    onClick={() => pipelineMutation.mutate()}
                    busy={pipelineMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(pipelineMutation.error)}
                    success={
                      pipelineMutation.data
                        ? `${pipelineMutation.data.pipeline_decision}: ${pipelineMutation.data.reasons.join(", ") || "no reason"}`
                        : undefined
                    }
                  />
                </div>
              </Panel>

              <Panel className="xl:col-span-6" title="Dangerous Controls">
                <div className="space-y-4">
                  <div className="rounded-xl border border-danger/40 bg-danger/10 p-3">
                    <div className="text-sm font-semibold text-red-200">
                      Activate Kill Switch
                    </div>
                    <div className="mt-1 text-xs text-red-100/80">
                      Immediately stops paper order execution through the normal pipeline.
                    </div>
                    <textarea
                      className="mt-3 h-20 w-full rounded-lg border border-danger/40 bg-surface px-3 py-2 text-sm outline-none"
                      placeholder="Reason"
                      value={killSwitchReason}
                      onChange={(event) => setKillSwitchReason(event.target.value)}
                    />
                    <div className="mt-3 flex items-center gap-3">
                      <ActionButton
                        label="Activate Kill Switch"
                        onClick={() => killSwitchMutation.mutate()}
                        tone="danger"
                        busy={killSwitchMutation.isPending}
                      />
                      <InlineStatus
                        error={getErrorMessage(killSwitchMutation.error)}
                        success={killSwitchMutation.data?.message}
                      />
                    </div>
                  </div>

                  <div className="rounded-xl border border-warning/40 bg-warning/10 p-3">
                    <div className="text-sm font-semibold text-amber-100">
                      Resume Trading
                    </div>
                    <div className="mt-1 text-xs text-amber-50/80">
                      Requires typed confirmation exactly equal to RESUME TRADING.
                    </div>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <Field
                        label="Confirmation"
                        value={resumeConfirmation}
                        onChange={setResumeConfirmation}
                        placeholder="RESUME TRADING"
                      />
                      <Field
                        label="Reason"
                        value={resumeReason}
                        onChange={setResumeReason}
                        placeholder="Operator reason"
                      />
                    </div>
                    <div className="mt-3 flex items-center gap-3">
                      <ActionButton
                        label="Resume Paper Trading"
                        onClick={() => resumeMutation.mutate()}
                        tone="warning"
                        busy={resumeMutation.isPending}
                        disabled={resumeConfirmation !== "RESUME TRADING"}
                      />
                      <InlineStatus
                        error={getErrorMessage(resumeMutation.error)}
                        success={resumeMutation.data?.message}
                      />
                    </div>
                  </div>
                </div>
              </Panel>
            </section>
          )}

          {section === "market-data" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-3" title="Symbols">
                <SimpleList items={dataSymbols} />
              </Panel>
              <Panel className="xl:col-span-4" title="Feed Status">
                <FeedTable feeds={feeds} loading={feedQuery.isLoading} error={getErrorMessage(feedQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-5" title="Latest Ticks">
                <TicksTable ticks={latestTicks} />
              </Panel>
              <Panel className="xl:col-span-12" title="Recent 1m Candles">
                <div className="mb-3 flex max-w-xs">
                  <Field
                    label="Symbol"
                    as="select"
                    value={selectedSymbol}
                    onChange={setSelectedSymbol}
                    options={dataSymbols}
                  />
                </div>
                <CandlesTable candles={candlesQuery.data?.candles ?? []} />
                <InlineStatus error={getErrorMessage(candlesQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-5" title="Candle Coverage">
                <div className="mb-3 flex max-w-xs">
                  <Field
                    label="Symbol"
                    as="select"
                    value={selectedSymbol}
                    onChange={setSelectedSymbol}
                    options={dataSymbols}
                  />
                </div>
                <CandleCoverageTable
                  coverage={candleCoverageQuery.data?.coverage ?? null}
                  loading={candleCoverageQuery.isLoading}
                  error={getErrorMessage(candleCoverageQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-7" title="Market Data Quality">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Symbol"
                    as="select"
                    value={marketDataQualityForm.symbol}
                    onChange={(value) =>
                      setMarketDataQualityForm((current) => ({ ...current, symbol: value }))
                    }
                    options={dataSymbols}
                  />
                  <Field
                    label="Interval"
                    as="select"
                    value={marketDataQualityForm.interval}
                    onChange={(value) =>
                      setMarketDataQualityForm((current) => ({ ...current, interval: value }))
                    }
                    options={["1m", "5m", "15m", "1h"]}
                  />
                  <Field
                    label="Start"
                    value={marketDataQualityForm.start_time}
                    onChange={(value) =>
                      setMarketDataQualityForm((current) => ({ ...current, start_time: value }))
                    }
                  />
                  <Field
                    label="End"
                    value={marketDataQualityForm.end_time}
                    onChange={(value) =>
                      setMarketDataQualityForm((current) => ({ ...current, end_time: value }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <ActionButton
                    label="Inspect Quality"
                    onClick={() => marketDataQualityMutation.mutate()}
                    busy={marketDataQualityMutation.isPending}
                  />
                  <ActionButton
                    label="Plan Repair"
                    onClick={() => marketDataRepairPlanMutation.mutate()}
                    busy={marketDataRepairPlanMutation.isPending}
                  />
                  <ActionButton
                    label="Run Repair"
                    onClick={() => marketDataRepairRunMutation.mutate()}
                    busy={marketDataRepairRunMutation.isPending}
                  />
                  <InlineStatus
                    error={
                      getErrorMessage(marketDataQualityMutation.error) ||
                      getErrorMessage(marketDataRepairPlanMutation.error) ||
                      getErrorMessage(marketDataRepairRunMutation.error)
                    }
                    success={
                      lastMarketDataRepairRun
                        ? `${lastMarketDataRepairRun.status} gaps ${lastMarketDataRepairRun.gap_count_before}->${lastMarketDataRepairRun.gap_count_after}`
                        : undefined
                    }
                  />
                </div>
                <MarketDataQualityPanel report={lastMarketDataQualityReport} />
                <MarketDataRepairPanel
                  plan={lastMarketDataRepairPlan}
                  run={lastMarketDataRepairRun}
                  recentRuns={repairRunsQuery.data?.runs ?? []}
                />
              </Panel>
              <Panel className="xl:col-span-7" title="Research Data">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Exchange"
                    value={researchDataForm.exchange ?? "binance"}
                    onChange={(value) =>
                      setResearchDataForm((current) => ({ ...current, exchange: value }))
                    }
                    disabled
                  />
                  <Field
                    label="Symbol"
                    as="select"
                    value={researchDataForm.symbol}
                    onChange={(value) =>
                      setResearchDataForm((current) => ({ ...current, symbol: value }))
                    }
                    options={dataSymbols}
                  />
                  <Field
                    label="Intervals"
                    value={researchDataForm.intervals.join(",")}
                    onChange={(value) =>
                      setResearchDataForm((current) => ({
                        ...current,
                        intervals: value
                          .split(",")
                          .map((entry) => entry.trim())
                          .filter(Boolean),
                      }))
                    }
                  />
                  <Field
                    label="Required %"
                    value={researchDataForm.required_coverage_pct ?? "95"}
                    onChange={(value) =>
                      setResearchDataForm((current) => ({
                        ...current,
                        required_coverage_pct: value,
                      }))
                    }
                  />
                  <Field
                    label="Start"
                    value={researchDataForm.start_time}
                    onChange={(value) =>
                      setResearchDataForm((current) => ({ ...current, start_time: value }))
                    }
                  />
                  <Field
                    label="End"
                    value={researchDataForm.end_time}
                    onChange={(value) =>
                      setResearchDataForm((current) => ({ ...current, end_time: value }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <ActionButton
                    label="Inspect Coverage"
                    onClick={() => researchCoverageMutation.mutate()}
                    busy={researchCoverageMutation.isPending}
                  />
                  <ActionButton
                    label="Build Dataset"
                    onClick={() => researchBuildMutation.mutate()}
                    busy={researchBuildMutation.isPending}
                    disabled={user.role !== "OWNER" && user.role !== "OPERATOR"}
                  />
                  <InlineStatus
                    error={
                      getErrorMessage(researchCoverageMutation.error) ||
                      getErrorMessage(researchBuildMutation.error)
                    }
                    success={
                      lastResearchBuild
                        ? `Build ${lastResearchBuild.status} with ${lastResearchBuild.coverage_after.status} readiness`
                        : lastResearchCoverage
                          ? `Coverage ${lastResearchCoverage.status}`
                          : undefined
                    }
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-8" title="Research Coverage Result">
                <ResearchCoverageTable coverage={lastResearchCoverage} />
              </Panel>
              <Panel className="xl:col-span-4" title="Recent Dataset Builds">
                <ResearchDatasetBuildsTable
                  builds={researchBuildsQuery.data?.builds ?? []}
                  selectedBuildId={selectedResearchBuildId}
                  onSelect={setSelectedResearchBuildId}
                />
                <InlineStatus error={getErrorMessage(researchBuildsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-12" title="Selected Dataset Build">
                <ResearchDatasetBuildDetail
                  build={
                    selectedResearchBuildQuery.data?.build ?? lastResearchBuild ?? null
                  }
                  loading={selectedResearchBuildQuery.isLoading}
                  error={getErrorMessage(selectedResearchBuildQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-7" title="Aggregate Candles">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Exchange"
                    value={aggregationForm.exchange ?? "binance"}
                    onChange={(value) =>
                      setAggregationForm((current) => ({ ...current, exchange: value }))
                    }
                    disabled
                  />
                  <Field
                    label="Symbol"
                    as="select"
                    value={aggregationForm.symbol}
                    onChange={(value) =>
                      setAggregationForm((current) => ({ ...current, symbol: value }))
                    }
                    options={dataSymbols}
                  />
                  <Field
                    label="Source"
                    value={aggregationForm.source_interval}
                    onChange={(value) =>
                      setAggregationForm((current) => ({ ...current, source_interval: value }))
                    }
                    options={["1m"]}
                  />
                  <Field
                    label="Target"
                    value={aggregationForm.target_interval}
                    onChange={(value) =>
                      setAggregationForm((current) => ({ ...current, target_interval: value }))
                    }
                    options={AGGREGATION_TARGET_OPTIONS}
                  />
                  <Field
                    label="Start"
                    value={aggregationForm.start_time}
                    onChange={(value) =>
                      setAggregationForm((current) => ({ ...current, start_time: value }))
                    }
                  />
                  <Field
                    label="End"
                    value={aggregationForm.end_time}
                    onChange={(value) =>
                      setAggregationForm((current) => ({ ...current, end_time: value }))
                    }
                  />
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Aggregate Candles"
                    onClick={() => aggregateCandlesMutation.mutate()}
                    busy={aggregateCandlesMutation.isPending}
                    disabled={user.role !== "OWNER" && user.role !== "OPERATOR"}
                  />
                  <InlineStatus
                    error={getErrorMessage(aggregateCandlesMutation.error)}
                    success={
                      lastAggregationResult
                        ? `Persisted ${lastAggregationResult.inserted} inserts / ${lastAggregationResult.updated} updates`
                        : undefined
                    }
                  />
                </div>
                {lastAggregationResult ? (
                  <div className="mt-3">
                    <KeyValue
                      items={[
                        ["Source Candles", String(lastAggregationResult.source_candles)],
                        ["Aggregated", String(lastAggregationResult.aggregated_candles)],
                        ["Skipped Incomplete", String(lastAggregationResult.skipped_incomplete)],
                      ]}
                    />
                  </div>
                ) : null}
              </Panel>
              <Panel className="xl:col-span-5" title="Historical Backfill">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Exchange"
                    value={backfillForm.exchange ?? "binance"}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, exchange: value }))
                    }
                    disabled
                  />
                  <Field
                    label="Symbol"
                    as="select"
                    value={backfillForm.symbol}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, symbol: value }))
                    }
                    options={dataSymbols}
                  />
                  <Field
                    label="Interval"
                    value={backfillForm.interval}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, interval: value }))
                    }
                  />
                  <Field
                    label="Limit"
                    value={String(backfillForm.limit_per_request ?? 1000)}
                    onChange={(value) =>
                      setBackfillForm((current) => ({
                        ...current,
                        limit_per_request: Number(value) || 1000,
                      }))
                    }
                  />
                  <Field
                    label="Start"
                    value={backfillForm.start_time}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, start_time: value }))
                    }
                  />
                  <Field
                    label="End"
                    value={backfillForm.end_time}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, end_time: value }))
                    }
                  />
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Run Candle Backfill"
                    onClick={() => backfillMutation.mutate()}
                    busy={backfillMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(backfillMutation.error)}
                    success={
                      lastBackfillResult
                        ? `${lastBackfillResult.status} ${lastBackfillResult.inserted_candles} inserts`
                        : undefined
                    }
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-7" title="Recent Backfill Runs">
                <BackfillRunsTable
                  runs={backfillRunsQuery.data?.runs ?? []}
                  selectedRunId={selectedBackfillRunId}
                  onSelect={setSelectedBackfillRunId}
                />
                <InlineStatus error={getErrorMessage(backfillRunsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-12" title="Selected Backfill Run">
                <KeyValue
                  items={[
                    ["Run ID", selectedBackfillRunQuery.data?.run.run_id ?? "N/A"],
                    ["Status", selectedBackfillRunQuery.data?.run.status ?? "N/A"],
                    ["Exchange", selectedBackfillRunQuery.data?.run.exchange ?? "N/A"],
                    ["Symbol", selectedBackfillRunQuery.data?.run.symbol ?? "N/A"],
                    ["Interval", selectedBackfillRunQuery.data?.run.interval ?? "N/A"],
                    [
                      "Provider URL",
                      selectedBackfillRunQuery.data?.run.selected_provider ?? "N/A",
                    ],
                    [
                      "Provider Attempts",
                      selectedBackfillRunQuery.data?.run.provider_attempts
                        ?.map((attempt) =>
                          `${attempt.base_url} ${attempt.success ? "OK" : attempt.error_kind ?? "FAILED"}`,
                        )
                        .join(" | ") ?? "N/A",
                    ],
                    [
                      "Counts",
                      selectedBackfillRunQuery.data
                        ? `${selectedBackfillRunQuery.data.run.inserted_candles} inserted / ${selectedBackfillRunQuery.data.run.updated_candles} updated / ${selectedBackfillRunQuery.data.run.skipped_candles} skipped`
                        : "N/A",
                    ],
                    [
                      "Failure Reason",
                      selectedBackfillRunQuery.data?.run.failed_reason ?? "N/A",
                    ],
                    [
                      "Error Kind",
                      selectedBackfillRunQuery.data?.run.failure_diagnostic?.error_kind ??
                        "N/A",
                    ],
                    [
                      "Recommendation",
                      selectedBackfillRunQuery.data?.run.recommendation ?? "N/A",
                    ],
                  ]}
                  loading={selectedBackfillRunQuery.isLoading}
                  error={getErrorMessage(selectedBackfillRunQuery.error)}
                />
              </Panel>
            </section>
          )}

          {section === "strategies" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-7" title="Strategy List">
                <StrategiesTable
                  strategies={strategies}
                  onSelect={setSelectedStrategyId}
                  selectedStrategyId={selectedStrategyId}
                  onToggle={(strategy, enabled) =>
                    toggleStrategyMutation.mutate({
                      strategyId: strategy.strategy_id,
                      enabled,
                    })
                  }
                  onEvaluate={(strategy) =>
                    evaluateMutation.mutate({
                      strategyId: strategy.strategy_id,
                      symbol: strategy.symbols[0],
                    })
                  }
                  busyStrategyId={
                    toggleStrategyMutation.variables?.strategyId ??
                    evaluateMutation.variables?.strategyId
                  }
                />
                <InlineStatus
                  error={
                    toggleStrategyMutation.error
                      ? getErrorMessage(toggleStrategyMutation.error)
                      : evaluateMutation.error
                        ? getErrorMessage(evaluateMutation.error)
                        : undefined
                  }
                />
              </Panel>
              <Panel className="xl:col-span-5" title="Selected Strategy Status">
                <div className="space-y-3">
                  <KeyValue
                    items={[
                      [
                        "Strategy",
                        selectedStrategyStatusQuery.data?.strategy.strategy_id ?? "N/A",
                      ],
                      [
                        "Enabled",
                        String(selectedStrategyStatusQuery.data?.strategy.enabled ?? false),
                      ],
                      ["Mode", selectedStrategyStatusQuery.data?.strategy.mode ?? "N/A"],
                      [
                        "Version",
                        String(
                          selectedStrategyStatusQuery.data?.strategy.config_version ?? "N/A",
                        ),
                      ],
                      [
                        "Last Evaluated",
                        formatDateTime(
                          selectedStrategyStatusQuery.data?.strategy.last_evaluated_at,
                        ),
                      ],
                    ]}
                    loading={selectedStrategyStatusQuery.isLoading}
                    error={getErrorMessage(selectedStrategyStatusQuery.error)}
                  />
                  <div className="grid gap-3 md:grid-cols-2">
                    <Field
                      label="Mode"
                      value={strategyConfigForm.mode}
                      as="select"
                      options={["paper", "research", "shadow"]}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({ ...current, mode: value }))
                      }
                    />
                    <Field
                      label="Enabled"
                      value={String(strategyConfigForm.enabled)}
                      as="select"
                      options={["true", "false"]}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          enabled: value === "true",
                        }))
                      }
                    />
                    <Field
                      label="Symbols"
                      value={strategyConfigForm.symbols.join(",")}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          symbols: value.split(",").map((item) => item.trim()).filter(Boolean),
                        }))
                      }
                    />
                    <Field
                      label="Timeframe"
                      value={strategyConfigForm.timeframe}
                      as="select"
                      options={TIMEFRAME_OPTIONS}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({ ...current, timeframe: value }))
                      }
                    />
                    <Field
                      label="Suggested Notional"
                      value={strategyConfigForm.suggested_notional}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          suggested_notional: value,
                        }))
                      }
                    />
                    <Field
                      label="Lookback Candles"
                      value={String(strategyConfigForm.lookback_candles)}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          lookback_candles: Number(value) || 0,
                        }))
                      }
                    />
                    <Field
                      label="Trend Lookback"
                      value={String(strategyConfigForm.trend_lookback_candles ?? "")}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          trend_lookback_candles: value ? Number(value) || 0 : null,
                        }))
                      }
                    />
                    <Field
                      label="Momentum Lookback"
                      value={String(strategyConfigForm.momentum_lookback_candles ?? "")}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          momentum_lookback_candles: value ? Number(value) || 0 : null,
                        }))
                      }
                    />
                    <Field
                      label="Breakout Lookback"
                      value={String(strategyConfigForm.breakout_lookback_candles ?? "")}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          breakout_lookback_candles: value ? Number(value) || 0 : null,
                        }))
                      }
                    />
                    <Field
                      label="Max Signal Age"
                      value={String(strategyConfigForm.max_signal_age_ms)}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          max_signal_age_ms: Number(value) || 0,
                        }))
                      }
                    />
                    <Field
                      label="Cooldown Seconds"
                      value={String(strategyConfigForm.cooldown_seconds)}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          cooldown_seconds: Number(value) || 0,
                        }))
                      }
                    />
                    <Field
                      label="Confidence Floor"
                      value={strategyConfigForm.confidence_floor ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          confidence_floor: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Lower Band %"
                      value={strategyConfigForm.lower_band_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          lower_band_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Upper Band %"
                      value={strategyConfigForm.upper_band_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          upper_band_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Min Range Width %"
                      value={strategyConfigForm.min_range_width_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          min_range_width_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Max Range Width %"
                      value={strategyConfigForm.max_range_width_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          max_range_width_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Min Close Above SMA %"
                      value={strategyConfigForm.min_close_above_sma_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          min_close_above_sma_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Max Close Above SMA %"
                      value={strategyConfigForm.max_close_above_sma_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          max_close_above_sma_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Min Momentum Return %"
                      value={strategyConfigForm.min_momentum_return_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          min_momentum_return_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Stop Loss %"
                      value={strategyConfigForm.stop_loss_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          stop_loss_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Take Profit %"
                      value={strategyConfigForm.take_profit_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          take_profit_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Holding Candles"
                      value={String(strategyConfigForm.holding_candles ?? "")}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          holding_candles: value ? Number(value) : null,
                        }))
                      }
                    />
                    <Field
                      label="Notes"
                      value={strategyConfigForm.notes ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          notes: value,
                        }))
                      }
                    />
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <ActionButton
                      label="Validate"
                      onClick={() => validateStrategyConfigMutation.mutate()}
                      busy={validateStrategyConfigMutation.isPending}
                    />
                    <ActionButton
                      label="Update"
                      onClick={() => updateStrategyConfigMutation.mutate()}
                      busy={updateStrategyConfigMutation.isPending}
                    />
                    <ActionButton
                      label="Dry Run"
                      onClick={() => strategyDryRunMutation.mutate()}
                      busy={strategyDryRunMutation.isPending}
                    />
                  </div>
                  <InlineStatus
                    error={
                      getErrorMessage(validateStrategyConfigMutation.error) ??
                      getErrorMessage(updateStrategyConfigMutation.error) ??
                      getErrorMessage(strategyDryRunMutation.error)
                    }
                    success={
                      validateStrategyConfigMutation.data
                        ? `validation: ${validateStrategyConfigMutation.data.validation.valid ? "valid" : "rejected"}`
                        : strategyDryRunMutation.data
                          ? `dry-run: ${strategyDryRunMutation.data.result.reason}`
                          : updateStrategyConfigMutation.data
                            ? "config updated"
                            : undefined
                    }
                  />
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    {(validateStrategyConfigMutation.data?.validation.issues ?? []).map((issue) => (
                      <div key={`${issue.field}-${issue.code}`}>
                        {issue.severity} {issue.field}: {issue.message}
                      </div>
                    ))}
                  </div>
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Config Versions</div>
                    {(strategyConfigVersionsQuery.data?.versions ?? []).slice(0, 5).map((entry) => (
                      <div key={`${entry.strategy_id}-${entry.version}`}>
                        v{entry.version} {entry.config.mode} enabled={String(entry.config.enabled)}
                      </div>
                    ))}
                  </div>
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Recent Config Audit</div>
                    {(strategyConfigAuditQuery.data?.audit ?? []).slice(0, 5).map((entry) => (
                      <div key={entry.audit_id}>
                        {formatDateTime(entry.created_at)} v{entry.version ?? "-"} issues=
                        {entry.validation_issues.length}
                      </div>
                    ))}
                  </div>
                </div>
              </Panel>
              <Panel className="xl:col-span-12" title="Recent Signals">
                <SignalsTable signals={recentSignals} />
              </Panel>
              <Panel className="xl:col-span-12" title="Strategy Diagnostics">
                <div className="space-y-3">
                  <div className="grid gap-3 md:grid-cols-4">
                    <Field
                      label="Strategy"
                      value={selectedStrategyId}
                      as="select"
                      options={strategies.map((strategy) => strategy.strategy_id)}
                      onChange={setSelectedStrategyId}
                    />
                    <Field
                      label="Symbol"
                      value={strategyDiagnosticsForm.symbol}
                      onChange={(value) =>
                        setStrategyDiagnosticsForm((current) => ({
                          ...current,
                          symbol: value,
                        }))
                      }
                    />
                    <Field
                      label="Timeframe"
                      value={strategyDiagnosticsForm.timeframe}
                      as="select"
                      options={TIMEFRAME_OPTIONS}
                      onChange={(value) =>
                        setStrategyDiagnosticsForm((current) => ({
                          ...current,
                          timeframe: value,
                        }))
                      }
                    />
                    <Field
                      label="Close Limit"
                      value={String(strategyDiagnosticsForm.limit)}
                      onChange={(value) =>
                        setStrategyDiagnosticsForm((current) => ({
                          ...current,
                          limit: Number(value) || 20,
                        }))
                      }
                    />
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <ActionButton
                      label="Run Diagnostics"
                      onClick={() => strategyDiagnosticsMutation.mutate()}
                      busy={strategyDiagnosticsMutation.isPending}
                    />
                  </div>
                  <InlineStatus
                    error={getErrorMessage(strategyDiagnosticsMutation.error)}
                    success={
                      strategyDiagnosticsResult
                        ? `diagnostics: ${strategyDiagnosticsResult.final_decision}`
                        : undefined
                    }
                  />
                  <KeyValue
                    items={[
                      ["Decision", strategyDiagnosticsResult?.final_decision ?? "N/A"],
                      ["No-Signal Reason", strategyDiagnosticsResult?.no_signal_reason ?? "N/A"],
                      ["Enabled", String(strategyDiagnosticsResult?.strategy_enabled ?? false)],
                      ["Config Valid", String(strategyDiagnosticsResult?.config_valid ?? false)],
                      [
                        "Latest Candle",
                        formatDateTime(
                          strategyDiagnosticsResult?.data_health.latest_closed_candle_time,
                        ),
                      ],
                      [
                        "Closed Candles",
                        strategyDiagnosticsResult
                          ? `${strategyDiagnosticsResult.data_health.available_closed_candles} / ${strategyDiagnosticsResult.data_health.required_closed_candles}`
                          : "N/A",
                      ],
                    ]}
                    loading={strategyDiagnosticsMutation.isPending}
                    error={undefined}
                  />
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-sm text-slate-200">
                    <div className="font-medium text-slate-100">Summary</div>
                    <div className="mt-2 text-slate-300">
                      {strategyDiagnosticsResult?.summary ?? "Run diagnostics to inspect strategy conditions."}
                    </div>
                  </div>
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Condition Checks</div>
                    {(strategyDiagnosticsResult?.condition_checks ?? []).map((check) => (
                      <div key={`${check.name}-${check.message}`} className="mt-2">
                        {check.severity} {check.name}: {check.message}
                      </div>
                    ))}
                  </div>
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Latest Closes</div>
                    {(strategyDiagnosticsResult?.data_health.latest_closes ?? []).map((close) => (
                      <div key={close} className="mt-2">
                        {close}
                      </div>
                    ))}
                  </div>
                </div>
              </Panel>
              <Panel className="xl:col-span-12" title="Strategy Opportunity">
                <div className="space-y-3">
                  <div className="grid gap-3 md:grid-cols-5">
                    <Field
                      label="Strategy"
                      value={selectedStrategyId}
                      as="select"
                      options={strategies.map((strategy) => strategy.strategy_id)}
                      onChange={setSelectedStrategyId}
                    />
                    <Field
                      label="Symbol"
                      value={strategyOpportunityForm.symbol}
                      onChange={(value) =>
                        setStrategyOpportunityForm((current) => ({ ...current, symbol: value }))
                      }
                    />
                    <Field
                      label="Timeframe"
                      value={strategyOpportunityForm.timeframe}
                      as="select"
                      options={TIMEFRAME_OPTIONS}
                      onChange={(value) =>
                        setStrategyOpportunityForm((current) => ({ ...current, timeframe: value }))
                      }
                    />
                    <Field
                      label="Start"
                      value={strategyOpportunityForm.start_time}
                      onChange={(value) =>
                        setStrategyOpportunityForm((current) => ({ ...current, start_time: value }))
                      }
                    />
                    <Field
                      label="End"
                      value={strategyOpportunityForm.end_time}
                      onChange={(value) =>
                        setStrategyOpportunityForm((current) => ({ ...current, end_time: value }))
                      }
                    />
                  </div>
                  <ActionButton
                    label="Analyze Opportunity"
                    onClick={() => strategyOpportunityMutation.mutate()}
                    busy={strategyOpportunityMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(strategyOpportunityMutation.error)}
                    success={
                      strategyOpportunityResult
                        ? `opportunity: ${strategyOpportunityResult.recommendation.status}`
                        : undefined
                    }
                  />
                  <KeyValue
                    items={[
                      ["Status", strategyOpportunityResult?.recommendation.status ?? "N/A"],
                      ["Signal Rate", strategyOpportunityResult ? `${strategyOpportunityResult.signal_rate_pct}%` : "N/A"],
                      [
                        "Windows",
                        strategyOpportunityResult
                          ? `${strategyOpportunityResult.evaluable_windows} / candles ${strategyOpportunityResult.total_closed_candles}`
                          : "N/A",
                      ],
                      ["Data Quality", strategyOpportunityResult?.data_quality_status ?? "N/A"],
                    ]}
                    loading={strategyOpportunityMutation.isPending}
                    error={undefined}
                  />
                  <div className="grid gap-3 lg:grid-cols-2">
                    <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                      <div className="font-medium text-slate-100">Top Blocking Conditions</div>
                      {(strategyOpportunityResult?.top_blocking_conditions ?? []).map((row) => (
                        <div key={row.condition} className="mt-2">
                          {row.condition}: {row.failed_count} ({row.failure_rate_pct}%)
                        </div>
                      ))}
                    </div>
                    <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                      <div className="font-medium text-slate-100">Recommendations</div>
                      {(strategyOpportunityResult?.recommendation.messages ?? []).map((message) => (
                        <div key={message} className="mt-2">
                          {message}
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="overflow-x-auto rounded-xl border border-border bg-surface/40">
                    <table className="min-w-full text-left text-xs text-slate-300">
                      <thead className="text-slate-100">
                        <tr>
                          <th className="px-3 py-2">Condition</th>
                          <th className="px-3 py-2">Passed</th>
                          <th className="px-3 py-2">Failed</th>
                          <th className="px-3 py-2">Pass Rate</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(strategyOpportunityResult?.condition_pass_rates ?? []).map((row) => (
                          <tr key={row.condition} className="border-t border-border">
                            <td className="px-3 py-2">{row.condition}</td>
                            <td className="px-3 py-2">{row.passed_count}</td>
                            <td className="px-3 py-2">{row.failed_count}</td>
                            <td className="px-3 py-2">{row.pass_rate_pct}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="grid gap-3 lg:grid-cols-2">
                    <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                      <div className="font-medium text-slate-100">Sample Fail Windows</div>
                      {(strategyOpportunityResult?.example_fail_windows ?? []).map((window) => (
                        <div key={window.source_candle_open_time} className="mt-2">
                          {formatDateTime(window.source_candle_open_time)} blocker=
                          {window.blocking_condition ?? "-"}
                        </div>
                      ))}
                    </div>
                    <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                      <div className="font-medium text-slate-100">Sample Pass Windows</div>
                      {(strategyOpportunityResult?.example_pass_windows ?? []).map((window) => (
                        <div key={window.source_candle_open_time} className="mt-2">
                          {formatDateTime(window.source_candle_open_time)}
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </Panel>
              <Panel className="xl:col-span-12" title="Strategy Exit Attribution">
                <div className="space-y-3">
                  <div className="grid gap-3 md:grid-cols-4">
                    <Field
                      label="Symbol"
                      value={strategyExitAttributionForm.symbol}
                      onChange={(value) =>
                        setStrategyExitAttributionForm((current) => ({ ...current, symbol: value }))
                      }
                    />
                    <Field
                      label="Timeframe"
                      value={strategyExitAttributionForm.timeframe}
                      as="select"
                      options={TIMEFRAME_OPTIONS}
                      onChange={(value) =>
                        setStrategyExitAttributionForm((current) => ({ ...current, timeframe: value }))
                      }
                    />
                    <Field
                      label="Start"
                      value={strategyExitAttributionForm.start_time}
                      onChange={(value) =>
                        setStrategyExitAttributionForm((current) => ({ ...current, start_time: value }))
                      }
                    />
                    <Field
                      label="End"
                      value={strategyExitAttributionForm.end_time}
                      onChange={(value) =>
                        setStrategyExitAttributionForm((current) => ({ ...current, end_time: value }))
                      }
                    />
                    <Field
                      label="Experiment Run ID"
                      value={strategyExitAttributionForm.experiment_run_id}
                      onChange={(value) =>
                        setStrategyExitAttributionForm((current) => ({ ...current, experiment_run_id: value }))
                      }
                    />
                    <Field
                      label="Holding Windows"
                      value={strategyExitAttributionForm.holding_windows}
                      onChange={(value) =>
                        setStrategyExitAttributionForm((current) => ({ ...current, holding_windows: value }))
                      }
                    />
                    <Field
                      label="Fee Bps"
                      value={strategyExitAttributionForm.fee_bps}
                      onChange={(value) =>
                        setStrategyExitAttributionForm((current) => ({ ...current, fee_bps: value }))
                      }
                    />
                    <Field
                      label="Slippage Bps"
                      value={strategyExitAttributionForm.slippage_bps}
                      onChange={(value) =>
                        setStrategyExitAttributionForm((current) => ({ ...current, slippage_bps: value }))
                      }
                    />
                  </div>
                  <ActionButton
                    label="Run Exit Attribution"
                    onClick={() => strategyExitAttributionMutation.mutate()}
                    busy={strategyExitAttributionMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(strategyExitAttributionMutation.error)}
                    success={
                      strategyExitAttributionResult
                        ? `exit attribution: ${strategyExitAttributionResult.status}`
                        : undefined
                    }
                  />
                  <KeyValue
                    items={[
                      ["Status", strategyExitAttributionResult?.status ?? "N/A"],
                      ["Recommendation", strategyExitAttributionResult?.recommendation ?? "N/A"],
                      [
                        "Best Holding",
                        strategyExitAttributionResult?.best_holding_window
                          ? String(strategyExitAttributionResult.best_holding_window)
                          : "N/A",
                      ],
                      [
                        "Worst Holding",
                        strategyExitAttributionResult?.worst_holding_window
                          ? String(strategyExitAttributionResult.worst_holding_window)
                          : "N/A",
                      ],
                      [
                        "Signals",
                        strategyExitAttributionResult
                          ? `${strategyExitAttributionResult.total_executable_signals} / raw ${strategyExitAttributionResult.total_raw_signals}`
                          : "N/A",
                      ],
                    ]}
                    loading={strategyExitAttributionMutation.isPending}
                    error={undefined}
                  />
                  <div className="grid gap-3 lg:grid-cols-2">
                    <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                      <div className="font-medium text-slate-100">Suppression Breakdown</div>
                      {(strategyExitAttributionResult?.suppression_breakdown ?? []).map((row) => (
                        <div key={row.reason} className="mt-2">
                          {row.reason}: {row.count}
                        </div>
                      ))}
                    </div>
                    <div className="overflow-x-auto rounded-xl border border-border bg-surface/40">
                      <table className="min-w-full text-left text-xs text-slate-300">
                        <thead className="text-slate-100">
                          <tr>
                            <th className="px-3 py-2">Hold</th>
                            <th className="px-3 py-2">Trades</th>
                            <th className="px-3 py-2">Win</th>
                            <th className="px-3 py-2">Avg</th>
                            <th className="px-3 py-2">Median</th>
                            <th className="px-3 py-2">Total</th>
                            <th className="px-3 py-2">Recommendation</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(strategyExitAttributionResult?.per_holding_window ?? []).map((row) => (
                            <tr key={row.holding_candles} className="border-t border-border">
                              <td className="px-3 py-2">{row.holding_candles}</td>
                              <td className="px-3 py-2">{row.trade_count}</td>
                              <td className="px-3 py-2">{formatNumber(row.win_rate)}%</td>
                              <td className="px-3 py-2">{formatNumber(row.avg_net_pnl_pct)}%</td>
                              <td className="px-3 py-2">{formatNumber(row.median_net_pnl_pct)}%</td>
                              <td className="px-3 py-2">{formatNumber(row.total_net_pnl_pct)}%</td>
                              <td className="px-3 py-2">{row.recommendation}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              </Panel>
              <Panel className="xl:col-span-12" title="Strategy Signal Feature Attribution">
                <div className="space-y-3">
                  <div className="grid gap-3 md:grid-cols-4">
                    <Field
                      label="Symbol"
                      value={strategySignalFeatureAttributionForm.symbol}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({ ...current, symbol: value }))
                      }
                    />
                    <Field
                      label="Timeframe"
                      value={strategySignalFeatureAttributionForm.timeframe}
                      as="select"
                      options={TIMEFRAME_OPTIONS}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({ ...current, timeframe: value }))
                      }
                    />
                    <Field
                      label="Start"
                      value={strategySignalFeatureAttributionForm.start_time}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({ ...current, start_time: value }))
                      }
                    />
                    <Field
                      label="End"
                      value={strategySignalFeatureAttributionForm.end_time}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({ ...current, end_time: value }))
                      }
                    />
                    <Field
                      label="Experiment Run ID"
                      value={strategySignalFeatureAttributionForm.experiment_run_id}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({ ...current, experiment_run_id: value }))
                      }
                    />
                    <Field
                      label="Holding Window"
                      value={strategySignalFeatureAttributionForm.holding_window}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({ ...current, holding_window: value }))
                      }
                    />
                    <Field
                      label="Fee Bps"
                      value={strategySignalFeatureAttributionForm.fee_bps}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({ ...current, fee_bps: value }))
                      }
                    />
                    <Field
                      label="Slippage Bps"
                      value={strategySignalFeatureAttributionForm.slippage_bps}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({ ...current, slippage_bps: value }))
                      }
                    />
                    <Field
                      label="Min Samples"
                      value={strategySignalFeatureAttributionForm.min_samples_per_bucket}
                      onChange={(value) =>
                        setStrategySignalFeatureAttributionForm((current) => ({
                          ...current,
                          min_samples_per_bucket: value,
                        }))
                      }
                    />
                  </div>
                  <ActionButton
                    label="Run Feature Attribution"
                    onClick={() => strategySignalFeatureAttributionMutation.mutate()}
                    busy={strategySignalFeatureAttributionMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(strategySignalFeatureAttributionMutation.error)}
                    success={
                      strategySignalFeatureAttributionResult
                        ? `feature attribution: ${strategySignalFeatureAttributionResult.status}`
                        : undefined
                    }
                  />
                  <KeyValue
                    items={[
                      ["Status", strategySignalFeatureAttributionResult?.status ?? "N/A"],
                      [
                        "Signals",
                        strategySignalFeatureAttributionResult
                          ? `${strategySignalFeatureAttributionResult.attributed_signals} attributed / ${strategySignalFeatureAttributionResult.executable_signals} executable / raw ${strategySignalFeatureAttributionResult.total_raw_signals}`
                          : "N/A",
                      ],
                      [
                        "Insufficient Forward",
                        String(strategySignalFeatureAttributionResult?.insufficient_forward_data_count ?? 0),
                      ],
                      [
                        "Holding Window",
                        strategySignalFeatureAttributionResult
                          ? String(strategySignalFeatureAttributionResult.holding_window)
                          : "N/A",
                      ],
                    ]}
                    loading={strategySignalFeatureAttributionMutation.isPending}
                    error={undefined}
                  />
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Recommendations</div>
                    {(strategySignalFeatureAttributionResult?.recommendations ?? []).length > 0 ? (
                      (strategySignalFeatureAttributionResult?.recommendations ?? []).map((message) => (
                        <div key={message} className="mt-2">
                          {message}
                        </div>
                      ))
                    ) : (
                      <div className="mt-2">Run feature attribution to inspect entry buckets.</div>
                    )}
                  </div>
                  <div className="grid gap-3 lg:grid-cols-2">
                    <FeatureBucketTable
                      title="Best Buckets"
                      buckets={strategySignalFeatureAttributionResult?.best_buckets ?? []}
                    />
                    <FeatureBucketTable
                      title="Worst Buckets"
                      buckets={strategySignalFeatureAttributionResult?.worst_buckets ?? []}
                    />
                  </div>
                  <FeatureBucketTable
                    title="Feature Bucket Details"
                    buckets={strategySignalFeatureAttributionResult?.feature_buckets ?? []}
                  />
                </div>
              </Panel>
            </section>
          )}

          {section === "risk" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-4" title="Risk Status">
                <KeyValue
                  items={[
                    ["Status", riskQuery.data?.status ?? "N/A"],
                    ["Market Mode", riskQuery.data?.market_mode ?? "N/A"],
                    [
                      "Paper Allowed",
                      String(riskQuery.data?.paper_trading_allowed ?? false),
                    ],
                    [
                      "Live Allowed",
                      String(riskQuery.data?.live_trading_allowed ?? false),
                    ],
                    [
                      "Kill Switch",
                      riskQuery.data?.kill_switch.enabled ? "ACTIVE" : "DISABLED",
                    ],
                  ]}
                  loading={riskQuery.isLoading}
                  error={getErrorMessage(riskQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-8" title="Risk Config">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Max Open Positions"
                    value={String(riskConfigForm.max_open_positions)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_open_positions: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Max Daily Loss %"
                    value={riskConfigForm.max_daily_loss_pct}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_daily_loss_pct: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Weekly Loss %"
                    value={riskConfigForm.max_weekly_loss_pct}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_weekly_loss_pct: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Position Notional"
                    value={riskConfigForm.max_position_notional}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_position_notional: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Slippage %"
                    value={riskConfigForm.max_slippage_pct}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_slippage_pct: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Consecutive Losses"
                    value={String(riskConfigForm.max_consecutive_losses)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_consecutive_losses: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Cooldown Seconds"
                    value={String(riskConfigForm.cooldown_seconds)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        cooldown_seconds: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Max Signal Age ms"
                    value={String(riskConfigForm.max_signal_age_ms)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_signal_age_ms: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Stale Feed Threshold s"
                    value={String(riskConfigForm.stale_feed_threshold_seconds)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        stale_feed_threshold_seconds: Number(value),
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <ActionButton
                    label="Validate"
                    onClick={() => validateRiskConfigMutation.mutate()}
                    busy={validateRiskConfigMutation.isPending}
                  />
                  <ActionButton
                    label="Update"
                    onClick={() => updateRiskConfigMutation.mutate()}
                    busy={updateRiskConfigMutation.isPending}
                  />
                </div>
                <InlineStatus
                  error={
                    getErrorMessage(validateRiskConfigMutation.error) ??
                    getErrorMessage(updateRiskConfigMutation.error)
                  }
                  success={
                    validateRiskConfigMutation.data
                      ? `validation: ${validateRiskConfigMutation.data.validation.valid ? "valid" : "rejected"}`
                      : updateRiskConfigMutation.data
                        ? "risk config updated"
                        : undefined
                  }
                />
                <div className="mt-3 rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                  <div className="font-medium text-slate-100">Validation Issues</div>
                  {(validateRiskConfigMutation.data?.validation.issues ?? []).map((issue) => (
                    <div key={`${issue.field}-${issue.code}`}>
                      {issue.severity} {issue.field}: {issue.message}
                    </div>
                  ))}
                </div>
                <div className="mt-3 rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                  <div className="font-medium text-slate-100">Config Versions</div>
                  {(riskConfigVersionsQuery.data?.versions ?? []).slice(0, 5).map((entry) => (
                    <div key={`${entry.config_id}-${entry.version}`}>
                      v{entry.version} max_open_positions={entry.config.max_open_positions} max_notional=
                      {entry.config.max_position_notional}
                    </div>
                  ))}
                </div>
                <div className="mt-3 rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                  <div className="font-medium text-slate-100">Recent Config Audit</div>
                  {(riskConfigAuditQuery.data?.audit ?? []).slice(0, 5).map((entry) => (
                    <div key={entry.audit_id}>
                      {formatDateTime(entry.created_at)} v{entry.version ?? "-"} issues=
                      {entry.validation_issues.length}
                    </div>
                  ))}
                </div>
              </Panel>
              <Panel className="xl:col-span-8" title="Recent Risk Events">
                <EventsTable
                  events={riskEvents}
                  loading={eventsQuery.isLoading}
                  error={getErrorMessage(eventsQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-8" title="Risk Decisions">
                <div className="mb-3 max-w-xs">
                  <Field
                    label="Symbol"
                    as="select"
                    value={selectedSymbol}
                    onChange={setSelectedSymbol}
                    options={dataSymbols}
                  />
                </div>
                <RiskDecisionsTable
                  decisions={riskDecisions}
                  onSelect={setSelectedRiskDecisionId}
                  selectedId={selectedRiskDecisionId}
                  loading={riskDecisionsQuery.isLoading}
                  error={getErrorMessage(riskDecisionsQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-4" title="Risk Decision Detail">
                <KeyValue
                  items={[
                    ["Decision ID", selectedRiskDecisionQuery.data?.decision.id ?? "N/A"],
                    [
                      "Signal ID",
                      selectedRiskDecisionQuery.data?.decision.signal_id ?? "N/A",
                    ],
                    [
                      "Strategy",
                      selectedRiskDecisionQuery.data?.decision.strategy_id ?? "N/A",
                    ],
                    ["Symbol", selectedRiskDecisionQuery.data?.decision.symbol ?? "N/A"],
                    ["Decision", selectedRiskDecisionQuery.data?.decision.decision ?? "N/A"],
                    [
                      "Approved Notional",
                      selectedRiskDecisionQuery.data?.decision.approved_notional ?? "N/A",
                    ],
                    [
                      "Risk Score",
                      selectedRiskDecisionQuery.data?.decision.risk_score ?? "N/A",
                    ],
                    [
                      "Reasons",
                      selectedRiskDecisionQuery.data?.decision.reasons.join(", ") || "N/A",
                    ],
                    [
                      "Correlation",
                      selectedRiskDecisionQuery.data?.decision.correlation_id ?? "N/A",
                    ],
                    [
                      "Created",
                      formatDateTime(selectedRiskDecisionQuery.data?.decision.created_at),
                    ],
                  ]}
                  loading={selectedRiskDecisionQuery.isLoading}
                  error={getErrorMessage(selectedRiskDecisionQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-6" title="Activate Kill Switch">
                <Field
                  label="Reason"
                  value={killSwitchReason}
                  onChange={setKillSwitchReason}
                  placeholder="Operator reason"
                />
                <div className="mt-3">
                  <ActionButton
                    label="Activate Kill Switch"
                    onClick={() => killSwitchMutation.mutate()}
                    tone="danger"
                    busy={killSwitchMutation.isPending}
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-6" title="Resume Paper Trading">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Confirmation"
                    value={resumeConfirmation}
                    onChange={setResumeConfirmation}
                    placeholder="RESUME TRADING"
                  />
                  <Field
                    label="Reason"
                    value={resumeReason}
                    onChange={setResumeReason}
                    placeholder="Operator reason"
                  />
                </div>
                <div className="mt-3">
                  <ActionButton
                    label="Resume"
                    onClick={() => resumeMutation.mutate()}
                    tone="warning"
                    busy={resumeMutation.isPending}
                    disabled={resumeConfirmation !== "RESUME TRADING"}
                  />
                </div>
              </Panel>
            </section>
          )}

          {section === "orders" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-8" title="Paper Orders">
                <OrdersTable orders={orders} onSelect={setSelectedOrderId} selectedId={selectedOrderId} />
              </Panel>
              <Panel className="xl:col-span-4" title="Order Detail">
                <KeyValue
                  items={[
                    ["Order ID", selectedOrderQuery.data?.order.order_id ?? "N/A"],
                    [
                      "Client Order ID",
                      selectedOrderQuery.data?.order.client_order_id ?? "N/A",
                    ],
                    [
                      "Exchange Order ID",
                      selectedOrderQuery.data?.order.exchange_order_id ?? "N/A",
                    ],
                    [
                      "Signal ID",
                      selectedOrderQuery.data?.order.signal_id ?? "N/A",
                    ],
                    [
                      "Strategy ID",
                      selectedOrderQuery.data?.order.strategy_id ?? "N/A",
                    ],
                    [
                      "Correlation ID",
                      selectedOrderQuery.data?.order.correlation_id ?? "N/A",
                    ],
                    [
                      "Execution State",
                      selectedOrderQuery.data?.order.execution_state ?? "N/A",
                    ],
                    ["Status", selectedOrderQuery.data?.order.status ?? "N/A"],
                    ["Side", selectedOrderQuery.data?.order.side ?? "N/A"],
                    ["Symbol", selectedOrderQuery.data?.order.symbol ?? "N/A"],
                    ["Mode", selectedOrderQuery.data?.order.mode ?? "N/A"],
                    [
                      "Idempotency Key",
                      selectedOrderQuery.data?.order.idempotency_key ?? "N/A",
                    ],
                    [
                      "Requested Notional",
                      selectedOrderQuery.data?.order.requested_notional ?? "N/A",
                    ],
                    ["Quantity", selectedOrderQuery.data?.order.quantity ?? "N/A"],
                    ["Filled Qty", selectedOrderQuery.data?.order.filled_qty ?? "N/A"],
                    [
                      "Avg Fill Price",
                      selectedOrderQuery.data?.order.avg_fill_price ?? "N/A",
                    ],
                    [
                      "Risk Decision ID",
                      selectedOrderQuery.data?.order.risk_decision_id ?? "N/A",
                    ],
                    [
                      "Status Reason",
                      selectedOrderQuery.data?.order.status_reason ?? "N/A",
                    ],
                    [
                      "Created",
                      formatDateTime(selectedOrderQuery.data?.order.created_at),
                    ],
                    [
                      "Updated",
                      formatDateTime(selectedOrderQuery.data?.order.updated_at),
                    ],
                  ]}
                  loading={selectedOrderQuery.isLoading}
                  error={getErrorMessage(selectedOrderQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-8" title="Paper Positions">
                <div className="mb-3 flex items-center gap-3">
                  <select
                    className="rounded-lg border border-border bg-surface px-3 py-2 text-sm text-slate-100"
                    value={paperPositionStatus}
                    onChange={(event) => setPaperPositionStatus(event.target.value)}
                  >
                    {["OPEN", "CLOSED", "ALL"].map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                  <span className="text-xs uppercase tracking-[0.2em] text-muted">
                    Simulated paper positions only
                  </span>
                </div>
                {closeTarget ? (
                  <div className="mb-4 rounded-xl border border-amber-400/40 bg-amber-500/10 p-4">
                    <div className="text-sm font-semibold text-amber-100">
                      Close {closeTarget.symbol} paper position
                    </div>
                    <div className="mt-1 text-xs text-amber-50/80">
                      Type <code>CLOSE {closeTarget.symbol}</code> to submit a simulated market close.
                    </div>
                    <div className="mt-3 grid gap-3 md:grid-cols-[2fr,1fr,auto,auto]">
                      <input
                        className="rounded-lg border border-border bg-surface px-3 py-2 text-sm text-slate-100"
                        value={closeConfirmation}
                        onChange={(event) => setCloseConfirmation(event.target.value)}
                        placeholder={`CLOSE ${closeTarget.symbol}`}
                      />
                      <select
                        className="rounded-lg border border-border bg-surface px-3 py-2 text-sm text-slate-100"
                        value={closeReason}
                        onChange={(event) => setCloseReason(event.target.value)}
                      >
                        {["manual_operator_exit", "risk_operator_exit", "emergency_exit"].map((option) => (
                          <option key={option} value={option}>
                            {option}
                          </option>
                        ))}
                      </select>
                      <ActionButton
                        label="Close Paper Position"
                        onClick={() => paperCloseMutation.mutate()}
                        busy={paperCloseMutation.isPending}
                      />
                      <button
                        className="rounded-lg border border-border px-3 py-2 text-sm text-slate-100"
                        onClick={() => {
                          setCloseTarget(null);
                          setCloseConfirmation("");
                        }}
                        type="button"
                      >
                        Cancel
                      </button>
                    </div>
                    <InlineStatus
                      error={getErrorMessage(paperCloseMutation.error)}
                      success={
                        paperCloseMutation.data
                          ? `Simulated close booked with realized PnL ${paperCloseMutation.data.realized_pnl}`
                          : undefined
                      }
                    />
                  </div>
                ) : null}
                <PaperPositionsTable positions={paperPositions} onClose={setCloseTarget} />
                <InlineStatus error={getErrorMessage(paperPositionsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-4" title="Paper Journal">
                <SimpleList
                  items={(paperJournalQuery.data?.journal ?? []).slice(0, 10).map((entry) => {
                    const symbol = entry.symbol ? ` ${entry.symbol}` : "";
                    const pnl = entry.pnl ? ` pnl=${entry.pnl}` : "";
                    return `${entry.event_type}${symbol}${pnl}`;
                  })}
                />
                <InlineStatus error={getErrorMessage(paperJournalQuery.error)} />
              </Panel>
            </section>
          )}

          {section === "analytics" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-4" title="Selected Strategy">
                <KeyValue
                  items={[
                    ["Strategy", selectedStrategyId],
                    ["Symbol", selectedSymbol],
                    ["Timeframe", selectedAnalyticsTimeframe],
                    [
                      "Window",
                      strategyPerformanceQuery.data?.summary
                        ? `${formatDateTime(strategyPerformanceQuery.data.summary.window_start)} -> ${formatDateTime(strategyPerformanceQuery.data.summary.window_end)}`
                        : "Last 7 days",
                    ],
                  ]}
                  loading={strategyPerformanceQuery.isLoading}
                  error={getErrorMessage(strategyPerformanceQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="Combined Performance">
                <KeyValue
                  items={[
                    ["Runs", String(strategyPerformanceQuery.data?.summary.total_runs ?? 0)],
                    ["Signals", String(strategyPerformanceQuery.data?.summary.total_signals ?? 0)],
                    [
                      "Risk Rejection Rate",
                      formatNumber(strategyPerformanceQuery.data?.summary.risk_rejection_rate),
                    ],
                    [
                      "Realized PnL",
                      formatNumber(strategyPerformanceQuery.data?.summary.realized_pnl),
                    ],
                    [
                      "Unrealized PnL",
                      formatNumber(strategyPerformanceQuery.data?.summary.unrealized_pnl),
                    ],
                    [
                      "Win Rate",
                      formatNumber(strategyPerformanceQuery.data?.summary.win_rate),
                    ],
                  ]}
                  loading={strategyPerformanceQuery.isLoading}
                  error={getErrorMessage(strategyPerformanceQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="Operator Questions">
                <KeyValue
                  items={[
                    [
                      "Most Would-Submit",
                      shadowRankingsQuery.data?.rankings[0]?.strategy_id ?? "Insufficient data",
                    ],
                    [
                      "Most Risk-Rejected",
                      [...(strategyRankingsQuery.data?.rankings ?? [])]
                        .sort((left, right) => right.rejected_risk_decisions - left.rejected_risk_decisions)[0]
                        ?.strategy_id ?? "Insufficient data",
                    ],
                    [
                      "Best Paper Realized PnL",
                      [...(strategyRankingsQuery.data?.rankings ?? [])]
                        .sort((left, right) => Number(right.realized_pnl) - Number(left.realized_pnl))[0]
                        ?.strategy_id ?? "Insufficient data",
                    ],
                    [
                      "Best Backtest Avg PnL",
                      strategyRankingsQuery.data?.rankings[0]?.strategy_id ?? "Insufficient data",
                    ],
                    [
                      "Selected Symbol No-Signal",
                      String(strategyDecisionBreakdownQuery.data?.breakdown.no_signal_count ?? 0),
                    ],
                  ]}
                  loading={strategyRankingsQuery.isLoading || shadowRankingsQuery.isLoading}
                  error={
                    getErrorMessage(strategyRankingsQuery.error) ??
                    getErrorMessage(shadowRankingsQuery.error)
                  }
                />
              </Panel>

              <Panel className="xl:col-span-7" title="Strategy Rankings">
                <AnalyticsRankingsTable rankings={strategyRankingsQuery.data?.rankings ?? []} />
                <InlineStatus error={getErrorMessage(strategyRankingsQuery.error)} />
              </Panel>

              <Panel className="xl:col-span-5" title="Shadow Decision Breakdown">
                <KeyValue
                  items={[
                    [
                      "Would Submit",
                      String(strategyDecisionBreakdownQuery.data?.breakdown.would_submit_count ?? 0),
                    ],
                    [
                      "No Signal",
                      String(strategyDecisionBreakdownQuery.data?.breakdown.no_signal_count ?? 0),
                    ],
                    [
                      "Risk Rejected",
                      String(strategyDecisionBreakdownQuery.data?.breakdown.risk_rejected_count ?? 0),
                    ],
                    [
                      "Skipped",
                      String(strategyDecisionBreakdownQuery.data?.breakdown.skipped_count ?? 0),
                    ],
                    ["Errors", String(strategyDecisionBreakdownQuery.data?.breakdown.error_count ?? 0)],
                  ]}
                  loading={strategyDecisionBreakdownQuery.isLoading}
                  error={getErrorMessage(strategyDecisionBreakdownQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-6" title="Paper PnL Breakdown">
                <AnalyticsPnlBreakdownCard
                  breakdown={strategyPaperPnlBreakdownQuery.data?.breakdown}
                  loading={strategyPaperPnlBreakdownQuery.isLoading}
                  error={getErrorMessage(strategyPaperPnlBreakdownQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-6" title="Backtest Summary">
                <AnalyticsPnlBreakdownCard
                  breakdown={strategyBacktestBreakdownQuery.data?.breakdown}
                  loading={strategyBacktestBreakdownQuery.isLoading}
                  error={getErrorMessage(strategyBacktestBreakdownQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-7" title="Promotion Funnel">
                <TestnetPromotionFunnelCards
                  summary={testnetPromotionFunnelQuery.data?.summary}
                  loading={testnetPromotionFunnelQuery.isLoading}
                  error={getErrorMessage(testnetPromotionFunnelQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-5" title="Promotion Rates">
                <KeyValue
                  items={[
                    [
                      "Preview Rate",
                      formatPercent(testnetPromotionFunnelQuery.data?.summary.preview_rate_pct),
                    ],
                    [
                      "Submit Rate",
                      formatPercent(testnetPromotionFunnelQuery.data?.summary.submit_rate_pct),
                    ],
                    [
                      "Fill Rate",
                      formatPercent(testnetPromotionFunnelQuery.data?.summary.fill_rate_pct),
                    ],
                    [
                      "Reconciliation Required Rate",
                      formatPercent(
                        testnetPromotionFunnelQuery.data?.summary
                          .reconciliation_required_rate_pct,
                      ),
                    ],
                    [
                      "Shadow -> Preview Avg Seconds",
                      formatNumber(
                        testnetPromotionFunnelQuery.data?.summary
                          .avg_time_shadow_to_preview_seconds,
                      ),
                    ],
                    [
                      "Preview -> Submit Avg Seconds",
                      formatNumber(
                        testnetPromotionFunnelQuery.data?.summary
                          .avg_time_preview_to_submit_seconds,
                      ),
                    ],
                  ]}
                  loading={testnetPromotionFunnelQuery.isLoading}
                  error={getErrorMessage(testnetPromotionFunnelQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-12" title="Recent Promotion Rows">
                <TestnetPromotionRowsTable
                  rows={testnetPromotionRowsQuery.data?.rows ?? []}
                  loading={testnetPromotionRowsQuery.isLoading}
                  error={getErrorMessage(testnetPromotionRowsQuery.error)}
                />
              </Panel>
            </section>
          )}

          {section === "reports" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-5" title="Daily Report Form">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Start Time"
                    value={reportForm.start_time ?? ""}
                    onChange={(value) =>
                      setReportForm((current) => ({ ...current, start_time: value }))
                    }
                  />
                  <Field
                    label="End Time"
                    value={reportForm.end_time ?? ""}
                    onChange={(value) =>
                      setReportForm((current) => ({ ...current, end_time: value }))
                    }
                  />
                  <Field
                    label="Symbol"
                    value={reportForm.symbol ?? ""}
                    onChange={(value) =>
                      setReportForm((current) => ({ ...current, symbol: value || undefined }))
                    }
                  />
                  <Field
                    label="Strategy"
                    value={reportForm.strategy_id ?? ""}
                    onChange={(value) =>
                      setReportForm((current) => ({
                        ...current,
                        strategy_id: value || undefined,
                      }))
                    }
                  />
                  <Field
                    label="Interval"
                    value={reportForm.interval ?? ""}
                    onChange={(value) =>
                      setReportForm((current) => ({ ...current, interval: value || undefined }))
                    }
                  />
                  <Field
                    label="Format"
                    value={reportForm.format ?? "MARKDOWN"}
                    onChange={(value) =>
                      setReportForm((current) => ({
                        ...current,
                        format: value as OperatorReportRequest["format"],
                      }))
                    }
                    as="select"
                    options={["MARKDOWN", "JSON"]}
                  />
                  <label className="block text-sm">
                    <span className="text-xs uppercase tracking-[0.18em] text-muted">
                      Persist Report
                    </span>
                    <div className="mt-3 flex items-center gap-2">
                      <input
                        checked={Boolean(reportForm.persist)}
                        className="h-4 w-4"
                        type="checkbox"
                        onChange={(event) =>
                          setReportForm((current) => ({
                            ...current,
                            persist: event.target.checked,
                          }))
                        }
                      />
                      <span className="text-sm text-slate-200">
                        Store in `operator_reports`
                      </span>
                    </div>
                  </label>
                </div>
                <div className="mt-4 flex items-center gap-3">
                  <ActionButton
                    label="Generate Report"
                    onClick={() => operatorReportMutation.mutate()}
                    busy={operatorReportMutation.isPending}
                  />
                  <InlineStatus error={getErrorMessage(operatorReportMutation.error)} />
                </div>
              </Panel>

              <Panel className="xl:col-span-7" title="Generated Summary">
                <KeyValue
                  items={[
                    ["Report ID", activeOperatorReport?.report_id ?? "N/A"],
                    ["Status", activeOperatorReport?.status ?? "N/A"],
                    [
                      "Window",
                      activeOperatorReport
                        ? `${formatDateTime(activeOperatorReport.window_start)} -> ${formatDateTime(activeOperatorReport.window_end)}`
                        : "N/A",
                    ],
                    [
                      "Findings",
                      String(activeOperatorReport?.summary.total_findings ?? 0),
                    ],
                    [
                      "Highest Severity",
                      activeOperatorReport?.summary.highest_severity ?? "N/A",
                    ],
                    [
                      "Risk Rejection Rate %",
                      formatNumber(activeOperatorReport?.summary.risk_rejection_rate_pct),
                    ],
                    [
                      "Paper Daily PnL",
                      formatNumber(activeOperatorReport?.summary.paper_daily_pnl),
                    ],
                    [
                      "Reconciliation Required",
                      String(activeOperatorReport?.summary.reconciliation_required_count ?? 0),
                    ],
                  ]}
                  loading={operatorReportMutation.isPending || selectedOperatorReportQuery.isLoading}
                  error={
                    getErrorMessage(operatorReportMutation.error) ??
                    getErrorMessage(selectedOperatorReportQuery.error)
                  }
                />
              </Panel>

              <Panel className="xl:col-span-6" title="Findings">
                <OperatorReportFindingsTable findings={activeOperatorReport?.findings ?? []} />
              </Panel>

              <Panel className="xl:col-span-6" title="Recommendations">
                <SimpleList
                  items={(activeOperatorReport?.recommendations ?? []).map(
                    (recommendation) =>
                      `${recommendation.priority}: ${recommendation.detail}`,
                  )}
                />
              </Panel>

              <Panel className="xl:col-span-7" title="Section Snapshots">
                <OperatorReportSections sections={activeOperatorReport?.sections ?? []} />
              </Panel>

              <Panel className="xl:col-span-5" title="Persisted Reports">
                <OperatorReportList
                  reports={operatorReportsQuery.data?.reports ?? []}
                  loading={operatorReportsQuery.isLoading}
                  error={getErrorMessage(operatorReportsQuery.error)}
                  selectedReportId={selectedReportId}
                  onSelect={setSelectedReportId}
                />
              </Panel>

              <Panel className="xl:col-span-12" title="Markdown Preview">
                {activeOperatorReport?.markdown ? (
                  <pre className="overflow-x-auto whitespace-pre-wrap rounded-xl border border-border bg-surface/60 p-4 text-sm text-slate-100">
                    {activeOperatorReport.markdown}
                  </pre>
                ) : (
                  <EmptyState label="Generate a report to preview markdown." />
                )}
              </Panel>
            </section>
          )}

          {section === "backtests" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-7" title="Backtest Run Form">
                <div className="grid gap-3 md:grid-cols-3">
                  {(
                    [
                      ["strategy_id", "Strategy ID"],
                      ["symbol", "Symbol"],
                      ["timeframe", "Timeframe"],
                      ["start_time", "Start Time"],
                      ["end_time", "End Time"],
                      ["initial_capital", "Initial Capital"],
                      ["fee_bps", "Fee Bps"],
                      ["slippage_bps", "Slippage Bps"],
                    ] as const
                  ).map(([key, label]) => (
                    <Field
                      key={key}
                      label={label}
                      value={String(backtestForm[key])}
                      onChange={(value) =>
                        setBacktestForm((current) => ({ ...current, [key]: value }))
                      }
                    />
                  ))}
                  <Field
                    label="Holding Candles"
                    value={String(backtestForm.holding_candles ?? 3)}
                    onChange={(value) =>
                      setBacktestForm((current) => ({
                        ...current,
                        holding_candles: Number(value),
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Run Backtest"
                    onClick={() => runBacktestMutation.mutate()}
                    busy={runBacktestMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(runBacktestMutation.error)}
                    success={
                      lastBacktestResult
                        ? `Run ${shortenId(lastBacktestResult.run_id)} completed with pnl ${lastBacktestResult.pnl}`
                        : undefined
                    }
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-5" title="Run Result Summary">
                <KeyValue
                  items={[
                    ["Run ID", lastBacktestResult?.run_id ?? selectedRunQuery.data?.run.run_id ?? "N/A"],
                    ["Status", lastBacktestResult?.status ?? selectedRunQuery.data?.run.status ?? "N/A"],
                    ["PnL", lastBacktestResult?.pnl ?? selectedRunQuery.data?.run.pnl ?? "N/A"],
                    ["PnL %", lastBacktestResult?.pnl_pct ?? selectedRunQuery.data?.run.pnl_pct ?? "N/A"],
                    [
                      "Max Drawdown %",
                      lastBacktestResult?.max_drawdown_pct ??
                        selectedRunQuery.data?.run.max_drawdown_pct ??
                        "N/A",
                    ],
                    [
                      "Win Rate",
                      lastBacktestResult?.win_rate ??
                        selectedRunQuery.data?.run.win_rate ??
                        "N/A",
                    ],
                    [
                      "Signals",
                      selectedRunQuery.data?.run
                        ? `raw ${selectedRunQuery.data.run.raw_signal_count ?? 0} / executed ${
                            selectedRunQuery.data.run.executed_trade_count ??
                            selectedRunQuery.data.run.trade_count
                          }`
                        : lastBacktestResult
                          ? `raw ${lastBacktestResult.raw_signal_count ?? 0} / executed ${
                              lastBacktestResult.executed_trade_count ??
                              lastBacktestResult.trade_count
                            }`
                          : "N/A",
                    ],
                    [
                      "Suppressed",
                      selectedRunQuery.data?.run
                        ? `cooldown ${
                            selectedRunQuery.data.run.cooldown_suppressed_count ?? 0
                          } / open ${
                            selectedRunQuery.data.run.open_position_suppressed_count ?? 0
                          }`
                        : lastBacktestResult
                          ? `cooldown ${
                              lastBacktestResult.cooldown_suppressed_count ?? 0
                            } / open ${
                              lastBacktestResult.open_position_suppressed_count ?? 0
                            }`
                          : "N/A",
                    ],
                  ]}
                  loading={selectedRunQuery.isLoading}
                  error={getErrorMessage(selectedRunQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-4" title="Recent Backtest Runs">
                <BacktestRunsTable runs={backtestRuns} onSelect={setSelectedRunId} selectedId={selectedRunId} />
              </Panel>
              <Panel className="xl:col-span-4" title="Selected Run Trades">
                <BacktestTradesTable trades={selectedRunTradesQuery.data?.trades ?? []} />
                <InlineStatus error={getErrorMessage(selectedRunTradesQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-4" title="Selected Run Equity">
                <BacktestEquityTable equity={selectedRunEquityQuery.data?.equity ?? []} />
                <InlineStatus error={getErrorMessage(selectedRunEquityQuery.error)} />
              </Panel>
            </section>
          )}

          {section === "experiments" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <ResearchRobustnessMatrixPanel />
              <Panel className="xl:col-span-12" title="Regime Calibration">
                <div className="grid gap-3 md:grid-cols-4">
                  <Field
                    label="Symbol"
                    value={researchRegimeCalibrationForm.symbol}
                    onChange={(value) =>
                      setResearchRegimeCalibrationForm((current) => ({ ...current, symbol: value }))
                    }
                  />
                  <Field
                    label="Timeframe"
                    value={researchRegimeCalibrationForm.timeframe}
                    onChange={(value) =>
                      setResearchRegimeCalibrationForm((current) => ({ ...current, timeframe: value }))
                    }
                  />
                  <Field
                    label="Scan Start"
                    value={researchRegimeCalibrationForm.scan_start}
                    onChange={(value) =>
                      setResearchRegimeCalibrationForm((current) => ({ ...current, scan_start: value }))
                    }
                  />
                  <Field
                    label="Scan End"
                    value={researchRegimeCalibrationForm.scan_end}
                    onChange={(value) =>
                      setResearchRegimeCalibrationForm((current) => ({ ...current, scan_end: value }))
                    }
                  />
                  <Field
                    label="Window Hours"
                    value={String(researchRegimeCalibrationForm.window_hours)}
                    onChange={(value) =>
                      setResearchRegimeCalibrationForm((current) => ({
                        ...current,
                        window_hours: Number(value) || 24,
                      }))
                    }
                  />
                  <Field
                    label="Step Hours"
                    value={String(researchRegimeCalibrationForm.step_hours)}
                    onChange={(value) =>
                      setResearchRegimeCalibrationForm((current) => ({
                        ...current,
                        step_hours: Number(value) || 12,
                      }))
                    }
                  />
                  <Field
                    label="Min / Regime"
                    value={String(researchRegimeCalibrationForm.target_min_windows_per_regime ?? 5)}
                    onChange={(value) =>
                      setResearchRegimeCalibrationForm((current) => ({
                        ...current,
                        target_min_windows_per_regime: Number(value) || 5,
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <ActionButton
                    label="Run Calibration"
                    onClick={() => researchRegimeCalibrationMutation.mutate()}
                    busy={researchRegimeCalibrationMutation.isPending}
                    disabled={user.role === "VIEWER"}
                  />
                  <InlineStatus
                    error={getErrorMessage(researchRegimeCalibrationMutation.error)}
                    success={
                      lastResearchRegimeCalibration
                        ? `Calibration ${shortenId(lastResearchRegimeCalibration.calibration_id)} ${lastResearchRegimeCalibration.status}`
                        : undefined
                    }
                  />
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-3">
                  <ResearchRegimeCalibrationsTable
                    calibrations={researchRegimeCalibrations}
                    selectedId={selectedResearchRegimeCalibrationId}
                    onSelect={setSelectedResearchRegimeCalibrationId}
                  />
                  <ResearchRegimeCalibrationRecommended calibration={selectedResearchRegimeCalibration} />
                  <ResearchRegimeCalibrationTopConfigs
                    calibration={selectedResearchRegimeCalibration}
                    candidates={selectedResearchRegimeCalibrationCandidates}
                  />
                  <ResearchRegimeCalibrationSamples calibration={selectedResearchRegimeCalibration} />
                </div>
                <InlineStatus
                  error={
                    getErrorMessage(researchRegimeCalibrationsQuery.error) ||
                    getErrorMessage(selectedResearchRegimeCalibrationQuery.error) ||
                    getErrorMessage(selectedResearchRegimeCalibrationCandidatesQuery.error)
                  }
                />
              </Panel>
              <Panel className="xl:col-span-12" title="Regime Discovery">
                <div className="grid gap-3 md:grid-cols-4">
                  <Field
                    label="Symbol"
                    value={researchRegimeDiscoveryForm.symbol}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({ ...current, symbol: value }))
                    }
                  />
                  <Field
                    label="Timeframe"
                    value={researchRegimeDiscoveryForm.timeframe}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({ ...current, timeframe: value }))
                    }
                  />
                  <Field
                    label="Scan Start"
                    value={researchRegimeDiscoveryForm.scan_start}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({ ...current, scan_start: value }))
                    }
                  />
                  <Field
                    label="Scan End"
                    value={researchRegimeDiscoveryForm.scan_end}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({ ...current, scan_end: value }))
                    }
                  />
                  <Field
                    label="Window Hours"
                    value={String(researchRegimeDiscoveryForm.window_hours)}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({
                        ...current,
                        window_hours: Number(value) || 24,
                      }))
                    }
                  />
                  <Field
                    label="Step Hours"
                    value={String(researchRegimeDiscoveryForm.step_hours)}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({
                        ...current,
                        step_hours: Number(value) || 12,
                      }))
                    }
                  />
                  <Field
                    label="Max / Regime"
                    value={String(researchRegimeDiscoveryForm.max_windows_per_regime ?? 10)}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({
                        ...current,
                        max_windows_per_regime: Number(value) || 10,
                      }))
                    }
                  />
                  <Field
                    label="Min Confidence"
                    value={researchRegimeDiscoveryForm.min_confidence ?? ""}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({
                        ...current,
                        min_confidence: value || null,
                      }))
                    }
                  />
                  <Field
                    label="Target Regimes"
                    value={researchRegimeDiscoveryForm.target_regimes?.join(",") ?? ""}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({
                        ...current,
                        target_regimes: parseStringList(value) as ResearchRegimeDiscoveryRequest["target_regimes"],
                      }))
                    }
                  />
                  <Field
                    label="Calibration ID"
                    value={researchRegimeDiscoveryForm.calibration_id ?? ""}
                    onChange={(value) =>
                      setResearchRegimeDiscoveryForm((current) => ({
                        ...current,
                        calibration_id: value || null,
                        classifier_config: null,
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <label className="flex items-center gap-2 text-sm text-slate-300">
                    <input
                      type="checkbox"
                      checked={researchRegimeDiscoveryForm.require_existing_candles ?? true}
                      onChange={(event) =>
                        setResearchRegimeDiscoveryForm((current) => ({
                          ...current,
                          require_existing_candles: event.target.checked,
                        }))
                      }
                    />
                    Require existing candles
                  </label>
                  <ActionButton
                    label="Run Discovery"
                    onClick={() => researchRegimeDiscoveryMutation.mutate()}
                    busy={researchRegimeDiscoveryMutation.isPending}
                    disabled={user.role === "VIEWER"}
                  />
                  <ActionButton
                    label="Create Dataset"
                    onClick={() => researchRegimeDatasetFromDiscoveryMutation.mutate()}
                    busy={researchRegimeDatasetFromDiscoveryMutation.isPending}
                    disabled={user.role === "VIEWER" || !selectedResearchRegimeDiscoveryId}
                  />
                  <InlineStatus
                    error={
                      getErrorMessage(researchRegimeDiscoveryMutation.error) ||
                      getErrorMessage(researchRegimeDatasetFromDiscoveryMutation.error)
                    }
                    success={
                      lastResearchRegimeDiscovery
                        ? `Discovery ${shortenId(lastResearchRegimeDiscovery.discovery_id)} ${lastResearchRegimeDiscovery.status}`
                        : undefined
                    }
                  />
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-3">
                  <ResearchRegimeDiscoveriesTable
                    discoveries={researchRegimeDiscoveries}
                    selectedId={selectedResearchRegimeDiscoveryId}
                    onSelect={setSelectedResearchRegimeDiscoveryId}
                  />
                  <ResearchRegimeDiscoverySummaryTable discovery={selectedResearchRegimeDiscovery} />
                  <div>
                    <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
                      Recommendations
                    </div>
                    <SimpleList
                      items={[
                        ...(selectedResearchRegimeDiscovery?.missing_regimes.length
                          ? [
                              `Missing regimes: ${selectedResearchRegimeDiscovery.missing_regimes.join(", ")}`,
                            ]
                          : []),
                        ...(selectedResearchRegimeDiscovery?.recommendations ?? []).map(
                          (recommendation) =>
                            `${recommendation.priority} ${recommendation.code}: ${recommendation.message}`,
                        ),
                      ]}
                    />
                    <InlineStatus
                      error={getErrorMessage(selectedResearchRegimeDiscoveryQuery.error)}
                      success={selectedResearchRegimeDiscoveryQuery.isLoading ? "Loading discovery" : undefined}
                    />
                  </div>
                </div>
                <div className="mt-4">
                  <ResearchRegimeDiscoveryWindowsTable windows={selectedResearchRegimeDiscoveryWindows} />
                  <InlineStatus
                    error={getErrorMessage(selectedResearchRegimeDiscoveryWindowsQuery.error)}
                    success={
                      selectedResearchRegimeDiscoveryWindowsQuery.isLoading
                        ? "Loading discovery windows"
                        : undefined
                    }
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-12" title="Regime Datasets">
                <div className="grid gap-3 md:grid-cols-4">
                  <Field
                    label="Symbol"
                    value={researchRegimeDatasetForm.symbol}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({ ...current, symbol: value }))
                    }
                  />
                  <Field
                    label="Timeframe"
                    value={researchRegimeDatasetForm.timeframe}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({ ...current, timeframe: value }))
                    }
                  />
                  <Field
                    label="Start"
                    value={researchRegimeDatasetForm.start_time}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({ ...current, start_time: value }))
                    }
                  />
                  <Field
                    label="End"
                    value={researchRegimeDatasetForm.end_time}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({ ...current, end_time: value }))
                    }
                  />
                  <Field
                    label="Window Hours"
                    value={String(researchRegimeDatasetForm.window_hours)}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({
                        ...current,
                        window_hours: Number(value) || 24,
                      }))
                    }
                  />
                  <Field
                    label="Step Hours"
                    value={String(researchRegimeDatasetForm.step_hours)}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({
                        ...current,
                        step_hours: Number(value) || 12,
                      }))
                    }
                  />
                  <Field
                    label="Min Candles"
                    value={String(researchRegimeDatasetForm.min_candles_per_window)}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({
                        ...current,
                        min_candles_per_window: Number(value) || 5,
                      }))
                    }
                  />
                  <Field
                    label="Max / Regime"
                    value={String(researchRegimeDatasetForm.max_windows_per_regime ?? "")}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({
                        ...current,
                        max_windows_per_regime: value ? Number(value) || null : null,
                      }))
                    }
                  />
                  <Field
                    label="Target Regimes"
                    value={researchRegimeDatasetForm.target_regimes?.join(",") ?? ""}
                    onChange={(value) =>
                      setResearchRegimeDatasetForm((current) => ({
                        ...current,
                        target_regimes: parseStringList(value) as ResearchRegimeDatasetRequest["target_regimes"],
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <label className="flex items-center gap-2 text-sm text-slate-300">
                    <input
                      type="checkbox"
                      checked={researchRegimeDatasetForm.require_good_data_quality ?? true}
                      onChange={(event) =>
                        setResearchRegimeDatasetForm((current) => ({
                          ...current,
                          require_good_data_quality: event.target.checked,
                        }))
                      }
                    />
                    Require good data quality
                  </label>
                  <ActionButton
                    label="Build Dataset"
                    onClick={() => researchRegimeDatasetMutation.mutate()}
                    busy={researchRegimeDatasetMutation.isPending}
                    disabled={user.role === "VIEWER"}
                  />
                  <InlineStatus
                    error={getErrorMessage(researchRegimeDatasetMutation.error)}
                    success={
                      lastResearchRegimeDataset
                        ? `Dataset ${shortenId(lastResearchRegimeDataset.dataset_id)} ${lastResearchRegimeDataset.status}`
                        : undefined
                    }
                  />
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-3">
                  <ResearchRegimeDatasetsTable
                    datasets={researchRegimeDatasets}
                    selectedId={selectedResearchRegimeDatasetId}
                    onSelect={setSelectedResearchRegimeDatasetId}
                  />
                  <ResearchRegimeSummaryTable dataset={selectedResearchRegimeDataset} />
                  <div>
                    <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
                      Recommendations
                    </div>
                    <SimpleList
                      items={[
                        ...(selectedResearchRegimeDataset?.summary.missing_regimes.length
                          ? [
                              `Missing regimes: ${selectedResearchRegimeDataset.summary.missing_regimes.join(", ")}`,
                            ]
                          : []),
                        ...(selectedResearchRegimeDataset?.summary.recommendations ?? []).map(
                          (recommendation) =>
                            `${recommendation.priority} ${recommendation.code}: ${recommendation.message}`,
                        ),
                      ]}
                    />
                    <InlineStatus
                      error={getErrorMessage(selectedResearchRegimeDatasetQuery.error)}
                      success={selectedResearchRegimeDatasetQuery.isLoading ? "Loading dataset" : undefined}
                    />
                  </div>
                </div>
                <div className="mt-4">
                  <ResearchRegimeWindowsTable windows={selectedResearchRegimeWindows} />
                  <InlineStatus
                    error={getErrorMessage(selectedResearchRegimeDatasetWindowsQuery.error)}
                    success={
                      selectedResearchRegimeDatasetWindowsQuery.isLoading ? "Loading windows" : undefined
                    }
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-12" title="Research Campaigns">
                <div className="grid gap-3 md:grid-cols-4">
                  <Field
                    label="Strategies"
                    value={researchCampaignForm.strategies.join(",")}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        strategies: parseStringList(value),
                      }))
                    }
                  />
                  <Field
                    label="Symbols"
                    value={researchCampaignForm.symbols.join(",")}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        symbols: parseStringList(value),
                      }))
                    }
                  />
                  <Field
                    label="Timeframes"
                    value={researchCampaignForm.experiment_timeframes.join(",")}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        experiment_timeframes: parseStringList(value),
                      }))
                    }
                  />
                  <Field
                    label="Start"
                    value={researchCampaignForm.campaign_start ?? ""}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        campaign_start: value || null,
                      }))
                    }
                  />
                  <Field
                    label="End"
                    value={researchCampaignForm.campaign_end ?? ""}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        campaign_end: value || null,
                      }))
                    }
                  />
                  <Field
                    label="Window Hours"
                    value={String(researchCampaignForm.window_hours ?? 24)}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        window_hours: Number(value) || 24,
                      }))
                    }
                  />
                  <Field
                    label="Step Hours"
                    value={String(researchCampaignForm.step_hours ?? 24)}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        step_hours: Number(value) || 24,
                      }))
                    }
                  />
                  <Field
                    label="Initial Capital"
                    value={researchCampaignForm.initial_capital}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        initial_capital: value,
                      }))
                    }
                  />
                  <Field
                    label="Fee Bps"
                    value={researchCampaignForm.fee_bps}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({ ...current, fee_bps: value }))
                    }
                  />
                  <Field
                    label="Slippage Bps"
                    value={researchCampaignForm.slippage_bps}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        slippage_bps: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Batches"
                    value={String(researchCampaignForm.max_batches ?? "")}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        max_batches: value ? Number(value) || null : null,
                      }))
                    }
                  />
                  <Field
                    label="Regime Dataset ID"
                    value={researchCampaignForm.regime_dataset_id ?? ""}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        regime_dataset_id: value || null,
                      }))
                    }
                  />
                  <Field
                    label="Target Regimes"
                    value={researchCampaignForm.target_regimes?.join(",") ?? ""}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        target_regimes: parseStringList(value) as ResearchCampaignRequest["target_regimes"],
                      }))
                    }
                  />
                  <Field
                    label="Max Windows / Regime"
                    value={String(researchCampaignForm.max_windows_per_regime ?? "")}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        max_windows_per_regime: value ? Number(value) || null : null,
                      }))
                    }
                  />
                  <Field
                    label="Max Candidates / Batch"
                    value={String(researchCampaignForm.max_candidates_per_batch ?? 2)}
                    onChange={(value) =>
                      setResearchCampaignForm((current) => ({
                        ...current,
                        max_candidates_per_batch: Number(value) || 2,
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <label className="flex items-center gap-2 text-sm text-slate-300">
                    <input
                      type="checkbox"
                      checked={researchCampaignForm.repair_degraded_data ?? true}
                      onChange={(event) =>
                        setResearchCampaignForm((current) => ({
                          ...current,
                          repair_degraded_data: event.target.checked,
                        }))
                      }
                    />
                    Repair degraded data
                  </label>
                  <ActionButton
                    label="Run Campaign"
                    onClick={() => researchCampaignMutation.mutate()}
                    busy={researchCampaignMutation.isPending}
                    disabled={user.role === "VIEWER"}
                  />
                  <InlineStatus
                    error={getErrorMessage(researchCampaignMutation.error)}
                    success={
                      lastResearchCampaign
                        ? `Campaign ${shortenId(lastResearchCampaign.campaign_id)} ${lastResearchCampaign.status}`
                        : undefined
                    }
                  />
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-3">
                  <div>
                    <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
                      Recent Campaigns
                    </div>
                    <ResearchCampaignsTable
                      campaigns={researchCampaigns}
                      selectedId={selectedResearchCampaignId}
                      onSelect={setSelectedResearchCampaignId}
                    />
                  </div>
                  <div>
                    <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
                      Summary
                    </div>
                    <KeyValue
                      items={[
                        ["Campaign ID", selectedResearchCampaign?.campaign_id ?? "N/A"],
                        ["Status", selectedResearchCampaign?.status ?? "N/A"],
                        [
                          "Batches",
                          selectedResearchCampaign
                            ? `${selectedResearchCampaign.summary.total_batches_completed}/${selectedResearchCampaign.summary.total_batches_planned}`
                            : "N/A",
                        ],
                        [
                          "Failed",
                          String(selectedResearchCampaign?.summary.total_batches_failed ?? 0),
                        ],
                        [
                          "Actionable",
                          String(selectedResearchCampaign?.summary.actionable_batches ?? 0),
                        ],
                        [
                          "Overfit",
                          String(selectedResearchCampaign?.summary.overfit_only_batches ?? 0),
                        ],
                        ["Weak", String(selectedResearchCampaign?.summary.weak_batches ?? 0)],
                        [
                          "Best",
                          selectedResearchCampaign?.summary.best_strategy_symbol_timeframe ?? "-",
                        ],
                      ]}
                      loading={selectedResearchCampaignQuery.isLoading}
                      error={getErrorMessage(selectedResearchCampaignQuery.error)}
                    />
                  </div>
                  <div>
                    <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
                      Findings / Recommendations
                    </div>
                    <SimpleList
                      items={[
                        ...(selectedResearchCampaign?.summary.findings ?? []).map(
                          (finding) => `${finding.severity} ${finding.code}: ${finding.message}`,
                        ),
                        ...(selectedResearchCampaign?.summary.recommendations ?? []).map(
                          (recommendation) =>
                            `${recommendation.priority} ${recommendation.code}: ${recommendation.message}`,
                        ),
                      ]}
                    />
                  </div>
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-2">
                  <ResearchCampaignFailureAttributionCard
                    attribution={selectedResearchCampaignFailureAttribution}
                    loading={selectedResearchCampaignFailureAttributionQuery.isLoading}
                    error={getErrorMessage(selectedResearchCampaignFailureAttributionQuery.error)}
                  />
                  <ResearchCampaignRegimeSummaryTable
                    attribution={selectedResearchCampaignFailureAttribution}
                  />
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-2">
                  <ResearchCampaignRegimeLeaderboardCard
                    leaderboard={selectedResearchCampaignRegimeLeaderboard}
                    loading={selectedResearchCampaignRegimeLeaderboardQuery.isLoading}
                    error={getErrorMessage(selectedResearchCampaignRegimeLeaderboardQuery.error)}
                  />
                  <ResearchCampaignRegimeLeaderboardTable
                    leaderboard={selectedResearchCampaignRegimeLeaderboard}
                  />
                </div>
                <div className="mt-4">
                  <ResearchCampaignOverallLeaderboardTable
                    leaderboard={selectedResearchCampaignRegimeLeaderboard}
                  />
                </div>
                <div className="mt-4">
                  <ResearchHypothesesPanel
                    hypotheses={filteredResearchHypotheses}
                    selectedHypothesis={selectedResearchHypothesis}
                    priorityFilter={researchHypothesisPriorityFilter}
                    statusFilter={researchHypothesisStatusFilter}
                    loading={researchHypothesesQuery.isLoading}
                    error={getErrorMessage(researchHypothesesQuery.error)}
                    generateBusy={generateResearchHypothesesMutation.isPending}
                    decideBusy={decideResearchHypothesisMutation.isPending}
                    canMutate={user.role === "OWNER" || user.role === "OPERATOR"}
                    onPriorityFilter={setResearchHypothesisPriorityFilter}
                    onStatusFilter={setResearchHypothesisStatusFilter}
                    onSelect={setSelectedResearchHypothesisId}
                    onGenerate={() => generateResearchHypothesesMutation.mutate()}
                    onDecide={(id, decision) =>
                      decideResearchHypothesisMutation.mutate({ id, decision })
                    }
                  />
                </div>
                <div className="mt-4">
                  <ResearchCampaignFailureReasonsTable
                    attribution={selectedResearchCampaignFailureAttribution}
                  />
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-2">
                  <ResearchCampaignBatchTable
                    batches={selectedResearchCampaign?.batches ?? []}
                  />
                  <ResearchCampaignTopCandidatesTable
                    candidates={selectedResearchCampaign?.summary.top_candidates ?? []}
                    onSelectCandidate={setSelectedResearchCandidateId}
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-12" title="Research Batches">
                <div className="grid gap-3 md:grid-cols-4">
                  {(
                    [
                      ["strategy_id", "Strategy ID"],
                      ["symbol", "Symbol"],
                      ["base_interval", "Base Interval"],
                      ["start_time", "Start Time"],
                      ["end_time", "End Time"],
                      ["initial_capital", "Initial Capital"],
                      ["fee_bps", "Fee Bps"],
                      ["slippage_bps", "Slippage Bps"],
                    ] as const
                  ).map(([key, label]) => (
                    <Field
                      key={key}
                      label={label}
                      value={String(researchBatchForm[key] ?? "")}
                      onChange={(value) =>
                        setResearchBatchForm((current) => ({ ...current, [key]: value }))
                      }
                    />
                  ))}
                  <Field
                    label="Target Intervals"
                    value={researchBatchForm.target_intervals.join(",")}
                    onChange={(value) =>
                      setResearchBatchForm((current) => ({
                        ...current,
                        target_intervals: parseStringList(value),
                      }))
                    }
                  />
                  <Field
                    label="Experiment Timeframes"
                    value={researchBatchForm.experiment_timeframes.join(",")}
                    onChange={(value) =>
                      setResearchBatchForm((current) => ({
                        ...current,
                        experiment_timeframes: parseStringList(value),
                      }))
                    }
                  />
                  <Field
                    label="Lookbacks"
                    value={researchBatchForm.lookback_candidates.join(",")}
                    onChange={(value) =>
                      setResearchBatchForm((current) => ({
                        ...current,
                        lookback_candidates: parseIntegerList(value),
                      }))
                    }
                  />
                  <Field
                    label="Momentum Lookbacks"
                    value={researchBatchForm.momentum_lookback_candidates?.join(",") ?? ""}
                    onChange={(value) =>
                      setResearchBatchForm((current) => ({
                        ...current,
                        momentum_lookback_candidates: parseIntegerList(value),
                      }))
                    }
                  />
                  <Field
                    label="Max Close Above SMA %"
                    value={researchBatchForm.max_close_above_sma_pct_candidates?.join(",") ?? ""}
                    onChange={(value) =>
                      setResearchBatchForm((current) => ({
                        ...current,
                        max_close_above_sma_pct_candidates: parseDecimalList(value),
                      }))
                    }
                  />
                  <Field
                    label="Holding Candles"
                    value={researchBatchForm.holding_candles_candidates?.join(",") ?? ""}
                    onChange={(value) =>
                      setResearchBatchForm((current) => ({
                        ...current,
                        holding_candles_candidates: parseIntegerList(value),
                      }))
                    }
                  />
                  <Field
                    label="Walk-forward Top N"
                    value={String(researchBatchForm.walk_forward_top_n ?? 3)}
                    onChange={(value) =>
                      setResearchBatchForm((current) => ({
                        ...current,
                        walk_forward_top_n: Number(value) || 3,
                      }))
                    }
                  />
                  <Field
                    label="Max Candidates"
                    value={String(researchBatchForm.max_candidates ?? 3)}
                    onChange={(value) =>
                      setResearchBatchForm((current) => ({
                        ...current,
                        max_candidates: Number(value) || 3,
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-3">
                  <label className="flex items-center gap-2 text-sm text-slate-300">
                    <input
                      type="checkbox"
                      checked={researchBatchForm.repair_degraded_data ?? true}
                      onChange={(event) =>
                        setResearchBatchForm((current) => ({
                          ...current,
                          repair_degraded_data: event.target.checked,
                        }))
                      }
                    />
                    Repair degraded data
                  </label>
                  <label className="flex items-center gap-2 text-sm text-slate-300">
                    <input
                      type="checkbox"
                      checked={researchBatchForm.create_candidates ?? true}
                      onChange={(event) =>
                        setResearchBatchForm((current) => ({
                          ...current,
                          create_candidates: event.target.checked,
                        }))
                      }
                    />
                    Create candidates
                  </label>
                  <ActionButton
                    label="Run Batch"
                    onClick={() => researchBatchMutation.mutate()}
                    busy={researchBatchMutation.isPending}
                    disabled={user.role === "VIEWER"}
                  />
                  <InlineStatus
                    error={getErrorMessage(researchBatchMutation.error)}
                    success={
                      lastResearchBatch
                        ? `Batch ${shortenId(lastResearchBatch.batch_id)} ${lastResearchBatch.status}`
                        : undefined
                    }
                  />
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-2">
                  <ResearchBatchesTable
                    batches={researchBatches}
                    selectedId={selectedResearchBatchId}
                    onSelect={setSelectedResearchBatchId}
                  />
                  <ResearchBatchDetail
                    batch={selectedResearchBatch}
                    triage={selectedResearchBatchTriageQuery.data?.triage ?? null}
                    onSelectCandidate={setSelectedResearchCandidateId}
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-7" title="Strategy Experiment Form">
                <div className="grid gap-3 md:grid-cols-3">
                  {(
                    [
                      ["strategy_id", "Strategy ID"],
                      ["symbol", "Symbol"],
                      ["timeframes", "Timeframes"],
                      ["start_time", "Start Time"],
                      ["end_time", "End Time"],
                      ["initial_capital", "Initial Capital"],
                      ["fee_bps", "Fee Bps"],
                      ["slippage_bps", "Slippage Bps"],
                      ["lookbacks", "Lookbacks"],
                      ["trend_lookbacks", "Trend Lookbacks"],
                      ["momentum_lookbacks", "Momentum Lookbacks"],
                      ["breakout_lookbacks", "Breakout Lookbacks"],
                      ["lower_band_pct", "Lower Band %"],
                      ["min_range_width_pct", "Min Range Width %"],
                      ["max_range_width_pct", "Max Range Width %"],
                      ["min_close_above_sma_pct", "Min Close Above SMA %"],
                      ["max_close_above_sma_pct", "Max Close Above SMA %"],
                      ["min_momentum_return_pct", "Min Momentum Return %"],
                      ["holding_candles", "Holding Candles"],
                      ["stop_loss_pct", "Stop Loss %"],
                      ["take_profit_pct", "Take Profit %"],
                      ["max_signal_age_ms", "Max Signal Age Ms"],
                      ["max_runs", "Max Runs"],
                    ] as const
                  ).map(([key, label]) => (
                    <Field
                      key={key}
                      label={label}
                      value={strategyExperimentForm[key]}
                      onChange={(value) =>
                        setStrategyExperimentForm((current) => ({ ...current, [key]: value }))
                      }
                    />
                  ))}
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Run Experiment"
                    onClick={() => runStrategyExperimentMutation.mutate()}
                    busy={runStrategyExperimentMutation.isPending}
                    disabled={user.role === "VIEWER"}
                  />
                  <InlineStatus
                    error={getErrorMessage(runStrategyExperimentMutation.error)}
                    success={
                      lastStrategyExperimentResult
                        ? `Group ${shortenId(lastStrategyExperimentResult.comparison.experiment_group_id)} ranked ${lastStrategyExperimentResult.comparison.global_ranking.ranked_runs.length} candidates`
                        : undefined
                    }
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-5" title="Comparison Summary">
                <KeyValue
                  items={[
                    ["Experiment Group", selectedExperimentComparison?.experiment_group_id ?? "N/A"],
                    ["Status", selectedExperimentComparison?.status ?? "N/A"],
                    ["Timeframes", selectedExperimentComparison?.requested_timeframes.join(", ") ?? "N/A"],
                    [
                      "Best Global Candidate",
                      selectedExperimentComparison?.global_ranking.ranked_runs[0]
                        ? shortenId(selectedExperimentComparison.global_ranking.ranked_runs[0].run.id)
                        : "N/A",
                    ],
                    [
                      "Skipped Timeframes",
                      String(
                        selectedExperimentComparison?.timeframe_comparisons.filter((item) => item.skipped_reason).length ?? 0,
                      ),
                    ],
                    ["Ranking Metric", selectedExperimentComparison?.global_ranking.ranking_metric ?? "N/A"],
                  ]}
                  loading={selectedExperimentComparisonQuery.isLoading}
                  error={getErrorMessage(selectedExperimentComparisonQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-4" title="Recent Experiments">
                <StrategyExperimentsTable
                  experiments={strategyExperiments}
                  onSelect={setSelectedExperimentId}
                  selectedId={selectedExperimentId}
                />
                <InlineStatus error={getErrorMessage(strategyExperimentsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-8" title="Global Ranking">
                <div className="overflow-auto rounded-2xl border border-border">
                  <table className="min-w-full text-sm">
                    <thead className="bg-surface/60 text-left text-slate-300">
                      <tr>
                        {["Timeframe", "Run", "PnL %", "Drawdown %", "Trades", "Win Rate", "Drag %", "Score", "Warnings"].map((label) => (
                          <th key={label} className="px-3 py-2 font-medium">{label}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {(selectedExperimentComparison?.global_ranking.ranked_runs ?? []).map((entry) => (
                        <tr key={entry.run.id} className="border-t border-border">
                          <td className="px-3 py-2">{entry.timeframe}</td>
                          <td className="px-3 py-2">{shortenId(entry.run.id)}</td>
                          <td className="px-3 py-2">{entry.run.pnl_pct}</td>
                          <td className="px-3 py-2">{entry.run.max_drawdown_pct}</td>
                          <td className="px-3 py-2">{entry.run.trade_count}</td>
                          <td className="px-3 py-2">{entry.run.win_rate}</td>
                          <td className="px-3 py-2">{entry.run.fee_slippage_drag_pct}</td>
                          <td className="px-3 py-2">{entry.run.score}</td>
                          <td className="px-3 py-2">{entry.warnings.join(", ") || "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <InlineStatus error={getErrorMessage(selectedExperimentComparisonQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-4" title="Per-timeframe Best">
                <div className="space-y-3">
                  {(selectedExperimentComparison?.timeframe_comparisons ?? []).map((item) => (
                    <div key={item.candidate.timeframe} className="rounded-2xl border border-border bg-surface/40 p-3">
                      <div className="flex items-center justify-between gap-3">
                        <div className="font-medium text-white">{item.candidate.timeframe}</div>
                        <div className="text-xs uppercase tracking-[0.18em] text-muted">{item.status}</div>
                      </div>
                      <div className="mt-2 text-xs text-slate-300">
                        candles={item.candidate.candle_count} required={item.candidate.required_candles}
                      </div>
                      <div className="mt-2 text-sm text-slate-200">
                        {item.skipped_reason
                          ? `Skipped: ${item.skipped_reason}`
                          : item.best_run
                            ? `Best ${shortenId(item.best_run.id)} pnl=${item.best_run.pnl_pct}% drawdown=${item.best_run.max_drawdown_pct}% trades=${item.best_run.trade_count}`
                            : "No ranked run"}
                      </div>
                    </div>
                  ))}
                </div>
              </Panel>
              <Panel className="xl:col-span-8" title="Selected Experiment Runs">
                <StrategyExperimentRunsTable runs={strategyExperimentRuns} />
                <InlineStatus error={getErrorMessage(selectedExperimentRunsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-4" title="Walk-forward Form">
                <div className="grid gap-3 md:grid-cols-2">
                  {(
                    [
                      ["strategy_id", "Strategy ID"],
                      ["symbol", "Symbol"],
                      ["timeframe", "Timeframe"],
                      ["experiment_run_id", "Experiment Run ID"],
                      ["config_json", "Config JSON"],
                      ["start_time", "Start Time"],
                      ["end_time", "End Time"],
                      ["train_hours", "Train Hours"],
                      ["test_hours", "Test Hours"],
                      ["step_hours", "Step Hours"],
                      ["initial_capital", "Initial Capital"],
                      ["fee_bps", "Fee Bps"],
                      ["slippage_bps", "Slippage Bps"],
                      ["lookback_candles", "Lookback Candles"],
                      ["trend_lookback", "Trend Lookback"],
                      ["momentum_lookback", "Momentum Lookback"],
                      ["breakout_lookback", "Breakout Lookback"],
                      ["holding_candles", "Holding Candles"],
                      ["stop_loss_pct", "Stop Loss %"],
                      ["take_profit_pct", "Take Profit %"],
                      ["max_signal_age_ms", "Max Signal Age Ms"],
                      ["min_required_test_windows", "Min Test Windows"],
                    ] as const
                  ).map(([key, label]) => (
                    <Field
                      key={key}
                      label={label}
                      value={strategyWalkForwardForm[key]}
                      onChange={(value) =>
                        setStrategyWalkForwardForm((current) => ({ ...current, [key]: value }))
                      }
                    />
                  ))}
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Run Walk-forward"
                    onClick={() => runStrategyWalkForwardMutation.mutate()}
                    busy={runStrategyWalkForwardMutation.isPending}
                    disabled={user.role === "VIEWER"}
                  />
                  <InlineStatus
                    error={getErrorMessage(runStrategyWalkForwardMutation.error)}
                    success={
                      lastStrategyWalkForwardResult
                        ? `Run ${shortenId(lastStrategyWalkForwardResult.walk_forward.walk_forward_id)} scored ${lastStrategyWalkForwardResult.walk_forward.robustness_score}`
                        : undefined
                    }
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-4" title="Walk-forward Summary">
                <KeyValue
                  items={[
                    ["Run", selectedWalkForward?.walk_forward_id ?? "N/A"],
                    ["Status", selectedWalkForward?.status ?? "N/A"],
                    ["Robustness", selectedWalkForward?.robustness_status ?? "N/A"],
                    ["Robustness Score", selectedWalkForward?.robustness_score ?? "N/A"],
                    ["Consistency Score", selectedWalkForward?.consistency_score ?? "N/A"],
                    [
                      "Profitable / Losing",
                      selectedWalkForward
                        ? `${selectedWalkForward.profitable_test_windows} / ${selectedWalkForward.losing_test_windows}`
                        : "N/A",
                    ],
                    ["Avg PnL %", selectedWalkForward?.avg_test_pnl_pct ?? "N/A"],
                    ["Worst / Best PnL %", selectedWalkForward
                      ? `${selectedWalkForward.worst_test_pnl_pct} / ${selectedWalkForward.best_test_pnl_pct}`
                      : "N/A"],
                    ["Skipped Windows", String(selectedWalkForward?.skipped_windows ?? 0)],
                    [
                      "Recommendation",
                      selectedWalkForward?.recommendation
                        ? `${selectedWalkForward.recommendation.action}: ${selectedWalkForward.recommendation.reason}`
                        : "N/A",
                    ],
                  ]}
                  loading={selectedWalkForwardQuery.isLoading}
                  error={getErrorMessage(selectedWalkForwardQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-4" title="Recent Walk-forward Runs">
                <StrategyWalkForwardRunsTable
                  runs={strategyWalkForwards}
                  onSelect={setSelectedWalkForwardId}
                  selectedId={selectedWalkForwardId}
                />
                <InlineStatus error={getErrorMessage(strategyWalkForwardsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-12" title="Walk-forward Windows">
                <StrategyWalkForwardWindowsTable windows={selectedWalkForwardWindows} />
                <InlineStatus error={getErrorMessage(selectedWalkForwardWindowsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-4" title="Research Candidates">
                <div className="mb-3 grid gap-3">
                  <Field
                    label="Strategy"
                    value={researchCandidateStrategyFilter}
                    onChange={setResearchCandidateStrategyFilter}
                    placeholder="all strategies"
                  />
                  <Field
                    label="Symbol"
                    value={researchCandidateSymbolFilter}
                    onChange={setResearchCandidateSymbolFilter}
                    placeholder="all symbols"
                  />
                  <Field
                    label="Timeframe"
                    value={researchCandidateTimeframeFilter}
                    onChange={setResearchCandidateTimeframeFilter}
                    placeholder="15m"
                  />
                  <Field
                    label="Status"
                    value={researchCandidateStatusFilter}
                    onChange={(value) =>
                      setResearchCandidateStatusFilter(
                        value as StrategyResearchCandidateStatus | "",
                      )
                    }
                    placeholder="all statuses"
                  />
                </div>
                <div className="overflow-auto rounded-2xl border border-border">
                  <table className="min-w-full text-sm">
                    <thead className="bg-surface/60 text-left text-slate-300">
                      <tr>
                        {["Candidate", "Score", "Status", "Source"].map((label) => (
                          <th key={label} className="px-3 py-2 font-medium">{label}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {researchCandidates.map((candidate) => (
                        <tr
                          key={candidate.id}
                          className={cn(
                            "cursor-pointer border-t border-border",
                            selectedResearchCandidateId === candidate.id && "bg-white/5",
                          )}
                          onClick={() => setSelectedResearchCandidateId(candidate.id)}
                        >
                          <td className="px-3 py-2">
                            <div>{candidate.strategy_id}</div>
                            <div className="text-xs text-muted">{shortenId(candidate.id)}</div>
                          </td>
                          <td className="px-3 py-2">{candidate.score ?? "-"}</td>
                          <td className="px-3 py-2">{candidate.status}</td>
                          <td className="px-3 py-2">
                            {candidate.experiment_run_id ? "EXPERIMENT_RUN" : "MANUAL"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <InlineStatus error={getErrorMessage(researchCandidatesQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-4" title="Research Watchlist">
                <div className="overflow-auto rounded-2xl border border-border">
                  <table className="min-w-full text-sm">
                    <thead className="bg-surface/60 text-left text-slate-300">
                      <tr>
                        {[
                          "Candidate",
                          "Candidate Status",
                          "Qualification",
                          "Walk-forward",
                          "Score",
                          "Trend",
                          "Last Evaluated",
                        ].map((label) => (
                          <th key={label} className="px-3 py-2 font-medium">{label}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {researchCandidateWatchlist.map((entry) => (
                        <tr
                          key={entry.candidate_id}
                          className="cursor-pointer border-t border-border"
                          onClick={() => setSelectedResearchCandidateId(entry.candidate_id)}
                        >
                          <td className="px-3 py-2 text-slate-100">
                            {entry.strategy_id} {entry.symbol} {entry.timeframe}
                          </td>
                          <td className="px-3 py-2">{entry.candidate_status}</td>
                          <td className="px-3 py-2">
                            {entry.latest_evaluation?.status ?? "UNKNOWN"}
                          </td>
                          <td className="px-3 py-2">
                            {entry.walk_forward_evidence?.robustness_status ?? "MISSING"}
                          </td>
                          <td className="px-3 py-2">
                            {entry.latest_evaluation?.score ?? "-"}
                          </td>
                          <td className="px-3 py-2">
                            {qualificationTrendLabel(entry.trend)}
                          </td>
                          <td className="px-3 py-2">
                            {formatDateTime(entry.latest_evaluation?.evaluated_at ?? null)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <InlineStatus error={getErrorMessage(researchCandidateWatchlistQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-8" title="Candidate Detail">
                <KeyValue
                  items={[
                    ["Candidate", selectedResearchCandidate?.id ?? "N/A"],
                    ["Strategy", selectedResearchCandidate?.strategy_id ?? "N/A"],
                    ["Symbol / Timeframe", selectedResearchCandidate
                      ? `${selectedResearchCandidate.symbol} / ${selectedResearchCandidate.timeframe}`
                      : "N/A"],
                    ["Score", selectedResearchCandidate?.score ?? "N/A"],
                    ["Status", selectedResearchCandidate?.status ?? "N/A"],
                    ["Experiment Run", selectedResearchCandidate?.experiment_run_id ?? "N/A"],
                    ["PnL %", selectedResearchCandidate?.pnl_pct ?? "N/A"],
                    ["Max Drawdown %", selectedResearchCandidate?.max_drawdown_pct ?? "N/A"],
                    ["Win Rate", selectedResearchCandidate?.win_rate ?? "N/A"],
                    ["Trade Count", String(selectedResearchCandidate?.trade_count ?? 0)],
                    ["Fee Drag", selectedResearchCandidate?.fee_drag ?? "N/A"],
                    ["Rejection", selectedResearchCandidate?.rejection_reason ?? "None"],
                    ["Notes", selectedResearchCandidate?.notes ?? "None"],
                  ]}
                  loading={selectedResearchCandidateQuery.isLoading}
                  error={getErrorMessage(selectedResearchCandidateQuery.error)}
                />
                <div
                  className={cn(
                    "mt-4 rounded-xl border p-4 text-sm",
                    researchCandidateWalkForwardEvidence?.robustness_status === "OVERFIT_RISK"
                      ? "border-rose-400/40 bg-rose-500/10 text-rose-100"
                      : researchCandidateWalkForwardEvidence?.robustness_status === "ROBUST"
                        ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-100"
                        : "border-amber-400/40 bg-amber-500/10 text-amber-100",
                  )}
                >
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="font-semibold">Walk-Forward Evidence</div>
                    <div className="rounded-full border border-current/30 px-3 py-1 text-[11px] uppercase tracking-[0.2em]">
                      {researchCandidateWalkForwardEvidence?.robustness_status ?? "MISSING"}
                    </div>
                  </div>
                  <div className="mt-3 grid gap-2 sm:grid-cols-2">
                    <div>Run: {researchCandidateWalkForwardEvidence?.walk_forward_run_id ?? "N/A"}</div>
                    <div>Consistency: {researchCandidateWalkForwardEvidence?.consistency_score ?? "N/A"}</div>
                    <div>Avg PnL %: {researchCandidateWalkForwardEvidence?.avg_pnl_pct ?? "N/A"}</div>
                    <div>
                      Profitable / Losing:{" "}
                      {researchCandidateWalkForwardEvidence
                        ? `${researchCandidateWalkForwardEvidence.profitable_windows} / ${researchCandidateWalkForwardEvidence.losing_windows}`
                        : "N/A"}
                    </div>
                  </div>
                  <div className="mt-2 text-xs">
                    {researchCandidateWalkForwardEvidence?.robustness_status === "OVERFIT_RISK"
                      ? "OVERFIT_RISK: do not accept. This evidence is read-only and does not submit orders."
                      : researchCandidateWalkForwardEvidence?.recommendation_reason ??
                        "No linked walk-forward evidence. Testnet review remains blocked until evidence is linked."}
                  </div>
                  <div className="mt-3 flex items-center gap-3">
                    <ActionButton
                      label="Link Selected Walk-forward"
                      onClick={() => linkResearchCandidateWalkForwardMutation.mutate()}
                      busy={linkResearchCandidateWalkForwardMutation.isPending}
                      disabled={
                        (user.role !== "OWNER" && user.role !== "OPERATOR") ||
                        !selectedResearchCandidate ||
                        !selectedWalkForward
                      }
                    />
                    <InlineStatus
                      error={getErrorMessage(linkResearchCandidateWalkForwardMutation.error)}
                      success={
                        linkResearchCandidateWalkForwardMutation.data?.latest
                          ? "linked"
                          : undefined
                      }
                    />
                  </div>
                  <InlineStatus error={getErrorMessage(selectedResearchCandidateWalkForwardQuery.error)} />
                </div>
                <div className="mt-4 grid gap-4 xl:grid-cols-3">
                  <div className="rounded-2xl border border-border bg-surface/40 p-4">
                    <div className="text-xs uppercase tracking-[0.2em] text-muted">Config</div>
                    <pre className="mt-2 overflow-auto text-xs text-slate-200">
                      {selectedResearchCandidate
                        ? JSON.stringify(selectedResearchCandidate.config, null, 2)
                        : "{}"}
                    </pre>
                  </div>
                  <div className="rounded-2xl border border-border bg-surface/40 p-4">
                    <div className="text-xs uppercase tracking-[0.2em] text-muted">Decision</div>
                    <Field
                      label="Reason / Notes"
                      value={researchCandidateDecisionReason}
                      onChange={setResearchCandidateDecisionReason}
                      placeholder="decision rationale"
                    />
                    <div className="mt-3 flex flex-wrap items-center gap-3">
                      <ActionButton
                        label="Accept"
                        onClick={() => decideResearchCandidateMutation.mutate("ACCEPT_FOR_SHADOW")}
                        busy={decideResearchCandidateMutation.isPending}
                        disabled={
                          (user.role !== "OWNER" && user.role !== "OPERATOR") ||
                          !selectedResearchCandidate ||
                          selectedResearchCandidate.status === "ARCHIVED" ||
                          acceptForShadowBlockedByStale
                        }
                      />
                      <ActionButton
                        label="Reject"
                        onClick={() => decideResearchCandidateMutation.mutate("REJECT")}
                        busy={decideResearchCandidateMutation.isPending}
                        disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                      />
                      <ActionButton
                        label="Archive"
                        onClick={() => decideResearchCandidateMutation.mutate("ARCHIVE")}
                        busy={decideResearchCandidateMutation.isPending}
                        disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                      />
                      <ActionButton
                        label="Reopen"
                        onClick={() => decideResearchCandidateMutation.mutate("REOPEN")}
                        busy={decideResearchCandidateMutation.isPending}
                        disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                      />
                      <InlineStatus
                        error={getErrorMessage(decideResearchCandidateMutation.error)}
                        success={
                          decideResearchCandidateMutation.data
                            ? decideResearchCandidateMutation.data.candidate.status
                          : undefined
                        }
                      />
                    </div>
                    {decideResearchCandidateErrorPayload?.rejection ? (
                      <div className="mt-3 rounded-xl border border-amber-400/40 bg-amber-500/10 p-3 text-xs text-amber-50/90">
                        <div className="font-semibold text-amber-100">
                          {decideResearchCandidateErrorPayload.rejection.reason_code}
                        </div>
                        <div className="mt-1">
                          {decideResearchCandidateErrorPayload.rejection.recommendation}
                        </div>
                        <div className="mt-1">
                          Last observed:{" "}
                          {formatDateTime(
                            decideResearchCandidateErrorPayload.rejection.last_observed_at,
                          )}
                        </div>
                        <div>
                          Observation age:{" "}
                          {decideResearchCandidateErrorPayload.rejection.observation_age_seconds ??
                            "-"}
                          s
                        </div>
                      </div>
                    ) : null}
                    <div className="mt-4 border-t border-border/70 pt-4">
                      <div className="text-xs uppercase tracking-[0.2em] text-muted">Review Actions</div>
                      <Field
                        label="Reason"
                        value={researchCandidateReviewReason}
                        onChange={setResearchCandidateReviewReason}
                        placeholder="required for reject/archive"
                      />
                      <div className="mt-3">
                        <Field
                          label="Notes"
                          value={researchCandidateReviewNotes}
                          onChange={setResearchCandidateReviewNotes}
                          placeholder="operator context"
                        />
                      </div>
                      <div className="mt-3 rounded-xl border border-sky-400/30 bg-sky-500/10 p-3 text-xs text-sky-100">
                        Ready for testnet review records human intent only. It does not submit
                        orders, create testnet orders, or auto-promote anything.
                      </div>
                      <div className="mt-3 flex flex-wrap gap-3">
                        <ActionButton
                          label="Mark Reviewed"
                          onClick={() => reviewResearchCandidateMutation.mutate("MARK_REVIEWED")}
                          busy={reviewResearchCandidateMutation.isPending}
                          disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                        />
                        <ActionButton
                          label="Needs More Observation"
                          onClick={() =>
                            reviewResearchCandidateMutation.mutate("MARK_NEEDS_MORE_OBSERVATION")
                          }
                          busy={reviewResearchCandidateMutation.isPending}
                          disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                        />
                        <ActionButton
                          label="Ready For Testnet Review"
                          onClick={() =>
                            reviewResearchCandidateMutation.mutate("MARK_READY_FOR_TESTNET_REVIEW")
                          }
                          busy={reviewResearchCandidateMutation.isPending}
                          disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                        />
                        <ActionButton
                          label="Investigated"
                          onClick={() =>
                            reviewResearchCandidateMutation.mutate("MARK_INVESTIGATED")
                          }
                          busy={reviewResearchCandidateMutation.isPending}
                          disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                        />
                        <ActionButton
                          label="Reject Watchlist"
                          onClick={() =>
                            reviewResearchCandidateMutation.mutate("REJECT_FROM_WATCHLIST")
                          }
                          busy={reviewResearchCandidateMutation.isPending}
                          disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                        />
                        <ActionButton
                          label="Archive Watchlist"
                          onClick={() =>
                            reviewResearchCandidateMutation.mutate("ARCHIVE_FROM_WATCHLIST")
                          }
                          busy={reviewResearchCandidateMutation.isPending}
                          disabled={(user.role !== "OWNER" && user.role !== "OPERATOR") || !selectedResearchCandidate}
                        />
                      </div>
                      <div className="mt-3">
                        <InlineStatus
                          error={getErrorMessage(reviewResearchCandidateMutation.error)}
                          success={reviewResearchCandidateMutation.data?.result.review.status}
                        />
                      </div>
                    </div>
                  </div>
                  <div className="rounded-2xl border border-border bg-surface/40 p-4">
                    <div className="text-xs uppercase tracking-[0.2em] text-muted">
                      Shadow Observation
                    </div>
                    <div className="mt-2 space-y-2 text-sm text-slate-200">
                      <div>
                        Freshness:{" "}
                        {researchCandidateObservationFreshness === "FRESH"
                          ? "Fresh"
                          : researchCandidateObservationFreshness === "STALE"
                            ? "Stale"
                            : researchCandidateObservationFreshness === "NOT_OBSERVED"
                              ? "Not observed"
                              : "Unknown"}
                      </div>
                      <div>
                        Last observed:{" "}
                        {formatDateTime(latestResearchCandidateObservation?.last_observed_at)}
                      </div>
                      <div>
                        Observation age:{" "}
                        {researchCandidateObservationAgeSeconds !== null
                          ? `${researchCandidateObservationAgeSeconds}s`
                          : "-"}
                      </div>
                      <div>
                        Status: {latestResearchCandidateObservation?.status ?? "NONE"}
                      </div>
                      <div>
                        Decision: {latestResearchCandidateObservation?.decision ?? "NONE"}
                      </div>
                      <div>
                        Runs: {latestResearchCandidateObservation?.summary.shadow_runs ?? 0} /
                        would-submit{" "}
                        {latestResearchCandidateObservation?.summary.would_submit_count ?? 0}
                      </div>
                      <div>
                        Readiness:{" "}
                        {latestResearchCandidateObservation?.summary.latest_readiness_status ??
                          "UNKNOWN"}
                        {" / "}
                        {latestResearchCandidateObservation?.summary.latest_readiness_score ?? "-"}
                      </div>
                      <div>
                        Runner:{" "}
                        {latestResearchCandidateRunnerAlignment
                          ? `${latestResearchCandidateRunnerAlignment.runner_status} · ${latestResearchCandidateRunnerAlignment.runner_timeframe} · ${latestResearchCandidateRunnerAlignment.strategy_config_matches_runner ? "aligned" : "mismatch"}`
                          : "UNKNOWN"}
                      </div>
                      <div>Eligibility: {latestEligibilityLabel}</div>
                    </div>
                    <div className="mt-4 rounded-xl border border-border/70 bg-black/10 p-3 text-xs text-slate-200">
                      <div className="font-semibold text-slate-100">Stability Summary</div>
                      <div className="mt-2 grid gap-1 sm:grid-cols-2">
                        <div>
                          Total observations:{" "}
                          {researchCandidateObservationSummary?.total_observations ?? 0}
                        </div>
                        <div>
                          Stale: {researchCandidateObservationSummary?.stale_count ?? 0}
                        </div>
                        <div>
                          Alignment mismatches:{" "}
                          {researchCandidateObservationSummary?.alignment_mismatch_count ?? 0}
                        </div>
                        <div>
                          Config drift:{" "}
                          {researchCandidateObservationSummary?.runner_config_drift_count ?? 0}
                        </div>
                      </div>
                      {(researchCandidateObservationSummary?.current_accept_for_shadow_blockers
                        ?.length ?? 0) > 0 ? (
                        <div className="mt-2 text-amber-100/90">
                          Blockers:{" "}
                          {researchCandidateObservationSummary?.current_accept_for_shadow_blockers.join(
                            ", ",
                          )}
                        </div>
                      ) : null}
                    </div>
                    {acceptForShadowBlockedByStale ? (
                      <div className="mt-4 rounded-xl border border-amber-400/40 bg-amber-500/10 p-4">
                        <div className="text-sm font-semibold text-amber-100">
                          Observe again before accept
                        </div>
                        <div className="mt-1 text-xs text-amber-50/80">
                          Latest persisted observation is stale. Run candidate observation again
                          before accepting for shadow.
                        </div>
                      </div>
                    ) : null}
                    {latestResearchCandidateRunnerAlignment &&
                    !latestResearchCandidateRunnerAlignment.strategy_config_matches_runner ? (
                      <div className="mt-4 rounded-xl border border-amber-400/40 bg-amber-500/10 p-4">
                        <div className="text-sm font-semibold text-amber-100">
                          Shadow runner mismatch
                        </div>
                        <div className="mt-1 text-xs text-amber-50/80">
                          Shadow runner is not configured for candidate timeframe/symbol/strategy.
                        </div>
                        <div className="mt-3 space-y-1 text-xs text-amber-50/80">
                          {latestResearchCandidateRunnerAlignment.mismatch_reasons.map((reason) => (
                            <div key={reason}>{reason}</div>
                          ))}
                          {(latestResearchCandidateObservation?.summary.recommendations ?? []).map(
                            (recommendation) => (
                              <div key={recommendation}>{recommendation}</div>
                            ),
                          )}
                        </div>
                      </div>
                    ) : null}
                    <div className="mt-3 flex items-center gap-3">
                      <ActionButton
                        label="Run Observation"
                        onClick={() => observeResearchCandidateMutation.mutate()}
                        busy={observeResearchCandidateMutation.isPending}
                        disabled={
                          (user.role !== "OWNER" && user.role !== "OPERATOR") ||
                          !selectedResearchCandidate
                        }
                      />
                      <InlineStatus
                        error={getErrorMessage(observeResearchCandidateMutation.error)}
                        success={
                          observeResearchCandidateMutation.data
                            ? observeResearchCandidateMutation.data.observation.decision
                            : undefined
                        }
                      />
                    </div>
                    <div className="mt-4 rounded-xl border border-border/70 bg-black/10 p-3 text-xs text-slate-200">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="font-semibold text-slate-100">
                          Shadow Promotion Preview
                        </div>
                        <ActionButton
                          label="Preview Shadow Promotion"
                          onClick={() => previewResearchCandidateShadowPromotionMutation.mutate()}
                          busy={previewResearchCandidateShadowPromotionMutation.isPending}
                          disabled={
                            (user.role !== "OWNER" && user.role !== "OPERATOR") ||
                            !selectedResearchCandidate
                          }
                        />
                      </div>
                      <label className="mt-3 flex items-center gap-2 rounded-xl border border-border bg-surface/50 px-3 py-2 text-sm">
                        <input
                          type="checkbox"
                          checked={researchCandidateAllowMissingRunnerAlignment}
                          onChange={(event) =>
                            setResearchCandidateAllowMissingRunnerAlignment(event.target.checked)
                          }
                        />
                        Allow adding missing strategy/symbol to the existing runner config
                      </label>
                      <InlineStatus
                        error={getErrorMessage(previewResearchCandidateShadowPromotionMutation.error)}
                        success={shadowPromotionPreview?.status}
                      />
                      {shadowPromotionPreview ? (
                        <>
                          <div className="mt-3 grid gap-3 sm:grid-cols-2">
                            <div>
                              <div className="text-[11px] uppercase tracking-[0.2em] text-muted">
                                Current Runner Config
                              </div>
                              <pre className="mt-2 overflow-auto rounded-xl border border-border/70 bg-surface/40 p-3 text-[11px] text-slate-200">
                                {JSON.stringify(
                                  shadowPromotionPreview.current_runner_config,
                                  null,
                                  2,
                                )}
                              </pre>
                            </div>
                            <div>
                              <div className="text-[11px] uppercase tracking-[0.2em] text-muted">
                                Proposed Runner Config
                              </div>
                              <pre className="mt-2 overflow-auto rounded-xl border border-border/70 bg-surface/40 p-3 text-[11px] text-slate-200">
                                {JSON.stringify(
                                  shadowPromotionPreview.proposed_runner_config,
                                  null,
                                  2,
                                )}
                              </pre>
                            </div>
                          </div>
                          <div className="mt-3 text-[11px] text-slate-300">
                            Status: {shadowPromotionPreview.status}
                          </div>
                          <div className="mt-2 text-[11px] text-slate-300">
                            Changes:{" "}
                            {shadowPromotionPreview.changes.length > 0
                              ? shadowPromotionPreview.changes.join(" | ")
                              : "none"}
                          </div>
                          <div className="mt-2 text-[11px] text-slate-300">
                            Reasons:{" "}
                            {shadowPromotionPreview.reasons.length > 0
                              ? shadowPromotionPreview.reasons.join(" | ")
                              : "none"}
                          </div>
                        </>
                      ) : null}
                      <div className="mt-3">
                        <Field
                          label="Typed Confirmation"
                          value={researchCandidateShadowPromotionConfirmation}
                          onChange={setResearchCandidateShadowPromotionConfirmation}
                          placeholder={expectedShadowPromotionConfirmation || "Select a candidate"}
                        />
                      </div>
                      <div className="mt-3 flex items-center gap-3">
                        <ActionButton
                          label="Apply Shadow Promotion"
                          onClick={() => applyResearchCandidateShadowPromotionMutation.mutate()}
                          busy={applyResearchCandidateShadowPromotionMutation.isPending}
                          disabled={
                            !canApplyShadowPromotion ||
                            researchCandidateShadowPromotionConfirmation !==
                              expectedShadowPromotionConfirmation
                          }
                        />
                        <InlineStatus
                          error={getErrorMessage(applyResearchCandidateShadowPromotionMutation.error)}
                          success={
                            researchCandidateShadowPromotionResult
                              ? researchCandidateShadowPromotionResult.applied
                                ? "Shadow runner config updated"
                                : researchCandidateShadowPromotionResult.status
                              : undefined
                          }
                        />
                      </div>
                    </div>
                    <div className="mt-4 rounded-xl border border-border/70 bg-black/10 p-3 text-xs text-slate-200">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="font-semibold text-slate-100">Qualification</div>
                        <ActionButton
                          label="Evaluate"
                          onClick={() => evaluateResearchCandidateQualificationMutation.mutate()}
                          busy={evaluateResearchCandidateQualificationMutation.isPending}
                          disabled={
                            (user.role !== "OWNER" && user.role !== "OPERATOR") ||
                            !selectedResearchCandidate
                          }
                        />
                      </div>
                      <div className="mt-2 grid gap-2 sm:grid-cols-2">
                        <div>Status: {researchCandidateQualification?.status ?? "UNKNOWN"}</div>
                        <div>Score: {researchCandidateQualification?.score ?? "-"}</div>
                        <div>
                          Readiness:{" "}
                          {researchCandidateQualification?.latest_readiness_status ?? "UNKNOWN"}
                        </div>
                        <div>
                          Readiness penalty: -
                          {researchCandidateQualification?.readiness_penalty_points ?? 0}
                        </div>
                        <div>
                          Walk-forward:{" "}
                          {researchCandidateQualification?.walk_forward_status ?? "MISSING"}
                        </div>
                        <div>
                          WF score: {researchCandidateQualification?.walk_forward_score ?? "-"}
                        </div>
                        <div>
                          Linked runs:{" "}
                          {researchCandidateQualification?.shadow_performance?.total_shadow_runs ??
                            0}
                        </div>
                        <div>
                          Would-submit:{" "}
                          {researchCandidateQualification?.shadow_performance
                            ?.would_submit_count ?? 0}
                        </div>
                        <div>Trend: {qualificationTrendLabel(researchCandidateQualificationHistory?.latest_trend ?? "INSUFFICIENT_HISTORY")}</div>
                        <div>
                          Last evaluated: {formatDateTime(latestQualificationEvaluation?.evaluated_at ?? null)}
                        </div>
                      </div>
                      {latestQualificationChange ? (
                        <div className="mt-3 rounded-xl border border-border/70 bg-surface/40 p-3">
                          Latest change:{" "}
                          {(latestQualificationChange.previous_status ?? "UNKNOWN").toString()} to{" "}
                          {latestQualificationChange.current_status}
                          {" · "}score {latestQualificationChange.previous_score ?? "-"} to{" "}
                          {latestQualificationChange.current_score}
                          {" · "}delta {latestQualificationChange.score_delta}
                        </div>
                      ) : null}
                      {researchCandidateQualification?.latest_readiness_status === "DEGRADED" ? (
                        <div className="mt-3 rounded-xl border border-amber-400/40 bg-amber-500/10 p-3 text-amber-100">
                          Resolve degraded readiness conditions before considering testnet
                          promotion.
                        </div>
                      ) : null}
                      {researchCandidateQualification?.latest_readiness_status === "NOT_READY" ? (
                        <div className="mt-3 rounded-xl border border-rose-400/40 bg-rose-500/10 p-3 text-rose-100">
                          Do not consider testnet promotion until readiness blockers are cleared.
                        </div>
                      ) : null}
                      {researchCandidateQualification?.threshold_override_below_default ? (
                        <div className="mt-3 rounded-xl border border-amber-400/40 bg-amber-500/10 p-3 text-amber-100">
                          Qualification threshold override is below default; treat result as
                          exploratory.
                        </div>
                      ) : null}
                      {researchCandidateQualification?.walk_forward_status === "OVERFIT_RISK" ? (
                        <div className="mt-3 rounded-xl border border-rose-400/40 bg-rose-500/10 p-3 text-rose-100">
                          OVERFIT_RISK: do not accept. Do not advance this candidate toward
                          testnet review.
                        </div>
                      ) : null}
                      {qualificationNeedsMoreData ? (
                        <div className="mt-3 rounded-xl border border-sky-400/40 bg-sky-500/10 p-3 text-sky-100">
                          Needs more data: linked shadow runs are below the configured threshold.
                        </div>
                      ) : null}
                      <div className="mt-3 rounded-xl border border-border/70 bg-surface/40 p-3">
                        Thresholds: runs ≥{" "}
                        {researchCandidateQualification?.thresholds.min_shadow_runs ?? 30}
                        {" · "}
                        would-submit ≥{" "}
                        {researchCandidateQualification?.thresholds.min_would_submit_count ?? 3}
                        {" · "}
                        risk rejected ≤{" "}
                        {researchCandidateQualification?.thresholds
                          .max_risk_rejection_rate_pct ?? "40"}
                        %{" · "}
                        skipped/error ≤{" "}
                        {researchCandidateQualification?.thresholds
                          .max_error_or_skipped_rate_pct ?? "20"}
                        %
                      </div>
                      {(researchCandidateQualification?.blockers.length ?? 0) > 0 ? (
                        <div className="mt-3 rounded-xl border border-rose-400/40 bg-rose-500/10 p-3 text-rose-100">
                          <div className="font-semibold">Blockers</div>
                          <div className="mt-2 space-y-1">
                            {researchCandidateQualification?.blockers.map((item) => (
                              <div key={item}>{item}</div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                      {(researchCandidateQualification?.warnings.length ?? 0) > 0 ? (
                        <div className="mt-3 rounded-xl border border-amber-400/40 bg-amber-500/10 p-3 text-amber-100">
                          <div className="font-semibold">Warnings</div>
                          <div className="mt-2 space-y-1">
                            {researchCandidateQualification?.warnings.map((item) => (
                              <div key={item}>{item}</div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                      {(researchCandidateQualification?.recommendations.length ?? 0) > 0 ? (
                        <div className="mt-3 rounded-xl border border-border/70 bg-black/10 p-3">
                          <div className="font-semibold text-slate-100">Recommendations</div>
                          <div className="mt-2 space-y-1 text-slate-200">
                            {researchCandidateQualification?.recommendations.map((item) => (
                              <div key={item}>{qualificationRecommendationLabel(item)}</div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                      {(researchCandidateQualification?.score_explanation.length ?? 0) > 0 ? (
                        <div className="mt-3 rounded-xl border border-border/70 bg-black/10 p-3">
                          <div className="font-semibold text-slate-100">Score Explanation</div>
                          <div className="mt-2 space-y-1 text-slate-200">
                            {researchCandidateQualification?.score_explanation.map((item) => (
                              <div key={item}>{item}</div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                      {researchCandidateTestnetReviewDossier ? (
                        <div className="mt-3 rounded-xl border border-border/70 bg-black/10 p-3">
                          <div className="flex flex-wrap items-center justify-between gap-3">
                            <div className="font-semibold text-slate-100">
                              Testnet Review Dossier
                            </div>
                            <div className="rounded-full border border-border/70 px-3 py-1 text-[11px] uppercase tracking-[0.2em] text-slate-200">
                              {researchCandidateTestnetReviewDossier.status}
                            </div>
                          </div>
                          <div className="mt-2 text-xs text-slate-300">
                            This does not submit orders. Execution tables are not touched.
                          </div>
                          <div className="mt-3 grid gap-2 sm:grid-cols-2">
                            <div>
                              Latest review:{" "}
                              {researchCandidateTestnetReviewDossier.evidence.latest_review_action
                                ?.action ?? "NONE"}
                            </div>
                            <div>
                              Recommendation:{" "}
                              {researchCandidateTestnetReviewDossier.recommendations[0] ??
                                "NONE"}
                            </div>
                            <div>
                              Walk-forward:{" "}
                              {researchCandidateTestnetReviewDossier.evidence.walk_forward_evidence
                                ?.robustness_status ?? "MISSING"}
                            </div>
                            <div>
                              WF run:{" "}
                              {researchCandidateTestnetReviewDossier.evidence.walk_forward_evidence
                                ?.walk_forward_run_id ?? "N/A"}
                            </div>
                          </div>
                          <div className="mt-3 rounded-xl border border-border/70 bg-surface/40 p-3">
                            <div className="font-semibold text-slate-100">Checklist</div>
                            <div className="mt-2 space-y-1 text-slate-200">
                              {researchCandidateTestnetReviewDossier.checklist.map((item) => (
                                <div key={item.code}>
                                  {item.passed ? "PASS" : "PENDING"} · {item.name}
                                </div>
                              ))}
                            </div>
                          </div>
                          {(researchCandidateTestnetReviewDossier.blockers.length ?? 0) > 0 ? (
                            <div className="mt-3 rounded-xl border border-rose-400/40 bg-rose-500/10 p-3 text-rose-100">
                              <div className="font-semibold">Blockers</div>
                              <div className="mt-2 space-y-1">
                                {researchCandidateTestnetReviewDossier.blockers.map((item) => (
                                  <div key={item}>{item}</div>
                                ))}
                              </div>
                            </div>
                          ) : null}
                          {(researchCandidateTestnetReviewDossier.warnings.length ?? 0) > 0 ? (
                            <div className="mt-3 rounded-xl border border-amber-400/40 bg-amber-500/10 p-3 text-amber-100">
                              <div className="font-semibold">Warnings</div>
                              <div className="mt-2 space-y-1">
                                {researchCandidateTestnetReviewDossier.warnings.map((item) => (
                                  <div key={item}>{item}</div>
                                ))}
                              </div>
                            </div>
                          ) : null}
                        </div>
                      ) : null}
                      {(researchCandidateQualificationHistory?.evaluations.length ?? 0) > 0 ? (
                        <div className="mt-3 rounded-xl border border-border/70 bg-black/10 p-3">
                          <div className="font-semibold text-slate-100">Qualification History</div>
                          <div className="mt-2 overflow-auto">
                            <table className="min-w-full text-[11px]">
                              <thead className="text-left text-slate-400">
                                <tr>
                                  {["Evaluated", "Status", "Score", "Readiness", "Runs", "Would Submit"].map((label) => (
                                    <th key={label} className="px-2 py-1 font-medium">{label}</th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {researchCandidateQualificationHistory?.evaluations.map((evaluation) => (
                                  <tr key={evaluation.id} className="border-t border-border/60">
                                    <td className="px-2 py-1">{formatDateTime(evaluation.evaluated_at)}</td>
                                    <td className="px-2 py-1">{evaluation.status}</td>
                                    <td className="px-2 py-1">{evaluation.score}</td>
                                    <td className="px-2 py-1">{evaluation.latest_readiness_status ?? "UNKNOWN"}</td>
                                    <td className="px-2 py-1">{evaluation.total_shadow_runs}</td>
                                    <td className="px-2 py-1">{evaluation.would_submit_count}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      ) : null}
                      {(researchCandidateReviews.length ?? 0) > 0 ? (
                        <div className="mt-3 rounded-xl border border-border/70 bg-black/10 p-3">
                          <div className="font-semibold text-slate-100">Review History</div>
                          <div className="mt-2 overflow-auto">
                            <table className="min-w-full text-[11px]">
                              <thead className="text-left text-slate-400">
                                <tr>
                                  {["Created", "Action", "Review", "Before", "After", "Reason"].map((label) => (
                                    <th key={label} className="px-2 py-1 font-medium">{label}</th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {researchCandidateReviews.map((review) => (
                                  <tr key={review.id} className="border-t border-border/60">
                                    <td className="px-2 py-1">{formatDateTime(review.created_at)}</td>
                                    <td className="px-2 py-1">{review.action}</td>
                                    <td className="px-2 py-1">{review.status}</td>
                                    <td className="px-2 py-1">{review.previous_candidate_status}</td>
                                    <td className="px-2 py-1">{review.next_candidate_status ?? review.previous_candidate_status}</td>
                                    <td className="px-2 py-1">{review.reason ?? "-"}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      ) : null}
                      <InlineStatus
                        error={getErrorMessage(selectedResearchCandidateQualificationQuery.error)}
                      />
                      <InlineStatus
                        error={getErrorMessage(selectedResearchCandidateQualificationHistoryQuery.error)}
                        success={
                          evaluateResearchCandidateQualificationMutation.data
                            ? qualificationTrendLabel(
                                evaluateResearchCandidateQualificationMutation.data.trend,
                              )
                            : undefined
                        }
                      />
                      <InlineStatus
                        error={getErrorMessage(selectedResearchCandidateTestnetReviewDossierQuery.error)}
                      />
                      <InlineStatus
                        error={getErrorMessage(selectedResearchCandidateReviewsQuery.error)}
                      />
                    </div>
                    <div className="mt-4 rounded-xl border border-border/70 bg-black/10 p-3 text-xs text-slate-200">
                      <div className="font-semibold text-slate-100">Shadow Performance</div>
                      <div className="mt-2 grid gap-2 sm:grid-cols-2">
                        <div>
                          Total runs: {researchCandidateShadowPerformance?.total_shadow_runs ?? 0}
                        </div>
                        <div>
                          Status: {researchCandidateShadowPerformance?.status ?? "UNKNOWN"}
                        </div>
                        <div>
                          Would-submit:{" "}
                          {researchCandidateShadowPerformance?.would_submit_count ?? 0}
                          {" / "}
                          {researchCandidateShadowPerformance?.would_submit_rate_pct ?? "0"}%
                        </div>
                        <div>
                          Risk rejected:{" "}
                          {researchCandidateShadowPerformance?.risk_rejected_count ?? 0}
                          {" / "}
                          {researchCandidateShadowPerformance?.risk_rejection_rate_pct ?? "0"}%
                        </div>
                        <div>
                          No signal: {researchCandidateShadowPerformance?.no_signal_count ?? 0}
                        </div>
                        <div>
                          Skipped / error:{" "}
                          {(researchCandidateShadowPerformance?.skipped_count ?? 0) +
                            (researchCandidateShadowPerformance?.error_count ?? 0)}
                        </div>
                        <div>
                          Last shadow run:{" "}
                          {formatDateTime(researchCandidateShadowPerformance?.last_shadow_run_at)}
                        </div>
                        <div>
                          Runner coverage:{" "}
                          {researchCandidateShadowPerformance?.runner_alignment_current
                            ? "covered"
                            : "not covered"}
                        </div>
                      </div>
                      <div className="mt-3 rounded-xl border border-border/70 bg-surface/40 p-3">
                        Recommendation: {shadowPerformanceRecommendationLabel}
                      </div>
                      <InlineStatus
                        error={getErrorMessage(selectedResearchCandidateShadowPerformanceQuery.error)}
                      />
                    </div>
                    <div className="mt-4 rounded-xl border border-border/70 bg-black/10 p-3 text-xs text-slate-200">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="font-semibold text-slate-100">
                          Shadow Hypothetical PnL
                        </div>
                        <div className="rounded-full border border-border/70 px-3 py-1 text-[11px] uppercase tracking-[0.2em] text-slate-300">
                          {researchCandidateShadowPnl?.latest_shadow_pnl_status ?? "UNKNOWN"}
                        </div>
                      </div>
                      <div className="mt-2 text-slate-300">
                        Research-only. This does not create orders.
                      </div>
                      <div className="mt-3 max-w-xs">
                        <Field
                          label="Holding Windows"
                          value={researchShadowPnlHoldingWindows}
                          onChange={setResearchShadowPnlHoldingWindows}
                          placeholder="1,3,5,10"
                        />
                      </div>
                      <div className="mt-3 grid gap-2 sm:grid-cols-3">
                        <div>
                          Attributed:{" "}
                          {researchCandidateShadowPnl?.summary.total_attributed_runs ?? 0}
                        </div>
                        <div>
                          Insufficient:{" "}
                          {researchCandidateShadowPnl?.summary
                            .insufficient_forward_data_count ?? 0}
                        </div>
                        <div>
                          Best:{" "}
                          {researchCandidateShadowPnl?.best_holding_window
                            ? `${researchCandidateShadowPnl.best_holding_window} candles / ${researchCandidateShadowPnl.best_avg_net_pnl_pct ?? "-"}%`
                            : "N/A"}
                        </div>
                      </div>
                      {(researchCandidateShadowPnl?.summary.extreme_pnl_count ?? 0) > 0 && (
                        <div className="mt-3 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
                          Attribution PnL is unusually large; inspect candle continuity and timestamps.
                        </div>
                      )}
                      {(researchCandidateShadowPnl?.summary.gap_detected_count ?? 0) > 0 && (
                        <div className="mt-2 rounded-md border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs text-rose-100">
                          Candle gaps detected in attribution. Review entry and exit timestamps before trusting results.
                        </div>
                      )}
                      <div className="mt-3 overflow-auto rounded-xl border border-border/70">
                        <table className="min-w-full text-[11px]">
                          <thead className="bg-surface/60 text-left text-slate-300">
                            <tr>
                              {["Window", "Trades", "Win %", "Avg Net", "Median", "Best", "Worst", "Total", "Fee Drag", "Rec"].map((label) => (
                                <th key={label} className="px-2 py-1 font-medium">{label}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {(researchCandidateShadowPnl?.summary.per_holding_window ?? []).map((window) => (
                              <tr key={window.holding_window} className="border-t border-border/60">
                                <td className="px-2 py-1">{window.holding_window}</td>
                                <td className="px-2 py-1">{window.trade_count}</td>
                                <td className="px-2 py-1">{window.win_rate}</td>
                                <td className="px-2 py-1">{window.avg_net_pnl_pct}</td>
                                <td className="px-2 py-1">{window.median_net_pnl_pct}</td>
                                <td className="px-2 py-1">{window.best_net_pnl_pct}</td>
                                <td className="px-2 py-1">{window.worst_net_pnl_pct}</td>
                                <td className="px-2 py-1">{window.total_net_pnl_pct}</td>
                                <td className="px-2 py-1">{window.fee_drag_pct}</td>
                                <td className="px-2 py-1">{window.recommendation}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <div className="mt-3 overflow-auto rounded-xl border border-border/70">
                        <table className="min-w-full text-[11px]">
                          <thead className="bg-surface/60 text-left text-slate-300">
                            <tr>
                              {["Run", "Shadow Time", "Entry", "Exit", "Status", "Windows"].map((label) => (
                                <th key={label} className="px-2 py-1 font-medium">{label}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {(researchCandidateShadowPnl?.trades ?? []).slice(0, 10).map((trade) => {
                              const latestWindow = trade.holding_windows.find((window) => window.net_pnl_pct !== null)
                                ?? trade.holding_windows[0];
                              return (
                                <tr key={trade.shadow_run_id} className="border-t border-border/60">
                                  <td className="px-2 py-1 font-mono">{shortenId(trade.shadow_run_id)}</td>
                                  <td className="px-2 py-1">{formatDateTime(trade.signal_time ?? trade.shadow_created_at)}</td>
                                  <td className="px-2 py-1">
                                    {trade.entry_price ?? "-"} @ {formatDateTime(trade.entry_candle_open_time)}
                                  </td>
                                  <td className="px-2 py-1">
                                    {latestWindow?.exit_price ?? "-"} @ {formatDateTime(latestWindow?.exit_candle_close_time ?? null)}
                                  </td>
                                  <td className="px-2 py-1">{latestWindow?.attribution_status ?? trade.status}</td>
                                  <td className="px-2 py-1">
                                    {trade.holding_windows
                                      .map((window) =>
                                        `${window.holding_window}:${window.net_pnl_pct ?? "NA"}`,
                                      )
                                      .join(" | ")}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                      <InlineStatus
                        error={getErrorMessage(selectedResearchCandidateShadowPnlQuery.error)}
                      />
                    </div>
                    <div className="mt-4 text-xs uppercase tracking-[0.2em] text-muted">
                      Linked Shadow Runs
                    </div>
                    <div className="mt-2 overflow-auto rounded-2xl border border-border">
                      <table className="min-w-full text-xs">
                        <thead className="bg-surface/60 text-left text-slate-300">
                          <tr>
                            <th className="px-3 py-2">Run</th>
                            <th className="px-3 py-2">Created</th>
                            <th className="px-3 py-2">Decision</th>
                            <th className="px-3 py-2">Status</th>
                            <th className="px-3 py-2">Signal</th>
                            <th className="px-3 py-2">Risk</th>
                            <th className="px-3 py-2">Linked</th>
                          </tr>
                        </thead>
                        <tbody>
                          {researchCandidateShadowRuns.length === 0 ? (
                            <tr>
                              <td className="px-3 py-3 text-slate-400" colSpan={7}>
                                No linked shadow runs yet.
                              </td>
                            </tr>
                          ) : (
                            researchCandidateShadowRuns.map((run) => (
                              <tr
                                key={run.shadow_run_id}
                                className="border-t border-border/60 text-slate-200"
                              >
                                <td className="px-3 py-2 font-mono">
                                  {shortenId(run.shadow_run_id)}
                                </td>
                                <td className="px-3 py-2">
                                  {formatDateTime(run.shadow_created_at)}
                                </td>
                                <td className="px-3 py-2">{run.decision}</td>
                                <td className="px-3 py-2">{run.status}</td>
                                <td className="px-3 py-2">
                                  {run.signal_id ? shortenId(run.signal_id) : "-"}
                                </td>
                                <td className="px-3 py-2">
                                  {run.risk_decision_id ? shortenId(run.risk_decision_id) : "-"}
                                </td>
                                <td className="px-3 py-2">{formatDateTime(run.linked_at)}</td>
                              </tr>
                            ))
                          )}
                        </tbody>
                      </table>
                    </div>
                    <InlineStatus
                      error={getErrorMessage(selectedResearchCandidateShadowRunsQuery.error)}
                    />
                    <div className="mt-4 text-xs uppercase tracking-[0.2em] text-muted">
                      Latest Findings
                    </div>
                    <div className="mt-2 space-y-1 text-xs text-slate-300">
                      {(latestResearchCandidateObservation?.summary.findings ?? []).length === 0
                        ? "No observation findings yet."
                        : latestResearchCandidateObservation?.summary.findings.map((finding) => (
                            <div key={finding.code}>
                              {finding.code}: {finding.message}
                            </div>
                          ))}
                    </div>
                    {(latestResearchCandidateObservation?.summary.recommendations ?? []).length >
                    0 ? (
                      <>
                        <div className="mt-4 text-xs uppercase tracking-[0.2em] text-muted">
                          Recommendations
                        </div>
                        <div className="mt-2 space-y-1 text-xs text-slate-300">
                          {latestResearchCandidateObservation?.summary.recommendations.map(
                            (recommendation) => (
                              <div key={recommendation}>{recommendation}</div>
                            ),
                          )}
                        </div>
                      </>
                    ) : null}
                    <div className="mt-4 text-xs uppercase tracking-[0.2em] text-muted">
                      Observation History
                    </div>
                    <div className="mt-2 overflow-auto rounded-2xl border border-border">
                      <table className="min-w-full text-xs">
                        <thead className="bg-surface/60 text-left text-slate-300">
                          <tr>
                            <th className="px-3 py-2">Observed</th>
                            <th className="px-3 py-2">Status</th>
                            <th className="px-3 py-2">Runner</th>
                            <th className="px-3 py-2">Readiness</th>
                            <th className="px-3 py-2">Freshness</th>
                            <th className="px-3 py-2">Drift</th>
                            <th className="px-3 py-2">Eligible</th>
                            <th className="px-3 py-2">Recommendations</th>
                          </tr>
                        </thead>
                        <tbody>
                          {researchCandidateObservationHistory.length === 0 ? (
                            <tr>
                              <td className="px-3 py-3 text-slate-400" colSpan={8}>
                                No persisted observation history yet.
                              </td>
                            </tr>
                          ) : (
                            researchCandidateObservationHistory.map((item) => (
                              <tr
                                key={item.observation.observation_id}
                                className="border-t border-border/60 text-slate-200"
                              >
                                <td className="px-3 py-2">
                                  {formatDateTime(item.observation.last_observed_at)}
                                </td>
                                <td className="px-3 py-2">
                                  {item.observation.status} / {item.observation.decision}
                                </td>
                                <td className="px-3 py-2">
                                  {item.observation.runner_alignment.strategy_config_matches_runner
                                    ? "Aligned"
                                    : "Mismatch"}
                                </td>
                                <td className="px-3 py-2">
                                  {item.observation.summary.latest_readiness_status ?? "UNKNOWN"}
                                </td>
                                <td className="px-3 py-2">
                                  {item.freshness_status}
                                  {item.observation_age_seconds !== null
                                    ? ` (${item.observation_age_seconds}s)`
                                    : ""}
                                </td>
                                <td className="px-3 py-2">
                                  {item.runner_config_drifted ? "Drifted" : "Stable"}
                                </td>
                                <td className="px-3 py-2">
                                  {item.accept_for_shadow_eligible ? "Yes" : "No"}
                                </td>
                                <td className="px-3 py-2">
                                  {item.observation.summary.recommendations.length > 0
                                    ? item.observation.summary.recommendations.join(" | ")
                                    : "-"}
                                </td>
                              </tr>
                            ))
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
                <div className="mt-4 rounded-2xl border border-border bg-surface/40 p-4">
                  <div className="text-xs uppercase tracking-[0.2em] text-muted">
                    Lifecycle Events
                  </div>
                  <div className="mt-3 overflow-auto rounded-2xl border border-border">
                    <table className="min-w-full text-sm">
                      <thead className="bg-surface/60 text-left text-slate-300">
                        <tr>
                          {["Created", "From", "To", "Decision", "Reason", "Notes"].map(
                            (label) => (
                              <th key={label} className="px-3 py-2 font-medium">
                                {label}
                              </th>
                            ),
                          )}
                        </tr>
                      </thead>
                      <tbody>
                        {researchCandidateEvents.map((event) => (
                          <tr key={event.id} className="border-t border-border">
                            <td className="px-3 py-2">{formatDateTime(event.created_at)}</td>
                            <td className="px-3 py-2">{event.previous_status ?? "-"}</td>
                            <td className="px-3 py-2">{event.next_status}</td>
                            <td className="px-3 py-2">{event.decision}</td>
                            <td className="px-3 py-2">{event.reason ?? "-"}</td>
                            <td className="px-3 py-2">{event.notes ?? "-"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <InlineStatus
                    error={getErrorMessage(selectedResearchCandidateEventsQuery.error)}
                  />
                </div>
              </Panel>
            </section>
          )}

          {section === "events" && (
            <section className="grid gap-4">
              <Panel title="System Events">
                <div className="mb-3 grid gap-3 md:grid-cols-3">
                  <Field
                    label="Event Type"
                    value={eventTypeFilter}
                    onChange={setEventTypeFilter}
                    placeholder="risk."
                  />
                  <Field
                    label="Source"
                    value={eventSourceFilter}
                    onChange={setEventSourceFilter}
                    placeholder="aegis-quant-api"
                  />
                  <Field
                    label="Correlation ID"
                    value={eventCorrelationFilter}
                    onChange={setEventCorrelationFilter}
                    placeholder="2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0"
                  />
                </div>
                <EventsTable
                  events={events}
                  loading={eventsQuery.isLoading}
                  error={getErrorMessage(eventsQuery.error)}
                />
              </Panel>
            </section>
          )}

          {section === "settings" && (
            <section className="grid gap-4">
              <Panel title="Testnet Exchange">
                <div className="mb-4 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                  TESTNET ONLY. No live trading, no production Binance endpoints, and no automatic paper-pipeline mutation from private exchange events.
                </div>
                <div className="grid gap-3 md:grid-cols-3">
                  <HeaderStat
                    label="Adapter"
                    value={exchangeTestnetStatusQuery.data?.configured ? "configured" : "missing creds"}
                    tone={exchangeTestnetStatusQuery.data?.configured ? "ok" : "warning"}
                  />
                  <HeaderStat
                    label="Environment"
                    value={exchangeTestnetStatusQuery.data?.environment ?? "testnet"}
                    tone="neutral"
                  />
                  <HeaderStat
                    label="Kill Switch"
                    value={riskQuery.data?.kill_switch.enabled ? "active" : "inactive"}
                    tone={riskQuery.data?.kill_switch.enabled ? "danger" : "ok"}
                  />
                </div>
                <InlineStatus error={getErrorMessage(exchangeTestnetStatusQuery.error)} />
              </Panel>
              <Panel title="Shadow Run">
                <div className="mb-4 grid gap-3 md:grid-cols-4">
                  <HeaderStat
                    label="Runner"
                    value={exchangeTestnetShadowRunnerStatusQuery.data?.state.status ?? "STOPPED"}
                    tone={
                      exchangeTestnetShadowRunnerStatusQuery.data?.state.status === "ERROR"
                        ? "danger"
                        : exchangeTestnetShadowRunnerStatusQuery.data?.state.status === "RUNNING"
                          ? "ok"
                          : "neutral"
                    }
                  />
                  <HeaderStat
                    label="Enabled"
                    value={
                      exchangeTestnetShadowRunnerStatusQuery.data?.config.enabled ? "yes" : "no"
                    }
                    tone="neutral"
                  />
                  <HeaderStat
                    label="Last Tick"
                    value={formatRelativeAge(
                      exchangeTestnetShadowRunnerStatusQuery.data?.state.last_tick_at,
                    )}
                    tone="neutral"
                  />
                  <HeaderStat
                    label="Runs"
                    value={String(
                      exchangeTestnetShadowRunnerStatusQuery.data?.state.total_runs ?? 0,
                    )}
                    tone="neutral"
                  />
                </div>
                <div className="mb-4 grid gap-3 md:grid-cols-4">
                  <Field
                    label="Interval Seconds"
                    value={String(shadowRunnerConfigForm.interval_seconds ?? 60)}
                    onChange={(value) =>
                      setShadowRunnerConfigForm((current) => ({
                        ...current,
                        interval_seconds: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Strategies"
                    value={String((shadowRunnerConfigForm.strategies as string[] | undefined)?.join(",") ?? "")}
                    onChange={(value) =>
                      setShadowRunnerConfigForm((current) => ({
                        ...current,
                        strategies: value.split(",").map((item) => item.trim()).filter(Boolean),
                      }))
                    }
                  />
                  <Field
                    label="Symbols"
                    value={String((shadowRunnerConfigForm.symbols as string[] | undefined)?.join(",") ?? "")}
                    onChange={(value) =>
                      setShadowRunnerConfigForm((current) => ({
                        ...current,
                        symbols: value.split(",").map((item) => item.trim()).filter(Boolean),
                      }))
                    }
                  />
                  <Field
                    label="Timeframe"
                    value={String(shadowRunnerConfigForm.timeframe ?? "1m")}
                    onChange={(value) =>
                      setShadowRunnerConfigForm((current) => ({ ...current, timeframe: value }))
                    }
                  />
                  <Field
                    label="Max Runs / Tick"
                    value={String(shadowRunnerConfigForm.max_runs_per_tick ?? 1)}
                    onChange={(value) =>
                      setShadowRunnerConfigForm((current) => ({
                        ...current,
                        max_runs_per_tick: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Stale Feed Policy"
                    value={String(shadowRunnerConfigForm.stale_feed_policy ?? "SKIP")}
                    onChange={(value) =>
                      setShadowRunnerConfigForm((current) => ({
                        ...current,
                        stale_feed_policy: value.toUpperCase(),
                      }))
                    }
                  />
                  <Field
                    label="Notes"
                    value={String(shadowRunnerConfigForm.notes ?? "")}
                    onChange={(value) =>
                      setShadowRunnerConfigForm((current) => ({ ...current, notes: value }))
                    }
                  />
                  <label className="flex items-center gap-2 rounded-xl border border-border bg-surface/50 px-3 py-2 text-sm">
                    <input
                      type="checkbox"
                      checked={Boolean(shadowRunnerConfigForm.enabled)}
                      onChange={(event) =>
                        setShadowRunnerConfigForm((current) => ({
                          ...current,
                          enabled: event.target.checked,
                        }))
                      }
                    />
                    Enabled
                  </label>
                </div>
                <div className="mb-4 flex flex-wrap gap-2">
                  <button
                    className="rounded-xl border border-sky-400/40 bg-sky-400/10 px-4 py-2 text-sm"
                    onClick={() => exchangeTestnetShadowRunnerConfigUpdateMutation.mutate()}
                    disabled={
                      exchangeTestnetShadowRunnerConfigUpdateMutation.isPending ||
                      user.role !== "OWNER"
                    }
                  >
                    {exchangeTestnetShadowRunnerConfigUpdateMutation.isPending
                      ? "Saving..."
                      : "Save Runner Config"}
                  </button>
                  {[
                    ["RUN_ONCE", "Run Once"],
                    ["PAUSE", "Pause"],
                    ["RESUME", "Resume"],
                    ["START", "Start"],
                    ["STOP", "Stop"],
                  ].map(([action, label]) => (
                    <button
                      key={action}
                      className="rounded-xl border border-border bg-surface/60 px-4 py-2 text-sm"
                      onClick={() => exchangeTestnetShadowRunnerControlMutation.mutate(action)}
                      disabled={
                        exchangeTestnetShadowRunnerControlMutation.isPending ||
                        (["START", "STOP"].includes(action)
                          ? user.role !== "OWNER"
                          : !(user.role === "OWNER" || user.role === "OPERATOR"))
                      }
                    >
                      {label}
                    </button>
                  ))}
                </div>
                <InlineStatus
                  error={
                    getErrorMessage(exchangeTestnetShadowRunnerStatusQuery.error) ??
                    getErrorMessage(exchangeTestnetShadowRunnerConfigUpdateMutation.error) ??
                    getErrorMessage(exchangeTestnetShadowRunnerControlMutation.error)
                  }
                  success={
                    exchangeTestnetShadowRunnerControlMutation.data?.tick
                      ? `${exchangeTestnetShadowRunnerControlMutation.data.tick.status} ${exchangeTestnetShadowRunnerControlMutation.data.tick.correlation_id}`
                      : exchangeTestnetShadowRunnerConfigUpdateMutation.data
                        ? "Shadow runner config updated"
                        : undefined
                  }
                />
                <div className="grid gap-3 md:grid-cols-4">
                  <Field
                    label="Strategy"
                    value={testnetShadowStrategyId}
                    onChange={setTestnetShadowStrategyId}
                    placeholder="momentum_v1"
                  />
                  <Field
                    label="Symbol"
                    value={testnetShadowSymbol}
                    onChange={setTestnetShadowSymbol}
                    placeholder="BTCUSDT"
                  />
                  <Field
                    label="Timeframe"
                    value={testnetShadowTimeframe}
                    onChange={setTestnetShadowTimeframe}
                    placeholder="1m"
                  />
                  <button
                    className="rounded-xl border border-sky-400/40 bg-sky-400/10 px-4 py-2 text-sm"
                    onClick={() => exchangeTestnetShadowRunMutation.mutate()}
                    disabled={
                      exchangeTestnetShadowRunMutation.isPending ||
                      !(user.role === "OWNER" || user.role === "OPERATOR")
                    }
                  >
                    {exchangeTestnetShadowRunMutation.isPending ? "Running..." : "Run Shadow"}
                  </button>
                </div>
                <InlineStatus
                  error={getErrorMessage(exchangeTestnetShadowRunMutation.error)}
                  success={
                    exchangeTestnetShadowRunMutation.data
                      ? `${exchangeTestnetShadowRunMutation.data.run.decision} ${exchangeTestnetShadowRunMutation.data.run.run_id}`
                      : undefined
                  }
                />
              </Panel>
              <Panel title="Recent Shadow Runs">
                <div className="space-y-2 text-sm text-slate-200">
                  {(exchangeTestnetShadowRunsQuery.data?.runs ?? []).map((item) => (
                    <div
                      key={item.run_id}
                      className={cn(
                        "rounded-xl border bg-surface/60 px-3 py-2",
                        selectedShadowRunId === item.run_id ? "border-accent" : "border-border",
                      )}
                    >
                      <button
                        className="w-full text-left"
                        onClick={() => setSelectedShadowRunId(item.run_id)}
                        type="button"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <span>{formatDateTime(item.created_at)}</span>
                          <span>{item.decision}</span>
                        </div>
                        <div className="text-slate-400">
                          {item.strategy_id} {item.symbol} signal={shortenId(item.signal_id)} risk={shortenId(item.risk_decision_id)}
                        </div>
                        <div className="text-slate-400">
                          price={item.resolved_price ?? "-"} source={item.price_source ?? "-"}
                        </div>
                      </button>
                      {item.decision === "WOULD_SUBMIT" &&
                      (user.role === "OWNER" || user.role === "OPERATOR") ? (
                        <button
                          className="mt-3 rounded-lg border border-emerald-400/40 bg-emerald-400/10 px-3 py-1 text-xs text-emerald-100"
                          disabled={exchangeTestnetShadowPromotionPreviewMutation.isPending}
                          onClick={() =>
                            exchangeTestnetShadowPromotionPreviewMutation.mutate(item.run_id)
                          }
                          type="button"
                        >
                          {exchangeTestnetShadowPromotionPreviewMutation.isPending
                            ? "Previewing..."
                            : "Preview Promotion"}
                        </button>
                      ) : null}
                    </div>
                  ))}
                </div>
                <InlineStatus
                  error={
                    getErrorMessage(exchangeTestnetShadowRunsQuery.error) ??
                    getErrorMessage(exchangeTestnetShadowPromotionPreviewMutation.error)
                  }
                  success={
                    exchangeTestnetShadowPromotionPreviewMutation.data
                      ? exchangeTestnetShadowPromotionPreviewMutation.data.promotion.promotion_id
                      : undefined
                  }
                />
                {selectedShadowRun ? (
                  <div className="mt-4 rounded-xl border border-border bg-surface/40 px-3 py-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Shadow Run Detail</div>
                    <div className="mt-2 space-y-1">
                      <div>Run ID: {selectedShadowRun.run_id}</div>
                      <div>Correlation ID: {selectedShadowRun.correlation_id}</div>
                      <div>Status: {selectedShadowRun.status}</div>
                      <div>Reasons: {selectedShadowRun.reasons.join(", ") || "-"}</div>
                    </div>
                    <pre className="mt-3 overflow-auto rounded-lg border border-border/60 bg-surface/70 p-3 text-[11px] text-slate-200">
                      {JSON.stringify(
                        {
                          would_submit_payload: selectedShadowRun.would_submit_order,
                          reasons: selectedShadowRun.reasons,
                          correlation_id: selectedShadowRun.correlation_id,
                        },
                        null,
                        2,
                      )}
                    </pre>
                  </div>
                ) : null}
              </Panel>
              <Panel title="Shadow Promotions">
                <div className="space-y-2 text-sm text-slate-200">
                  {(exchangeTestnetShadowPromotionsQuery.data?.promotions ?? []).map((item) => (
                    <button
                      key={item.promotion_id}
                      className={cn(
                        "w-full rounded-xl border bg-surface/60 px-3 py-2 text-left",
                        selectedShadowPromotionId === item.promotion_id
                          ? "border-accent"
                          : "border-border",
                      )}
                      onClick={() => setSelectedShadowPromotionId(item.promotion_id)}
                      type="button"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <span>{item.status}</span>
                        <span>{formatDateTime(item.expires_at)}</span>
                      </div>
                      <div className="text-slate-400">
                        shadow={shortenId(item.shadow_run_id)} {item.symbol} client={item.client_order_id ?? "-"}
                      </div>
                    </button>
                  ))}
                </div>
                <InlineStatus
                  error={
                    getErrorMessage(exchangeTestnetShadowPromotionsQuery.error) ??
                    getErrorMessage(exchangeTestnetShadowPromotionSubmitMutation.error)
                  }
                  success={
                    exchangeTestnetShadowPromotionSubmitMutation.data
                      ? exchangeTestnetShadowPromotionSubmitMutation.data.result.client_order_id
                      : undefined
                  }
                />
                {selectedShadowPromotion ? (
                  <div className="mt-4 rounded-xl border border-border bg-surface/40 px-3 py-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Promotion Detail</div>
                    <div className="mt-2 space-y-1">
                      <div>Promotion ID: {selectedShadowPromotion.promotion_id}</div>
                      <div>Shadow Run ID: {selectedShadowPromotion.shadow_run_id}</div>
                      <div>Risk Decision ID: {selectedShadowPromotion.risk_decision_id}</div>
                      <div>Resolved Price: {selectedShadowPromotion.resolved_price ?? "-"}</div>
                      <div>Price Source: {selectedShadowPromotion.price_source ?? "-"}</div>
                      <div>Status: {selectedShadowPromotion.status}</div>
                      <div>Expires At: {formatDateTime(selectedShadowPromotion.expires_at)}</div>
                    </div>
                    <pre className="mt-3 overflow-auto rounded-lg border border-border/60 bg-surface/70 p-3 text-[11px] text-slate-200">
                      {JSON.stringify(selectedShadowPromotion.would_submit_payload, null, 2)}
                    </pre>
                    {user.role === "OWNER" && selectedShadowPromotion.status === "PREVIEWED" ? (
                      <div className="mt-3 grid gap-3 md:grid-cols-[1fr_auto]">
                        <Field
                          label="Confirmation"
                          value={shadowPromotionConfirmation}
                          onChange={setShadowPromotionConfirmation}
                          placeholder={`PROMOTE TESTNET ${selectedShadowPromotion.symbol}`}
                        />
                        <button
                          className="rounded-xl border border-amber-400/40 bg-amber-400/10 px-4 py-2 text-sm"
                          disabled={exchangeTestnetShadowPromotionSubmitMutation.isPending}
                          onClick={() =>
                            exchangeTestnetShadowPromotionSubmitMutation.mutate(
                              selectedShadowPromotion.promotion_id,
                            )
                          }
                        >
                          {exchangeTestnetShadowPromotionSubmitMutation.isPending
                            ? "Submitting..."
                            : "Submit Promotion"}
                        </button>
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </Panel>
              <Panel title="Private Stream Status">
                <div className="grid gap-3 md:grid-cols-4">
                  <HeaderStat
                    label="Stream"
                    value={exchangePrivateStreamStatusQuery.data?.state.status ?? "DISCONNECTED"}
                    tone={exchangePrivateStreamStatusQuery.data?.state.is_stale ? "warning" : "ok"}
                  />
                  <HeaderStat
                    label="Last Event"
                    value={formatRelativeAge(exchangePrivateStreamStatusQuery.data?.state.last_event_at)}
                    tone="neutral"
                  />
                  <HeaderStat
                    label="Reconnects"
                    value={String(exchangePrivateStreamStatusQuery.data?.state.reconnect_count ?? 0)}
                    tone="neutral"
                  />
                  <HeaderStat
                    label="Listen Key"
                    value={exchangePrivateStreamStatusQuery.data?.state.listen_key_hash ? "hashed" : "missing"}
                    tone={exchangePrivateStreamStatusQuery.data?.state.listen_key_hash ? "ok" : "warning"}
                  />
                </div>
                <div className="mt-4 rounded-xl border border-border bg-surface/50 p-3 text-sm text-slate-200">
                  <div>Last error: {exchangePrivateStreamStatusQuery.data?.state.last_error ?? "none"}</div>
                  <div>Connected at: {formatDateTime(exchangePrivateStreamStatusQuery.data?.state.connected_at)}</div>
                  <div>Updated at: {formatDateTime(exchangePrivateStreamStatusQuery.data?.state.updated_at)}</div>
                </div>
                {(user.role === "OWNER" || user.role === "OPERATOR") ? (
                  <div className="mt-4 grid gap-3 md:grid-cols-[1fr_auto_auto_auto]">
                    <Field
                      label="Listen Key"
                      value={privateStreamListenKey}
                      onChange={setPrivateStreamListenKey}
                      placeholder="testnet listen key for keepalive/close"
                    />
                    <button
                      className="rounded-xl border border-emerald-400/40 bg-emerald-400/10 px-4 py-2 text-sm"
                      onClick={() => exchangePrivateStreamCreateListenKeyMutation.mutate()}
                      disabled={exchangePrivateStreamCreateListenKeyMutation.isPending}
                    >
                      Create
                    </button>
                    <button
                      className="rounded-xl border border-sky-400/40 bg-sky-400/10 px-4 py-2 text-sm"
                      onClick={() => exchangePrivateStreamKeepaliveMutation.mutate()}
                      disabled={
                        exchangePrivateStreamKeepaliveMutation.isPending ||
                        privateStreamListenKey.trim().length === 0
                      }
                    >
                      Keepalive
                    </button>
                    <button
                      className="rounded-xl border border-rose-400/40 bg-rose-400/10 px-4 py-2 text-sm"
                      onClick={() => exchangePrivateStreamCloseMutation.mutate()}
                      disabled={
                        exchangePrivateStreamCloseMutation.isPending ||
                        privateStreamListenKey.trim().length === 0
                      }
                    >
                      Close
                    </button>
                  </div>
                ) : null}
                <InlineStatus
                  error={
                    getErrorMessage(exchangePrivateStreamStatusQuery.error) ??
                    getErrorMessage(exchangePrivateStreamCreateListenKeyMutation.error) ??
                    getErrorMessage(exchangePrivateStreamKeepaliveMutation.error) ??
                    getErrorMessage(exchangePrivateStreamCloseMutation.error)
                  }
                  success={
                    exchangePrivateStreamCreateListenKeyMutation.data?.listen_key_masked
                      ? `created ${exchangePrivateStreamCreateListenKeyMutation.data.listen_key_masked}`
                      : exchangePrivateStreamKeepaliveMutation.data?.listen_key_masked
                        ? `keepalive ${exchangePrivateStreamKeepaliveMutation.data.listen_key_masked}`
                        : exchangePrivateStreamCloseMutation.data?.listen_key_masked
                          ? `closed ${exchangePrivateStreamCloseMutation.data.listen_key_masked}`
                          : undefined
                  }
                />
              </Panel>
              <Panel title="Symbols and Balances">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2 text-sm text-slate-200">
                    {(exchangeTestnetSymbolsQuery.data?.symbols ?? []).slice(0, 8).map((item) => (
                      <div key={item.symbol} className="rounded-xl border border-border bg-surface/60 px-3 py-2">
                        {item.symbol} {item.base_asset}/{item.quote_asset}
                      </div>
                    ))}
                  </div>
                  <div className="space-y-2 text-sm text-slate-200">
                    {(exchangeTestnetBalancesQuery.data?.balances ?? []).slice(0, 8).map((item) => (
                      <div key={item.asset} className="rounded-xl border border-border bg-surface/60 px-3 py-2">
                        {item.asset} free {item.free} locked {item.locked}
                      </div>
                    ))}
                    <InlineStatus error={getErrorMessage(exchangeTestnetBalancesQuery.error)} />
                  </div>
                </div>
              </Panel>
              <Panel title="Recent Testnet Orders">
                <div className="space-y-2 text-sm text-slate-200">
                  {(exchangeTestnetOrdersQuery.data?.orders ?? []).map((item) => (
                    <div
                      key={item.id}
                      className={cn(
                        "w-full rounded-xl border bg-surface/60 px-3 py-2 text-left",
                        selectedTestnetOrderId === item.client_order_id
                          ? "border-accent"
                          : "border-border",
                      )}
                      onClick={() => setSelectedTestnetOrderId(item.client_order_id)}
                    >
                      <div>{item.client_order_id}</div>
                      <div className="text-slate-400">
                        {item.symbol} {item.side} {item.order_type} status={item.status}
                      </div>
                      <div className="text-slate-400">
                        execution={item.execution_state}
                        {item.execution_state === "RECONCILIATION_REQUIRED" ||
                        item.execution_state === "UNKNOWN_EXCHANGE_STATE" ||
                        item.execution_state === "FAILED"
                          ? "  attention"
                          : ""}
                      </div>
                      {user.role === "OWNER" ? (
                        <button
                          className="mt-2 rounded-lg border border-border px-3 py-1 text-xs"
                          onClick={() => exchangeTestnetCancelMutation.mutate(item.client_order_id)}
                          disabled={testnetConfirmation !== "TESTNET ORDER" || exchangeTestnetCancelMutation.isPending}
                        >
                          Cancel
                        </button>
                      ) : null}
                    </div>
                  ))}
                </div>
                {exchangeTestnetLifecycleQuery.data ? (
                  <div className="mt-4 rounded-xl border border-border bg-surface/40 px-3 py-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">
                      Lifecycle {exchangeTestnetLifecycleQuery.data.current_state}
                    </div>
                    <div className="mt-2 space-y-2">
                      {exchangeTestnetLifecycleQuery.data.events.map((event, index) => (
                        <div key={`${event.created_at}-${index}`} className="rounded-lg border border-border/60 px-2 py-2">
                          <div>
                            {event.previous_state ?? "-"} to {event.next_state}
                          </div>
                          <div className="text-slate-400">
                            {event.transition_source} {event.reason ?? "-"} {formatDateTime(event.created_at)}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}
                {selectedTestnetOrderRepairable ? (
                  <div className="mt-4 rounded-xl border border-border bg-surface/40 px-3 py-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Repair Controls</div>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <Field
                        label="Action"
                        value={testnetRepairAction}
                        onChange={setTestnetRepairAction}
                        as="select"
                        options={
                          user.role === "OWNER"
                            ? [
                                "MANUAL_RECHECK",
                                "MARK_RECONCILIATION_REQUIRED",
                                "MARK_ACKED",
                                "MARK_CANCELLED",
                                "MARK_REJECTED",
                                "MARK_FAILED",
                                "SAFE_CANCEL_REQUEST",
                              ]
                            : ["MANUAL_RECHECK", "MARK_RECONCILIATION_REQUIRED"]
                        }
                      />
                      <Field
                        label="Reason"
                        value={testnetRepairReason}
                        onChange={setTestnetRepairReason}
                        placeholder="operator_requested_recheck"
                      />
                    </div>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <Field
                        label={`Type "${repairConfirmationText}"`}
                        value={testnetRepairConfirmation}
                        onChange={setTestnetRepairConfirmation}
                      />
                      <label className="block text-sm">
                        <span className="text-xs uppercase tracking-[0.18em] text-muted">Force</span>
                        <button
                          className={cn(
                            "mt-1 w-full rounded-lg border px-3 py-2 text-left text-sm",
                            testnetRepairForce
                              ? "border-rose-400/50 bg-rose-400/10 text-rose-100"
                              : "border-border bg-surface text-slate-200",
                          )}
                          onClick={() => setTestnetRepairForce((value) => !value)}
                          type="button"
                        >
                          {testnetRepairForce ? "Dangerous force enabled" : "Force disabled"}
                        </button>
                      </label>
                    </div>
                    {testnetRepairForce ? (
                      <div className="mt-3 rounded-lg border border-rose-400/40 bg-rose-400/10 px-3 py-2 text-rose-100">
                        Force actions are owner-only and may override missing cancellation evidence.
                      </div>
                    ) : null}
                    <button
                      className="mt-3 rounded-xl border border-amber-400/40 bg-amber-400/10 px-4 py-2 text-sm"
                      onClick={() =>
                        selectedTestnetOrder &&
                        exchangeTestnetRepairMutation.mutate(selectedTestnetOrder.client_order_id)
                      }
                      disabled={
                        !selectedTestnetOrder ||
                        exchangeTestnetRepairMutation.isPending ||
                        testnetRepairConfirmation !== repairConfirmationText
                      }
                    >
                      Apply Repair
                    </button>
                    <InlineStatus
                      error={getErrorMessage(exchangeTestnetRepairMutation.error)}
                      success={
                        exchangeTestnetRepairMutation.data
                          ? `${exchangeTestnetRepairMutation.data.action} ${exchangeTestnetRepairMutation.data.status.toLowerCase()}`
                          : undefined
                      }
                    />
                  </div>
                ) : null}
                {exchangeTestnetRepairsQuery.data ? (
                  <div className="mt-4 rounded-xl border border-border bg-surface/40 px-3 py-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Repair History</div>
                    <div className="mt-2 space-y-2">
                      {exchangeTestnetRepairsQuery.data.repairs.map((repair) => (
                        <div key={repair.id} className="rounded-lg border border-border/60 px-2 py-2">
                          <div>
                            {repair.action} {repair.status} {repair.previous_state ?? "-"} to{" "}
                            {repair.next_state ?? "-"}
                          </div>
                          <div className="text-slate-400">
                            {repair.reason ?? "-"} {formatDateTime(repair.created_at)}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}
              </Panel>
              <Panel title="Recent Private Stream Events">
                <div className="space-y-2 text-sm text-slate-200">
                  {(exchangePrivateStreamEventsQuery.data?.events ?? []).map((item) => (
                    <div key={item.id} className="rounded-xl border border-border bg-surface/60 px-3 py-2">
                      <div className="font-medium text-slate-100">
                        {item.event_type} {item.client_order_id ?? shortenId(item.id)}
                      </div>
                      <div className="text-slate-400">
                        {item.symbol ?? "n/a"} {item.execution_type ?? "-"} {item.order_status ?? "-"} {formatDateTime(item.received_at)}
                      </div>
                    </div>
                  ))}
                  <InlineStatus error={getErrorMessage(exchangePrivateStreamEventsQuery.error)} />
                </div>
              </Panel>
              <Panel title="Testnet Reconciliation">
                {(user.role === "OWNER" || user.role === "OPERATOR") ? (
                  <div className="mb-4">
                    <button
                      className="rounded-xl border border-emerald-400/40 bg-emerald-400/10 px-4 py-2 text-sm"
                      onClick={() => exchangeReconcileMutation.mutate()}
                      disabled={exchangeReconcileMutation.isPending}
                    >
                      Run Reconciliation
                    </button>
                  </div>
                ) : null}
                <InlineStatus
                  error={getErrorMessage(exchangeReconcileMutation.error)}
                  success={
                    exchangeReconcileMutation.data
                      ? `last run ${exchangeReconcileMutation.data.result.status.toLowerCase()} with ${exchangeReconcileMutation.data.result.mismatched_orders} mismatches`
                      : undefined
                  }
                />
                <div className="mt-4 grid gap-4 xl:grid-cols-2">
                  <div className="space-y-2 text-sm text-slate-200">
                    {(exchangeReconciliationRunsQuery.data?.runs ?? []).map((run) => (
                      <button
                        key={run.id}
                        className={cn(
                          "w-full rounded-xl border px-3 py-2 text-left",
                          selectedReconciliationRunId === run.id
                            ? "border-accent bg-accent/10"
                            : "border-border bg-surface/60",
                        )}
                        onClick={() => setSelectedReconciliationRunId(run.id)}
                        type="button"
                      >
                        <div>{shortenId(run.id)}</div>
                        <div className="text-slate-400">
                          {run.status} checked={run.checked_orders} mismatch={run.mismatched_orders} unknown={run.unknown_orders}
                        </div>
                      </button>
                    ))}
                    <InlineStatus error={getErrorMessage(exchangeReconciliationRunsQuery.error)} />
                  </div>
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-sm text-slate-200">
                    <div className="font-medium text-slate-100">Selected Run</div>
                    <div className="mt-2">
                      Status: {selectedExchangeReconciliationRunQuery.data?.run.status ?? "N/A"}
                    </div>
                    <div>
                      Failed reason: {selectedExchangeReconciliationRunQuery.data?.run.failed_reason ?? "none"}
                    </div>
                    <div className="mt-3 font-medium text-slate-100">Mismatch Details</div>
                    {(selectedExchangeReconciliationMismatchesQuery.data?.mismatches ?? []).map((mismatch) => (
                      <div
                        key={mismatch.id}
                        className="mt-2 rounded-lg border border-amber-400/30 bg-amber-500/10 p-2 text-xs"
                      >
                        <div className="font-medium text-amber-100">
                          {mismatch.mismatch_kind} {mismatch.client_order_id}
                        </div>
                        <div>
                          local={mismatch.local_status ?? "N/A"} exchange={mismatch.exchange_status ?? "N/A"} action={mismatch.action}
                        </div>
                      </div>
                    ))}
                    {!(selectedExchangeReconciliationMismatchesQuery.data?.mismatches ?? []).length ? (
                      <div className="mt-2 text-xs text-slate-400">
                        No mismatches for the selected run.
                      </div>
                    ) : null}
                    <InlineStatus error={getErrorMessage(selectedExchangeReconciliationRunQuery.error) ?? getErrorMessage(selectedExchangeReconciliationMismatchesQuery.error)} />
                  </div>
                </div>
              </Panel>
              {user.role === "OWNER" ? (
                <Panel title="Testnet Pipeline Preview">
                  <div className="grid gap-3 md:grid-cols-2">
                    <Field
                      label="Risk Decision ID"
                      value={testnetPipelineRiskDecisionId}
                      onChange={setTestnetPipelineRiskDecisionId}
                      placeholder="approved UUID"
                    />
                    <Field
                      label="Confirmation"
                      value={testnetPipelineConfirmation}
                      onChange={setTestnetPipelineConfirmation}
                      placeholder='SUBMIT TESTNET BTCUSDT'
                    />
                  </div>
                  <div className="mt-4 flex gap-3">
                    <button
                      className="rounded-xl border border-sky-400/40 bg-sky-400/10 px-4 py-2 text-sm"
                      onClick={() => exchangeTestnetPipelinePreviewMutation.mutate()}
                      disabled={
                        exchangeTestnetPipelinePreviewMutation.isPending ||
                        testnetPipelineRiskDecisionId.trim().length === 0
                      }
                    >
                      Preview Testnet Pipeline
                    </button>
                    <button
                      className="rounded-xl border border-amber-400/40 bg-amber-400/10 px-4 py-2 text-sm"
                      onClick={() => exchangeTestnetPipelineSubmitMutation.mutate()}
                      disabled={
                        exchangeTestnetPipelineSubmitMutation.isPending ||
                        !exchangeTestnetPipelinePreviewMutation.data ||
                        testnetPipelineConfirmation !==
                          exchangeTestnetPipelinePreviewMutation.data.preview.confirmation_text
                      }
                    >
                      Submit Previewed Testnet Order
                    </button>
                  </div>
                  {exchangeTestnetPipelinePreviewMutation.data ? (
                    <div className="mt-4 rounded-xl border border-border bg-surface/40 px-3 py-3 text-sm text-slate-200">
                      <div className="font-medium text-slate-100">
                        {exchangeTestnetPipelinePreviewMutation.data.preview.symbol}{" "}
                        {exchangeTestnetPipelinePreviewMutation.data.preview.side}
                      </div>
                      <div className="mt-2 grid gap-2 md:grid-cols-2">
                        <div>
                          Strategy {exchangeTestnetPipelinePreviewMutation.data.preview.strategy_id ?? "N/A"}
                        </div>
                        <div>
                          Signal {exchangeTestnetPipelinePreviewMutation.data.preview.signal_id ?? "N/A"}
                        </div>
                        <div>
                          Quantity {exchangeTestnetPipelinePreviewMutation.data.preview.quantity}
                        </div>
                        <div>
                          Quote notional {exchangeTestnetPipelinePreviewMutation.data.preview.quote_notional}
                        </div>
                        <div>
                          Reference price {exchangeTestnetPipelinePreviewMutation.data.preview.reference_price}
                        </div>
                        <div>
                          Type {exchangeTestnetPipelinePreviewMutation.data.preview.order_type}
                        </div>
                      </div>
                      <div className="mt-2 text-xs text-slate-400">
                        Owner confirmation required exactly:
                        {" "}
                        {exchangeTestnetPipelinePreviewMutation.data.preview.confirmation_text}
                      </div>
                    </div>
                  ) : null}
                  <InlineStatus
                    error={
                      getErrorMessage(exchangeTestnetPipelinePreviewMutation.error) ??
                      getErrorMessage(exchangeTestnetPipelineSubmitMutation.error)
                    }
                    success={
                      exchangeTestnetPipelineSubmitMutation.data
                        ? `${exchangeTestnetPipelineSubmitMutation.data.order.client_order_id} submitted`
                        : undefined
                    }
                  />
                </Panel>
              ) : null}
              {user.role === "OWNER" ? (
                <Panel title="Manual Testnet Order">
                  <div className="grid gap-3 md:grid-cols-2">
                    <Field label="Symbol" value={testnetSymbol} onChange={setTestnetSymbol} />
                    <Field label="Side" value={testnetSide} onChange={setTestnetSide} />
                    <Field label="Type" value={testnetOrderType} onChange={setTestnetOrderType} />
                    <Field label="Quote Notional" value={testnetQuoteNotional} onChange={setTestnetQuoteNotional} />
                    <Field label="Quantity" value={testnetQuantity} onChange={setTestnetQuantity} />
                    <Field label="Limit Price" value={testnetLimitPrice} onChange={setTestnetLimitPrice} />
                    <Field label="Risk Decision ID" value={testnetRiskDecisionId} onChange={setTestnetRiskDecisionId} placeholder="approved UUID" />
                    <Field label='Type "TESTNET ORDER"' value={testnetConfirmation} onChange={setTestnetConfirmation} />
                  </div>
                  <div className="mt-4 flex gap-3">
                    <button
                      className="rounded-xl border border-amber-400/40 bg-amber-400/10 px-4 py-2 text-sm"
                      onClick={() => exchangeTestnetSubmitMutation.mutate()}
                      disabled={exchangeTestnetSubmitMutation.isPending || testnetConfirmation !== "TESTNET ORDER"}
                    >
                      Submit Testnet Order
                    </button>
                  </div>
                  <InlineStatus error={getErrorMessage(exchangeTestnetSubmitMutation.error) ?? getErrorMessage(exchangeTestnetCancelMutation.error)} />
                </Panel>
              ) : null}
            </section>
          )}
        </main>
      </div>
    </div>
  );
}

function HeaderStat({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "ok" | "danger" | "warning" | "neutral";
}) {
  return (
    <div className="rounded-xl border border-border bg-surface/70 p-3">
      <div className="text-[11px] uppercase tracking-[0.24em] text-muted">{label}</div>
      <div
        className={cn(
          "mt-2 text-base font-semibold",
          tone === "ok" && "text-emerald-300",
          tone === "danger" && "text-red-300",
          tone === "warning" && "text-amber-200",
          tone === "neutral" && "text-slate-100",
        )}
      >
        {value}
      </div>
    </div>
  );
}

function Panel({
  title,
  children,
  className,
}: {
  title: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("rounded-2xl border border-border bg-panel/90 p-4 shadow-panel", className)}>
      <div className="mb-3 text-sm font-semibold uppercase tracking-[0.2em] text-slate-200">
        {title}
      </div>
      {children}
    </div>
  );
}

function KeyValue({
  items,
  loading,
  error,
}: {
  items: Array<[string, string]>;
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading..." />;
  }

  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }

  return (
    <div className="space-y-2">
      {items.map(([label, value]) => (
        <div
          key={label}
          className="flex items-start justify-between gap-4 rounded-lg border border-border bg-surface/60 px-3 py-2"
        >
          <div className="text-xs uppercase tracking-[0.14em] text-muted">{label}</div>
          <div className="text-right text-sm text-slate-100">{value}</div>
        </div>
      ))}
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  placeholder,
  as,
  options,
  disabled,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  as?: "input" | "select";
  options?: string[];
  disabled?: boolean;
}) {
  const commonClassName =
    "mt-1 w-full rounded-lg border border-border bg-surface px-3 py-2 text-sm text-slate-100 outline-none transition focus:border-accent";

  return (
    <label className="block text-sm">
      <span className="text-xs uppercase tracking-[0.18em] text-muted">{label}</span>
      {as === "select" ? (
        <select
          className={commonClassName}
          value={value}
          disabled={disabled}
          onChange={(event) => onChange(event.target.value)}
        >
          {(options ?? []).map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      ) : (
        <input
          className={commonClassName}
          value={value}
          disabled={disabled}
          placeholder={placeholder}
          onChange={(event) => onChange(event.target.value)}
        />
      )}
    </label>
  );
}

function ActionButton({
  label,
  onClick,
  busy,
  tone = "ok",
  disabled,
}: {
  label: string;
  onClick: () => void;
  busy?: boolean;
  tone?: "ok" | "danger" | "warning";
  disabled?: boolean;
}) {
  return (
    <button
      className={cn(
        "rounded-lg border px-4 py-2 text-sm font-medium transition",
        tone === "ok" && "border-accent/40 bg-accent/10 text-emerald-200",
        tone === "danger" && "border-danger/40 bg-danger/10 text-red-200",
        tone === "warning" && "border-warning/40 bg-warning/10 text-amber-100",
        "disabled:cursor-not-allowed disabled:opacity-50",
      )}
      onClick={onClick}
      disabled={busy || disabled}
    >
      {busy ? "Working..." : label}
    </button>
  );
}

function InlineStatus({
  error,
  success,
}: {
  error?: string;
  success?: string;
}) {
  if (error && error !== "Unknown error") {
    return <div className="text-sm text-red-300">{error}</div>;
  }

  if (success) {
    return <div className="text-sm text-emerald-300">{success}</div>;
  }

  return null;
}

function OperatorReportFindingsTable({
  findings,
}: {
  findings: Array<{
    code: string;
    severity: string;
    title: string;
    detail: string;
    section: string;
  }>;
}) {
  if (!findings.length) {
    return <EmptyState label="No findings." />;
  }

  return (
    <div className="space-y-2">
      {findings.map((finding) => (
        <div
          key={finding.code}
          className="rounded-xl border border-border bg-surface/60 px-3 py-3"
        >
          <div className="flex items-center justify-between gap-3">
            <div className="text-sm font-semibold text-slate-100">{finding.title}</div>
            <div className="text-xs uppercase tracking-[0.18em] text-amber-200">
              {finding.severity}
            </div>
          </div>
          <div className="mt-1 text-xs uppercase tracking-[0.16em] text-muted">
            {finding.section}
          </div>
          <div className="mt-2 text-sm text-slate-300">{finding.detail}</div>
        </div>
      ))}
    </div>
  );
}

function OperatorReportSections({
  sections,
}: {
  sections: Array<{
    key: string;
    title: string;
    status: string;
    summary: string;
    highlights: Array<{ label: string; value: string }>;
  }>;
}) {
  if (!sections.length) {
    return <EmptyState label="No report sections." />;
  }

  return (
    <div className="space-y-3">
      {sections.map((section) => (
        <div
          key={section.key}
          className="rounded-xl border border-border bg-surface/60 px-3 py-3"
        >
          <div className="flex items-center justify-between gap-3">
            <div className="text-sm font-semibold text-slate-100">{section.title}</div>
            <div className="text-xs uppercase tracking-[0.18em] text-muted">
              {section.status}
            </div>
          </div>
          <div className="mt-2 text-sm text-slate-300">{section.summary}</div>
          <div className="mt-3 grid gap-2 md:grid-cols-2">
            {section.highlights.map((highlight) => (
              <div
                key={`${section.key}-${highlight.label}`}
                className="rounded-lg border border-border/70 bg-panel/70 px-3 py-2"
              >
                <div className="text-[11px] uppercase tracking-[0.16em] text-muted">
                  {highlight.label}
                </div>
                <div className="mt-1 text-sm text-slate-100">{highlight.value}</div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function OperatorReportList({
  reports,
  loading,
  error,
  selectedReportId,
  onSelect,
}: {
  reports: OperatorReportListItem[];
  loading?: boolean;
  error?: string;
  selectedReportId: string | null;
  onSelect: (reportId: string) => void;
}) {
  if (loading) {
    return <EmptyState label="Loading persisted reports..." />;
  }

  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }

  if (!reports.length) {
    return <EmptyState label="No persisted reports yet." />;
  }

  return (
    <div className="space-y-2">
      {reports.map((report) => (
        <button
          key={report.report_id}
          className={cn(
            "w-full rounded-xl border px-3 py-3 text-left transition",
            selectedReportId === report.report_id
              ? "border-accent/50 bg-accent/10"
              : "border-border bg-surface/60",
          )}
          type="button"
          onClick={() => onSelect(report.report_id)}
        >
          <div className="flex items-center justify-between gap-3">
            <div className="text-sm font-semibold text-slate-100">
              {shortenId(report.report_id)}
            </div>
            <div className="text-xs uppercase tracking-[0.18em] text-muted">
              {report.status}
            </div>
          </div>
          <div className="mt-1 text-xs text-slate-300">
            {formatDateTime(report.window_start)} {"->"} {formatDateTime(report.window_end)}
          </div>
        </button>
      ))}
    </div>
  );
}

function EmptyState({ label, tone = "neutral" }: { label: string; tone?: "neutral" | "danger" }) {
  return (
    <div
      className={cn(
        "rounded-xl border border-dashed px-3 py-6 text-center text-sm",
        tone === "neutral" && "border-border text-slate-300",
        tone === "danger" && "border-danger/40 text-red-300",
      )}
    >
      {label}
    </div>
  );
}

function SimpleList({ items }: { items: string[] }) {
  if (!items.length) {
    return <EmptyState label="No symbols." />;
  }

  return (
    <div className="space-y-2">
      {items.map((item) => (
        <div key={item} className="rounded-lg border border-border bg-surface/60 px-3 py-2 text-sm">
          {item}
        </div>
      ))}
    </div>
  );
}

function formatPercent(value?: string | null) {
  const formatted = formatNumber(value);
  return formatted === "-" ? "-" : `${formatted}%`;
}

function FeatureBucketTable({
  title,
  buckets,
}: {
  title: string;
  buckets: StrategySignalFeatureBucket[];
}) {
  return (
    <div className="overflow-x-auto rounded-xl border border-border bg-surface/40">
      <div className="px-3 py-2 text-xs font-medium text-slate-100">{title}</div>
      <table className="min-w-full text-left text-xs text-slate-300">
        <thead className="text-slate-100">
          <tr>
            <th className="px-3 py-2">Feature</th>
            <th className="px-3 py-2">Bucket</th>
            <th className="px-3 py-2">Samples</th>
            <th className="px-3 py-2">Win</th>
            <th className="px-3 py-2">Avg</th>
            <th className="px-3 py-2">Median</th>
            <th className="px-3 py-2">Best</th>
            <th className="px-3 py-2">Worst</th>
            <th className="px-3 py-2">Recommendation</th>
          </tr>
        </thead>
        <tbody>
          {buckets.length > 0 ? (
            buckets.map((bucket) => (
              <tr
                key={`${title}-${bucket.feature_name}-${bucket.bucket_label}`}
                className="border-t border-border"
              >
                <td className="px-3 py-2">{bucket.feature_name}</td>
                <td className="px-3 py-2">{bucket.bucket_label}</td>
                <td className="px-3 py-2">{bucket.sample_count}</td>
                <td className="px-3 py-2">{formatPercent(bucket.win_rate)}</td>
                <td className="px-3 py-2">{formatPercent(bucket.avg_net_pnl_pct)}</td>
                <td className="px-3 py-2">{formatPercent(bucket.median_net_pnl_pct)}</td>
                <td className="px-3 py-2">{formatPercent(bucket.best_net_pnl_pct)}</td>
                <td className="px-3 py-2">{formatPercent(bucket.worst_net_pnl_pct)}</td>
                <td className="px-3 py-2">{bucket.recommendation}</td>
              </tr>
            ))
          ) : (
            <tr className="border-t border-border">
              <td className="px-3 py-3 text-slate-500" colSpan={9}>
                No buckets.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function TestnetPromotionFunnelCards({
  summary,
  loading,
  error,
}: {
  summary?: TestnetPromotionFunnelSummary;
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading promotion funnel..." />;
  }

  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }

  if (!summary || summary.shadow_would_submit_count === 0) {
    return <EmptyState label="No promotion funnel data for this filter." />;
  }

  return (
    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
      {[
        ["Shadow Would Submit", String(summary.shadow_would_submit_count)],
        ["Previewed", String(summary.promotion_previewed_count)],
        ["Submitted", String(summary.promotion_submitted_count)],
        ["Acked", String(summary.acked_count)],
        ["Filled", String(summary.filled_count)],
        ["Reconciliation Required", String(summary.reconciliation_required_count)],
      ].map(([label, value]) => (
        <div
          key={label}
          className="rounded-xl border border-border bg-surface/60 px-3 py-4"
        >
          <div className="text-[11px] uppercase tracking-[0.18em] text-muted">{label}</div>
          <div className="mt-2 text-2xl font-semibold text-slate-100">{value}</div>
        </div>
      ))}
    </div>
  );
}

function TestnetPromotionRowsTable({
  rows,
  loading,
  error,
}: {
  rows: TestnetPromotionFunnelRow[];
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading promotion rows..." />;
  }

  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }

  if (!rows.length) {
    return <EmptyState label="No promotion rows found for this filter." />;
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-[0.18em] text-muted">
          <tr>
            <th className="px-3 py-2">Shadow Run</th>
            <th className="px-3 py-2">Promotion</th>
            <th className="px-3 py-2">Strategy</th>
            <th className="px-3 py-2">Symbol</th>
            <th className="px-3 py-2">Status</th>
            <th className="px-3 py-2">Client Order ID</th>
            <th className="px-3 py-2">Execution State</th>
            <th className="px-3 py-2">Created</th>
            <th className="px-3 py-2">Submitted</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.shadow_run_id} className="border-t border-border/60 text-slate-100">
              <td className="px-3 py-2 font-mono text-xs">{shortenId(row.shadow_run_id)}</td>
              <td className="px-3 py-2 font-mono text-xs">
                {row.promotion_id ? shortenId(row.promotion_id) : "-"}
              </td>
              <td className="px-3 py-2">{row.strategy_id}</td>
              <td className="px-3 py-2">{row.symbol}</td>
              <td className="px-3 py-2">{row.promotion_status ?? "-"}</td>
              <td className="px-3 py-2 font-mono text-xs">{row.client_order_id ?? "-"}</td>
              <td className="px-3 py-2">
                {row.execution_state ?? (row.linked_order_missing ? "MISSING_LINK" : "-")}
              </td>
              <td className="px-3 py-2">
                {formatDateTime(row.promotion_created_at ?? row.shadow_created_at)}
              </td>
              <td className="px-3 py-2">
                {row.submitted_at ? formatDateTime(row.submitted_at) : "-"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AnalyticsRankingsTable({ rankings }: { rankings: StrategyComparisonSummary[] }) {
  if (!rankings.length) {
    return <EmptyState label="No analytics rankings available." />;
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-[0.18em] text-muted">
          <tr>
            <th className="px-3 py-2">Strategy</th>
            <th className="px-3 py-2">Realized PnL</th>
            <th className="px-3 py-2">Would Submit</th>
            <th className="px-3 py-2">Risk Rejected</th>
            <th className="px-3 py-2">Backtest Avg %</th>
          </tr>
        </thead>
        <tbody>
          {rankings.map((ranking) => (
            <tr key={`${ranking.mode}-${ranking.strategy_id}`} className="border-t border-border/70">
              <td className="px-3 py-2 text-slate-100">{ranking.strategy_id}</td>
              <td className="px-3 py-2 text-copy/80">{formatNumber(ranking.realized_pnl)}</td>
              <td className="px-3 py-2 text-copy/80">{ranking.shadow_would_submit_count}</td>
              <td className="px-3 py-2 text-copy/80">{ranking.rejected_risk_decisions}</td>
              <td className="px-3 py-2 text-copy/80">
                {formatNumber(ranking.avg_backtest_pnl_pct)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SimpleStringTable({
  title,
  values,
}: {
  title: string;
  values: string[];
}) {
  return (
    <div className="rounded-2xl border border-border bg-surface/50 p-3">
      <div className="text-xs uppercase tracking-[0.2em] text-muted">{title}</div>
      <div className="mt-3 space-y-2 text-sm text-copy">
        {values.length ? (
          values.map((value) => (
            <div key={value} className="rounded-xl border border-border/70 px-3 py-2">
              {value}
            </div>
          ))
        ) : (
          <div className="rounded-xl border border-border/70 px-3 py-2 text-copy/60">
            None
          </div>
        )}
      </div>
    </div>
  );
}

function AnalyticsPnlBreakdownCard({
  breakdown,
  loading,
  error,
}: {
  breakdown?: StrategyPnlBreakdown;
  loading?: boolean;
  error?: string;
}) {
  if (error) {
    return <InlineStatus error={error} />;
  }
  if (loading) {
    return <div className="text-sm text-slate-300">Loading...</div>;
  }
  if (!breakdown) {
    return <EmptyState label="Insufficient analytics data." />;
  }

  return (
    <KeyValue
      items={[
        ["Mode", breakdown.mode],
        ["Positions Opened", String(breakdown.positions_opened)],
        ["Positions Closed", String(breakdown.positions_closed)],
        ["Realized PnL", formatNumber(breakdown.realized_pnl)],
        ["Unrealized PnL", formatNumber(breakdown.unrealized_pnl)],
        ["Win Rate", formatNumber(breakdown.win_rate)],
        ["Avg Win", formatNumber(breakdown.avg_win)],
        ["Avg Loss", formatNumber(breakdown.avg_loss)],
        ["Max Drawdown %", formatNumber(breakdown.max_drawdown_pct)],
      ]}
    />
  );
}

function FeedTable({
  feeds,
  loading,
  error,
}: {
  feeds: MarketFeedStatusRecord[];
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading feed status..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!feeds.length) {
    return <EmptyState label="No feed status rows." />;
  }

  return (
    <Table
      headers={["Symbol", "Status", "Freshness", "Last Event", "Reconnects"]}
      rows={feeds.map((feed) => [
        feed.symbol,
        badge(feed.status),
        badge(feed.freshness_status),
        formatRelativeAge(feed.last_event_at),
        String(feed.reconnect_count),
      ])}
    />
  );
}

function TicksTable({
  ticks,
}: {
  ticks: Array<{
    symbol: string;
    data?: { price: string; quantity: string; trade_time: string };
    error: unknown;
    isLoading: boolean;
  }>;
}) {
  return (
    <Table
      headers={["Symbol", "Price", "Qty", "Age"]}
      rows={ticks.map((tick) => [
        tick.symbol,
        tick.isLoading ? "Loading..." : formatNumber(tick.data?.price),
        tick.isLoading ? "Loading..." : formatNumber(tick.data?.quantity),
        tick.error ? getErrorMessage(tick.error) : formatRelativeAge(tick.data?.trade_time),
      ])}
    />
  );
}

function CandlesTable({
  candles,
}: {
  candles: Array<{
    open_time: string;
    close_time: string;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
    trade_count: number;
    is_closed: boolean;
  }>;
}) {
  if (!candles.length) {
    return <EmptyState label="No candles found." />;
  }

  return (
    <Table
      headers={["Open Time", "Close", "High", "Low", "Volume", "Trades", "State"]}
      rows={candles.map((candle) => [
        formatDateTime(candle.open_time),
        formatNumber(candle.close),
        formatNumber(candle.high),
        formatNumber(candle.low),
        formatNumber(candle.volume),
        String(candle.trade_count),
        candle.is_closed ? "closed" : "open",
      ])}
    />
  );
}

function CandleCoverageTable({
  coverage,
  loading,
  error,
}: {
  coverage: CandleCoverageSummary | null;
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading candle coverage..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!coverage) {
    return <EmptyState label="No coverage data." />;
  }

  return (
    <Table
      headers={["Interval", "Closed Candles"]}
      rows={coverage.intervals.map((entry) => [entry.interval, formatNumber(entry.candle_count)])}
    />
  );
}

function MarketDataQualityPanel({ report }: { report: MarketDataQualityReport | null }) {
  if (!report) {
    return <EmptyState label="No quality report yet." />;
  }

  const tone =
    report.status === "GOOD"
      ? "ok"
      : report.status === "DEGRADED"
        ? "warning"
        : report.status === "UNKNOWN"
          ? "neutral"
          : "danger";

  return (
    <div className="mt-4 space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <span
          className={cn(
            "rounded-md border px-2 py-1 text-xs font-semibold",
            tone === "ok" && "border-success/40 bg-success/10 text-emerald-200",
            tone === "warning" && "border-warning/40 bg-warning/10 text-amber-200",
            tone === "danger" && "border-danger/40 bg-danger/10 text-red-200",
            tone === "neutral" && "border-border bg-surface text-slate-200",
          )}
        >
          {report.status}
        </span>
        <span className="text-sm text-muted">
          {report.symbol} {report.interval} coverage {formatNumber(report.coverage_pct)}%
        </span>
      </div>
      <KeyValue
        items={[
          ["Expected", formatNumber(report.expected_candle_count)],
          ["Actual", formatNumber(report.actual_candle_count)],
          ["Closed", formatNumber(report.closed_candle_count)],
          ["Open", formatNumber(report.open_candle_count)],
          ["Missing", formatNumber(report.missing_candle_count)],
          ["Gaps", formatNumber(report.gap_count)],
          ["Largest Gap", `${formatNumber(report.largest_gap_seconds)}s`],
          ["First Candle", report.first_candle_time ? formatDateTime(report.first_candle_time) : "-"],
          ["Last Candle", report.last_candle_time ? formatDateTime(report.last_candle_time) : "-"],
        ]}
      />
      <div>
        <h4 className="mb-2 text-sm font-semibold text-foreground">Gaps</h4>
        {report.gaps.length ? (
          <Table
            headers={["Start", "End", "Missing", "Seconds"]}
            rows={report.gaps.map((gap) => [
              formatDateTime(gap.start_time),
              formatDateTime(gap.end_time),
              formatNumber(gap.missing_candle_count),
              formatNumber(gap.gap_seconds),
            ])}
          />
        ) : (
          <EmptyState label="No gaps returned." />
        )}
      </div>
      <div className="grid gap-3 md:grid-cols-2">
        <div>
          <h4 className="mb-2 text-sm font-semibold text-foreground">Findings</h4>
          {report.findings.length ? (
            <SimpleList
              items={report.findings.map(
                (finding) => `${finding.severity} ${finding.code}: ${finding.message}`,
              )}
            />
          ) : (
            <EmptyState label="No findings." />
          )}
        </div>
        <div>
          <h4 className="mb-2 text-sm font-semibold text-foreground">Recommendations</h4>
          {report.recommendations.length ? (
            <SimpleList
              items={report.recommendations.map(
                (recommendation) => `${recommendation.code}: ${recommendation.message}`,
              )}
            />
          ) : (
            <EmptyState label="No recommendations." />
          )}
        </div>
      </div>
    </div>
  );
}

function MarketDataRepairPanel({
  plan,
  run,
  recentRuns,
}: {
  plan: MarketDataRepairPlan | null;
  run: MarketDataRepairRunResult | null;
  recentRuns: MarketDataRepairRunResult[];
}) {
  return (
    <div className="mt-4 space-y-4">
      <div>
        <h4 className="mb-2 text-sm font-semibold text-foreground">Repair Plan</h4>
        {plan ? (
          <div className="space-y-3">
            <KeyValue
              items={[
                ["Status", plan.status],
                ["Before Quality", plan.initial_quality_status],
                ["Gaps", formatNumber(plan.gap_count)],
                ["Ranges", formatNumber(plan.repair_ranges.length)],
                ["Source", plan.estimated_source_interval ?? "-"],
                ["Reaggregate", plan.reaggregate_derived_intervals ? "yes" : "no"],
              ]}
            />
            <Table
              headers={["Source", "Start", "End", "Missing"]}
              rows={plan.repair_ranges.map((range) => [
                range.source_interval,
                formatDateTime(range.start_time),
                formatDateTime(range.end_time),
                formatNumber(range.missing_candle_count),
              ])}
            />
          </div>
        ) : (
          <EmptyState label="No repair plan yet." />
        )}
      </div>
      {run ? (
        <div>
          <h4 className="mb-2 text-sm font-semibold text-foreground">Repair Result</h4>
          <KeyValue
            items={[
              ["Status", run.status],
              ["Quality", `${run.before_quality_status} -> ${run.after_quality_status}`],
              ["Gaps", `${run.gap_count_before} -> ${run.gap_count_after}`],
              ["Inserted", formatNumber(run.inserted_candles)],
              ["Updated", formatNumber(run.updated_candles)],
              ["Skipped", formatNumber(run.skipped_candles)],
              ["Failed Ranges", formatNumber(run.failed_ranges)],
              ["Provider", run.selected_provider ?? "-"],
            ]}
          />
        </div>
      ) : null}
      <div>
        <h4 className="mb-2 text-sm font-semibold text-foreground">Recent Repair Runs</h4>
        {recentRuns.length ? (
          <Table
            headers={["Created", "Symbol", "Interval", "Status", "Quality", "Gaps", "Candles"]}
            rows={recentRuns.map((item) => [
              formatDateTime(item.created_at),
              item.plan.symbol,
              item.plan.interval,
              item.status,
              `${item.before_quality_status}->${item.after_quality_status}`,
              `${item.gap_count_before}->${item.gap_count_after}`,
              `${item.inserted_candles}/${item.updated_candles}/${item.skipped_candles}`,
            ])}
          />
        ) : (
          <EmptyState label="No repair runs yet." />
        )}
      </div>
    </div>
  );
}

function ResearchCoverageTable({
  coverage,
}: {
  coverage: ResearchDataCoverageResult | null;
}) {
  if (!coverage) {
    return <EmptyState label="No research coverage result yet." />;
  }

  return (
    <div className="space-y-3">
      <KeyValue
        items={[
          ["Window", `${formatDateTime(coverage.window_start)} -> ${formatDateTime(coverage.window_end)}`],
          ["Readiness", coverage.status],
          ["Required %", coverage.required_coverage_pct],
        ]}
      />
      <Table
        headers={["Interval", "Status", "Coverage %", "Expected", "Actual", "Missing Ranges"]}
        rows={coverage.per_interval.map((interval) => [
          interval.interval,
          interval.status,
          interval.coverage_pct,
          formatNumber(interval.expected_candles),
          formatNumber(interval.actual_candles),
          formatNumber(interval.missing_ranges.length),
        ])}
      />
    </div>
  );
}

function ResearchDatasetBuildsTable({
  builds,
  selectedBuildId,
  onSelect,
}: {
  builds: ResearchDatasetBuildResult[];
  selectedBuildId: string | null;
  onSelect: (buildId: string) => void;
}) {
  if (!builds.length) {
    return <EmptyState label="No dataset builds found." />;
  }

  return (
    <div className="space-y-2">
      {builds.map((build) => (
        <button
          key={build.build_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left",
            build.build_id === selectedBuildId
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60",
          )}
          onClick={() => onSelect(build.build_id)}
        >
          <div className="font-medium">
            {build.symbol} {build.requested_intervals.join(", ")}
          </div>
          <div className="mt-1 text-xs text-muted">
            {formatDateTime(build.created_at)} | {build.status} | {build.coverage_after.status}
          </div>
        </button>
      ))}
    </div>
  );
}

function ResearchDatasetBuildDetail({
  build,
  loading,
  error,
}: {
  build: ResearchDatasetBuildResult | null;
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading dataset build..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!build) {
    return <EmptyState label="No dataset build selected." />;
  }

  return (
    <div className="space-y-3">
      <KeyValue
        items={[
          ["Build ID", build.build_id],
          ["Status", build.status],
          ["Window", `${formatDateTime(build.start_time)} -> ${formatDateTime(build.end_time)}`],
          ["Intervals", build.requested_intervals.join(", ")],
          ["Readiness", build.coverage_after.status],
          ["Failure", build.failed_reason ?? "N/A"],
        ]}
      />
      <Table
        headers={["Step", "Status", "Started", "Completed"]}
        rows={build.steps.map((step) => [
          step.step,
          step.status,
          formatDateTime(step.started_at),
          step.completed_at ? formatDateTime(step.completed_at) : "-",
        ])}
      />
      <Table
        headers={["Interval", "Status", "Coverage %", "Expected", "Actual", "Missing Ranges"]}
        rows={build.coverage_after.per_interval.map((interval) => [
          interval.interval,
          interval.status,
          interval.coverage_pct,
          formatNumber(interval.expected_candles),
          formatNumber(interval.actual_candles),
          formatNumber(interval.missing_ranges.length),
        ])}
      />
    </div>
  );
}

function BackfillRunsTable({
  runs,
  selectedRunId,
  onSelect,
}: {
  runs: CandleBackfillResult[];
  selectedRunId: string | null;
  onSelect: (runId: string) => void;
}) {
  if (!runs.length) {
    return <EmptyState label="No backfill runs found." />;
  }

  return (
    <div className="space-y-2">
      {runs.map((run) => (
        <button
          key={run.run_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left",
            run.run_id === selectedRunId
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60",
          )}
          onClick={() => onSelect(run.run_id)}
        >
          <div className="flex flex-col gap-2 xl:flex-row xl:items-center xl:justify-between">
            <div>
              <div className="font-medium">
                {run.symbol} {run.interval}
              </div>
              <div className="mt-1 text-xs text-muted">
                {formatDateTime(run.created_at)} | {run.status}
              </div>
            </div>
            <div className="text-xs text-muted">
              {run.inserted_candles} inserted / {run.updated_candles} updated /{" "}
              {run.skipped_candles} skipped
            </div>
          </div>
          {run.failed_reason ? (
            <div className="mt-2 text-xs text-rose-200">{run.failed_reason}</div>
          ) : null}
        </button>
      ))}
    </div>
  );
}

function StrategiesTable({
  strategies,
  selectedStrategyId,
  onSelect,
  onToggle,
  onEvaluate,
  busyStrategyId,
}: {
  strategies: StrategyStatusView[];
  selectedStrategyId: string;
  onSelect: (strategyId: string) => void;
  onToggle: (strategy: StrategyStatusView, enabled: boolean) => void;
  onEvaluate: (strategy: StrategyStatusView) => void;
  busyStrategyId?: string;
}) {
  if (!strategies.length) {
    return <EmptyState label="No strategies found." />;
  }

  return (
    <div className="space-y-2">
      {strategies.map((strategy) => {
        const enabled = strategy.enabled;
        return (
          <div
            key={strategy.strategy_id}
            className={cn(
              "rounded-xl border p-3",
              strategy.strategy_id === selectedStrategyId
                ? "border-accent bg-accent/5"
                : "border-border bg-surface/60",
            )}
          >
            <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
              <button className="text-left" onClick={() => onSelect(strategy.strategy_id)}>
                <div className="font-medium">{strategy.strategy_id}</div>
                <div className="mt-1 text-xs text-muted">
                  {strategy.symbols.join(", ")} | {strategy.timeframe} | last eval{" "}
                  {formatRelativeAge(strategy.last_evaluated_at)}
                </div>
              </button>
              <div className="flex flex-wrap items-center gap-2">
                <span className="rounded-full border border-border px-2 py-1 text-xs">
                  {enabled ? "enabled" : "disabled"}
                </span>
                <ActionButton
                  label={enabled ? "Disable" : "Enable"}
                  onClick={() => onToggle(strategy, !enabled)}
                  tone={enabled ? "warning" : "ok"}
                  busy={busyStrategyId === strategy.strategy_id}
                />
                <ActionButton
                  label="Evaluate"
                  onClick={() => onEvaluate(strategy)}
                  busy={busyStrategyId === strategy.strategy_id}
                />
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function SignalsTable({
  signals,
}: {
  signals: Array<{
    id: string;
    strategy_id: string;
    symbol: string;
    side: string;
    confidence: string;
    reason: string;
    created_at: string;
  }>;
}) {
  if (!signals.length) {
    return <EmptyState label="No signals found." />;
  }

  return (
    <Table
      headers={["Signal ID", "Strategy", "Symbol", "Side", "Confidence", "Reason", "Created"]}
      rows={signals.map((signal) => [
        mono(shortenId(signal.id)),
        signal.strategy_id,
        signal.symbol,
        signal.side,
        signal.confidence,
        signal.reason,
        formatRelativeAge(signal.created_at),
      ])}
    />
  );
}

function OrdersTable({
  orders,
  onSelect,
  selectedId,
}: {
  orders: OrderRecord[];
  onSelect: (orderId: string) => void;
  selectedId?: string | null;
}) {
  if (!orders.length) {
    return <EmptyState label="No paper orders." />;
  }

  return (
    <div className="space-y-2">
      {orders.map((order) => (
        <button
          key={order.order_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left transition",
            selectedId === order.order_id
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60 hover:border-slate-500",
          )}
          onClick={() => onSelect(order.order_id)}
        >
          <div className="grid gap-2 md:grid-cols-6">
            <div className="font-mono text-xs">{shortenId(order.order_id)}</div>
            <div>{order.symbol}</div>
            <div>{order.side}</div>
            <div>{order.execution_state}</div>
            <div>{order.status}</div>
            <div className="font-mono text-xs">{shortenId(order.idempotency_key)}</div>
          </div>
        </button>
      ))}
    </div>
  );
}

function PaperPositionsTable({
  positions,
  onClose,
}: {
  positions: PaperPositionRecord[];
  onClose?: (position: PaperPositionRecord) => void;
}) {
  if (!positions.length) {
    return <EmptyState label="No paper positions." />;
  }

  return (
    <Table
      headers={[
        "Symbol",
        "Side",
        "Quantity",
        "Entry",
        "Mark",
        "Unrealized",
        "Realized",
        "Status",
        "Strategy",
        "Signal",
        "Action",
      ]}
      rows={positions.map((position) => [
        position.symbol,
        position.side,
        position.quantity,
        position.entry_price,
        position.mark_price ?? position.price_status,
        position.unrealized_pnl,
        position.realized_pnl,
        position.status,
        position.strategy_id ?? "N/A",
        position.signal_id ? shortenId(position.signal_id) : "N/A",
        position.status === "open" && onClose ? (
          <button
            className="rounded-md border border-amber-400/40 px-2 py-1 text-xs uppercase tracking-[0.2em] text-amber-100"
            onClick={() => onClose(position)}
            type="button"
          >
            Sim Close
          </button>
        ) : (
          "Closed"
        ),
      ])}
    />
  );
}

function BacktestRunsTable({
  runs,
  onSelect,
  selectedId,
}: {
  runs: BacktestResult[];
  onSelect: (runId: string) => void;
  selectedId?: string | null;
}) {
  if (!runs.length) {
    return <EmptyState label="No backtest runs." />;
  }

  return (
    <div className="space-y-2">
      {runs.map((run) => (
        <button
          key={run.run_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left transition",
            selectedId === run.run_id
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60 hover:border-slate-500",
          )}
          onClick={() => onSelect(run.run_id)}
        >
          <div className="grid gap-2 md:grid-cols-7">
            <div className="font-mono text-xs">{shortenId(run.run_id)}</div>
            <div>{run.strategy_id}</div>
            <div>{run.symbol}</div>
            <div>{run.status}</div>
            <div>PnL {run.pnl}</div>
            <div>
              signals {run.raw_signal_count ?? 0}/{run.executed_trade_count ?? run.trade_count}
            </div>
            <div>
              suppressed c{run.cooldown_suppressed_count ?? 0}/p
              {run.open_position_suppressed_count ?? 0}
            </div>
          </div>
        </button>
      ))}
    </div>
  );
}

function BacktestTradesTable({
  trades,
}: {
  trades: Array<{
    id: string;
    side: string;
    entry_time: string;
    exit_time: string | null;
    notional: string;
    realized_pnl: string;
    reason: string;
  }>;
}) {
  if (!trades.length) {
    return <EmptyState label="No backtest trades." />;
  }

  return (
    <Table
      headers={["Trade", "Side", "Entry", "Exit", "Notional", "PnL", "Reason"]}
      rows={trades.map((trade) => [
        mono(shortenId(trade.id)),
        trade.side,
        formatDateTime(trade.entry_time),
        formatDateTime(trade.exit_time),
        trade.notional,
        trade.realized_pnl,
        trade.reason,
      ])}
    />
  );
}

function BacktestEquityTable({
  equity,
}: {
  equity: Array<{
    id: string;
    timestamp: string;
    equity: string;
    drawdown_pct: string;
  }>;
}) {
  if (!equity.length) {
    return <EmptyState label="No equity points." />;
  }

  return (
    <Table
      headers={["Point", "Timestamp", "Equity", "Drawdown %"]}
      rows={equity.map((point) => [
        mono(shortenId(point.id)),
        formatDateTime(point.timestamp),
        point.equity,
        point.drawdown_pct,
      ])}
    />
  );
}

function ResearchBatchesTable({
  batches,
  selectedId,
  onSelect,
}: {
  batches: ResearchBatchResult[];
  selectedId: string | null;
  onSelect: (batchId: string) => void;
}) {
  if (!batches.length) {
    return <EmptyState label="No research batches found." />;
  }

  return (
    <div className="space-y-2">
      {batches.map((batch) => (
        <button
          key={batch.batch_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left",
            batch.batch_id === selectedId ? "border-accent bg-accent/5" : "border-border bg-surface/60",
          )}
          onClick={() => onSelect(batch.batch_id)}
        >
          <div className="flex items-center justify-between gap-3">
            <span className="font-mono text-xs">{shortenId(batch.batch_id)}</span>
            <span className="text-xs uppercase text-muted">{batch.status}</span>
          </div>
          <div className="mt-1 text-xs text-slate-300">
            experiments={batch.experiment_ids.length} wf={batch.walk_forward_run_ids.length} candidates={batch.created_candidate_ids.length}
          </div>
          <div className="mt-1 text-xs text-muted">{formatDateTime(batch.created_at)}</div>
        </button>
      ))}
    </div>
  );
}

function ResearchCampaignsTable({
  campaigns,
  selectedId,
  onSelect,
}: {
  campaigns: ResearchCampaignResult[];
  selectedId: string | null;
  onSelect: (campaignId: string) => void;
}) {
  if (!campaigns.length) {
    return <EmptyState label="No research campaigns found." />;
  }

  return (
    <div className="space-y-2">
      {campaigns.map((campaign) => (
        <button
          key={campaign.campaign_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left",
            campaign.campaign_id === selectedId
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60",
          )}
          onClick={() => onSelect(campaign.campaign_id)}
        >
          <div className="flex items-center justify-between gap-3">
            <span className="font-mono text-xs">{shortenId(campaign.campaign_id)}</span>
            <span className="text-xs uppercase text-muted">{campaign.status}</span>
          </div>
          <div className="mt-1 text-xs text-slate-300">
            planned={campaign.summary.total_batches_planned} actionable={campaign.summary.actionable_batches} overfit={campaign.summary.overfit_only_batches} weak={campaign.summary.weak_batches}
          </div>
          <div className="mt-1 text-xs text-muted">{formatDateTime(campaign.created_at)}</div>
        </button>
      ))}
    </div>
  );
}

function ResearchRegimeDatasetsTable({
  datasets,
  selectedId,
  onSelect,
}: {
  datasets: ResearchRegimeDatasetResult[];
  selectedId: string | null;
  onSelect: (datasetId: string) => void;
}) {
  if (!datasets.length) {
    return <EmptyState label="No regime datasets found." />;
  }

  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Recent Datasets
      </div>
      <div className="space-y-2">
        {datasets.map((dataset) => (
          <button
            key={dataset.dataset_id}
            className={cn(
              "w-full rounded-xl border p-3 text-left",
              dataset.dataset_id === selectedId
                ? "border-accent bg-accent/5"
                : "border-border bg-surface/60",
            )}
            onClick={() => onSelect(dataset.dataset_id)}
          >
            <div className="flex items-center justify-between gap-3">
              <span className="font-mono text-xs">{shortenId(dataset.dataset_id)}</span>
              <span className="text-xs uppercase text-muted">{dataset.status}</span>
            </div>
            <div className="mt-1 text-xs text-slate-300">
              {dataset.request.symbol} {dataset.request.timeframe} selected=
              {dataset.summary.selected_windows} missing={dataset.summary.missing_regimes.length}
            </div>
            <div className="mt-1 text-xs text-muted">{formatDateTime(dataset.created_at)}</div>
          </button>
        ))}
      </div>
    </div>
  );
}

function ResearchRegimeDiscoveriesTable({
  discoveries,
  selectedId,
  onSelect,
}: {
  discoveries: ResearchRegimeDiscoveryResult[];
  selectedId: string | null;
  onSelect: (discoveryId: string) => void;
}) {
  if (!discoveries.length) {
    return <EmptyState label="No regime discoveries found." />;
  }

  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Recent Discoveries
      </div>
      <div className="space-y-2">
        {discoveries.map((discovery) => (
          <button
            key={discovery.discovery_id}
            className={cn(
              "w-full rounded-xl border p-3 text-left",
              discovery.discovery_id === selectedId
                ? "border-accent bg-accent/5"
                : "border-border bg-surface/60",
            )}
            onClick={() => onSelect(discovery.discovery_id)}
          >
            <div className="flex items-center justify-between gap-3">
              <span className="font-mono text-xs">{shortenId(discovery.discovery_id)}</span>
              <span className="text-xs uppercase text-muted">{discovery.status}</span>
            </div>
            <div className="mt-1 text-xs text-slate-300">
              {discovery.symbol} {discovery.timeframe} selected=
              {discovery.summary.selected_window_count} missing={discovery.missing_regimes.length}
            </div>
            <div className="mt-1 text-xs text-muted">{formatDateTime(discovery.created_at)}</div>
          </button>
        ))}
      </div>
    </div>
  );
}

function ResearchRegimeCalibrationsTable({
  calibrations,
  selectedId,
  onSelect,
}: {
  calibrations: ResearchRegimeCalibrationResult[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  if (!calibrations.length) {
    return <EmptyState label="No calibrations yet." />;
  }
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Recent Calibrations
      </div>
      <div className="space-y-2">
        {calibrations.slice(0, 8).map((calibration) => (
          <button
            key={calibration.calibration_id}
            type="button"
            onClick={() => onSelect(calibration.calibration_id)}
            className={`w-full rounded-md border p-3 text-left text-sm ${
              selectedId === calibration.calibration_id
                ? "border-cyan-300/70 bg-cyan-400/10"
                : "border-slate-700 bg-slate-950/40"
            }`}
          >
            <div className="flex items-center justify-between gap-2">
              <span className="font-mono text-xs">{shortenId(calibration.calibration_id)}</span>
              <span className="text-xs text-muted">{calibration.status}</span>
            </div>
            <div className="mt-1 text-xs text-slate-300">
              {calibration.request.symbol} {calibration.request.timeframe}
            </div>
            <div className="mt-1 text-xs text-muted">{formatDateTime(calibration.created_at)}</div>
          </button>
        ))}
      </div>
    </div>
  );
}

function ResearchRegimeCalibrationTopConfigs({
  calibration,
  candidates,
}: {
  calibration: ResearchRegimeCalibrationResult | null;
  candidates?: ResearchRegimeCalibrationResult["candidates"];
}) {
  const rows = candidates ?? calibration?.candidates ?? [];
  if (!calibration && !rows.length) {
    return <EmptyState label="No calibration run yet." />;
  }
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Top Thresholds
      </div>
      <Table
        headers={["Candidate", "Score", "Diversity", "Dominant", "Counts"]}
        rows={rows.slice(0, 8).map((candidate) => [
          candidate.candidate_id,
          candidate.total_score,
          candidate.diversity_score,
          candidate.dominant_regime_share,
          Object.entries(candidate.counts_by_regime)
            .map(([regime, count]) => `${regime}=${count}`)
            .join(", ") || "-",
        ])}
      />
    </div>
  );
}

function ResearchRegimeCalibrationRecommended({
  calibration,
}: {
  calibration: ResearchRegimeCalibrationResult | null;
}) {
  const config = calibration?.recommended_config;
  const discoveryCommand = calibration
    ? `aegis research regime-discovery run --symbol ${calibration.request.symbol} --timeframe ${calibration.request.timeframe} --scan-start ${calibration.request.scan_start} --scan-end ${calibration.request.scan_end} --window-hours ${calibration.request.window_hours} --step-hours ${calibration.request.step_hours} --max-windows-per-regime 10 --calibration-id ${calibration.calibration_id}`
    : "-";
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Recommended Config
      </div>
      <KeyValue
        items={[
          ["Candidate", calibration?.recommended_candidate_id ?? "-"],
          ["Trend Return", config?.trend_return_threshold_pct ?? "-"],
          ["Range Return", config?.range_return_max_pct ?? "-"],
          ["Range Chop", config?.range_choppiness_min ?? "-"],
          ["High Vol", config?.high_volatility_threshold_pct ?? "-"],
          ["Low Vol", config?.low_volatility_threshold_pct ?? "-"],
          ["Missing", calibration?.missing_regimes.join(", ") || "-"],
          ["Discovery CLI", discoveryCommand],
        ]}
      />
      <SimpleList
        items={(calibration?.recommendations ?? []).map(
          (recommendation) =>
            `${recommendation.priority} ${recommendation.code}: ${recommendation.message}`,
        )}
      />
    </div>
  );
}

function ResearchRegimeCalibrationSamples({
  calibration,
}: {
  calibration: ResearchRegimeCalibrationResult | null;
}) {
  const samples = calibration?.candidates[0]?.explanation_samples ?? [];
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Explanation Samples
      </div>
      <SimpleList
        items={samples.slice(0, 5).map(
          (sample) =>
            `${sample.final_label} confidence=${sample.confidence} return=${sample.return_pct} vol=${sample.realized_volatility} range=${sample.avg_range_pct} slope=${sample.trend_slope} chop=${sample.choppiness_proxy} alternates=${sample.alternate_labels_considered.join(",") || "-"}`,
        )}
      />
    </div>
  );
}

function ResearchRegimeDiscoverySummaryTable({
  discovery,
}: {
  discovery: ResearchRegimeDiscoveryResult | null;
}) {
  const counts = discovery?.counts_by_regime ?? {};
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Discovery Summary
      </div>
      <Table
        headers={["Regime", "Windows"]}
        rows={["TREND_UP", "TREND_DOWN", "RANGE", "HIGH_VOLATILITY", "LOW_VOLATILITY"].map(
          (regime) => [regime, String(counts[regime as keyof typeof counts] ?? 0)],
        )}
      />
      <KeyValue
        items={[
          ["Calibration", discovery?.request.calibration_id ?? "-"],
          [
            "Config Source",
            discovery?.request.classifier_config
              ? discovery.request.calibration_id
                ? "inline override"
                : "inline"
              : discovery?.request.calibration_id
                ? "saved calibration"
                : "default",
          ],
        ]}
      />
      {discovery?.missing_regimes.length ? (
        <div className="mt-2 rounded-md border border-amber-400/40 bg-amber-400/10 p-2 text-xs text-amber-100">
          Missing: {discovery.missing_regimes.join(", ")}
        </div>
      ) : null}
    </div>
  );
}

function ResearchRegimeDiscoveryWindowsTable({
  windows,
}: {
  windows: ResearchRegimeDiscoveryCandidateWindow[];
}) {
  if (!windows.length) {
    return <EmptyState label="No discovery windows." />;
  }

  return (
    <Table
      headers={[
        "Regime",
        "Window",
        "Confidence",
        "Return %",
        "Vol",
        "Range",
        "Chop",
        "Quality",
        "Candles",
        "Explain",
      ]}
      rows={windows.slice(0, 80).map((window) => [
        window.regime_label,
        `${formatDateTime(window.start_time)} -> ${formatDateTime(window.end_time)}`,
        window.confidence,
        window.return_pct,
        window.realized_volatility,
        window.avg_range_pct,
        window.choppiness_proxy,
        window.data_quality_status,
        String(window.candle_count),
        `${window.explanation.final_label} c=${window.explanation.confidence} alt=${window.explanation.alternate_labels_considered.join(",") || "-"}`,
      ])}
    />
  );
}

function ResearchRegimeSummaryTable({ dataset }: { dataset: ResearchRegimeDatasetResult | null }) {
  const counts = dataset?.summary.regime_counts ?? {};
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Regime Summary
      </div>
      <Table
        headers={["Regime", "Windows"]}
        rows={["TREND_UP", "TREND_DOWN", "RANGE", "HIGH_VOLATILITY", "LOW_VOLATILITY"].map(
          (regime) => [regime, String(counts[regime as keyof typeof counts] ?? 0)],
        )}
      />
      {dataset?.summary.missing_regimes.length ? (
        <div className="mt-2 rounded-md border border-amber-400/40 bg-amber-400/10 p-2 text-xs text-amber-100">
          Missing: {dataset.summary.missing_regimes.join(", ")}
        </div>
      ) : null}
    </div>
  );
}

function ResearchRegimeWindowsTable({ windows }: { windows: ResearchRegimeWindow[] }) {
  if (!windows.length) {
    return <EmptyState label="No regime windows." />;
  }

  return (
    <Table
      headers={[
        "Regime",
        "Symbol",
        "Window",
        "Confidence",
        "Return %",
        "Vol",
        "Range",
        "Chop",
        "Quality",
        "Candles",
        "Explain",
      ]}
      rows={windows.slice(0, 80).map((window) => [
        window.regime_label,
        `${window.symbol} ${window.timeframe}`,
        `${formatDateTime(window.start_time)} -> ${formatDateTime(window.end_time)}`,
        window.confidence,
        window.return_pct,
        window.realized_volatility,
        window.avg_range_pct,
        window.choppiness_proxy,
        window.data_quality_status,
        String(window.candle_count),
        `${window.explanation.final_label} c=${window.explanation.confidence} alt=${window.explanation.alternate_labels_considered.join(",") || "-"}`,
      ])}
    />
  );
}

function ResearchCampaignBatchTable({ batches }: { batches: ResearchCampaignBatchResult[] }) {
  if (!batches.length) {
    return <EmptyState label="No campaign batches." />;
  }

  return (
    <Table
      headers={["Plan", "Strategy", "Symbol", "TF", "Regime", "Window", "Triage", "Candidates", "Error"]}
      rows={batches.map((batch) => [
        String(batch.plan.plan_index),
        batch.plan.strategy_id,
        batch.plan.symbol,
        batch.plan.timeframe,
        batch.plan.regime_label ?? "-",
        `${formatDateTime(batch.plan.start_time)} -> ${formatDateTime(batch.plan.end_time)}`,
        batch.triage_status,
        String(batch.candidates_created),
        batch.error ?? "-",
      ])}
    />
  );
}

function ResearchCampaignFailureAttributionCard({
  attribution,
  loading,
  error,
}: {
  attribution: ResearchCampaignFailureAttribution | null;
  loading: boolean;
  error?: string;
}) {
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Failure Attribution
      </div>
      <KeyValue
        items={[
          ["Top Reasons", attribution?.overall_failure_reasons.join(", ") || "-"],
          ["Findings", String(attribution?.findings.length ?? 0)],
          ["Recommendations", String(attribution?.recommendations.length ?? 0)],
          ["Generated", attribution ? formatDateTime(attribution.generated_at) : "-"],
        ]}
        loading={loading}
        error={error}
      />
      <SimpleList
        items={(attribution?.recommendations ?? []).map(
          (recommendation) =>
            `${recommendation.priority} ${recommendation.code}: ${recommendation.message}`,
        )}
      />
    </div>
  );
}

function ResearchCampaignRegimeSummaryTable({
  attribution,
}: {
  attribution: ResearchCampaignFailureAttribution | null;
}) {
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Regime Summary
      </div>
      <Table
        headers={["Regime", "Windows", "Candidates", "Return %", "Volatility", "Reasons"]}
        rows={(attribution?.regime_summary ?? []).map((regime) => [
          regime.label,
          String(regime.window_count),
          String(regime.candidate_count),
          regime.avg_return_pct,
          regime.avg_realized_volatility,
          regime.failure_reasons.join(", ") || "-",
        ])}
      />
    </div>
  );
}

function ResearchCampaignRegimeLeaderboardCard({
  leaderboard,
  loading,
  error,
}: {
  leaderboard: ResearchRegimeStrategyLeaderboard | null;
  loading: boolean;
  error?: string;
}) {
  const best = leaderboard?.overall_best ?? leaderboard?.overall_rankings[0];
  const promising = leaderboard?.overall_promising ?? null;
  const leastBad = leaderboard?.overall_least_bad ?? null;
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Regime Leaderboard
      </div>
      <KeyValue
        items={[
          [
            "Overall Promising",
            promising
              ? `${promising.strategy_id} ${promising.symbol} ${promising.timeframe}`
              : "No promising strategy found",
          ],
          [
            "Promising Status",
            promising ? `${promising.status} score=${promising.robustness_score}` : "-",
          ],
          [
            "Least-bad Strategy",
            leastBad
              ? `${leastBad.strategy_id} ${leastBad.symbol} ${leastBad.timeframe}`
              : "-",
          ],
          [
            "Least-bad Status",
            leastBad ? `${leastBad.status} score=${leastBad.robustness_score}` : "-",
          ],
          ["Overall Best", best ? `${best.strategy_id} ${best.symbol} ${best.timeframe}` : "-"],
          ["Generated", leaderboard ? formatDateTime(leaderboard.generated_at) : "-"],
        ]}
        loading={loading}
        error={error}
      />
      <SimpleList
        items={(leaderboard?.recommendations ?? []).map(
          (recommendation) =>
            `${recommendation.priority} ${recommendation.code}: ${recommendation.message}`,
        )}
      />
    </div>
  );
}

function ResearchCampaignRegimeLeaderboardTable({
  leaderboard,
}: {
  leaderboard: ResearchRegimeStrategyLeaderboard | null;
}) {
  const rows =
    leaderboard?.per_regime.flatMap((cell) =>
      cell.rankings.slice(0, 5).map((ranking) => [
        cell.regime_label,
        String(ranking.rank),
        ranking.strategy_id,
        `${ranking.symbol} ${ranking.timeframe}`,
        regimeStatusBadge(ranking.status),
        ranking.median_pnl_pct,
        String(ranking.robustness_score),
        `${ranking.actionable_count}/${ranking.weak_count}/${ranking.overfit_count}`,
      ]),
    ) ?? [];
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Per-Regime Ranking
      </div>
      <Table
        headers={["Regime", "Rank", "Strategy", "Market", "Status", "Median PnL %", "Score", "A/W/O"]}
        rows={rows}
      />
    </div>
  );
}

function ResearchCampaignOverallLeaderboardTable({
  leaderboard,
}: {
  leaderboard: ResearchRegimeStrategyLeaderboard | null;
}) {
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Overall Ranking
      </div>
      <Table
        headers={["Rank", "Strategy", "Market", "Status", "Median PnL %", "Avg PnL %", "Candidates", "Score"]}
        rows={(leaderboard?.overall_rankings ?? []).slice(0, 10).map((ranking) => [
          String(ranking.rank),
          ranking.strategy_id,
          `${ranking.symbol} ${ranking.timeframe}`,
          regimeStatusBadge(ranking.status),
          ranking.median_pnl_pct,
          ranking.avg_pnl_pct,
          String(ranking.candidate_count),
          String(ranking.robustness_score),
        ])}
      />
    </div>
  );
}

function ResearchCampaignFailureReasonsTable({
  attribution,
}: {
  attribution: ResearchCampaignFailureAttribution | null;
}) {
  return (
    <div>
      <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">
        Candidate Failure Reasons
      </div>
      <Table
        headers={["Strategy", "Symbol", "TF", "Regime", "PnL %", "Trades", "WF", "Reasons"]}
        rows={(attribution?.candidate_failure_table ?? []).map((row) => [
          row.strategy_id,
          row.symbol,
          row.timeframe,
          row.regime_label,
          row.pnl_pct ?? "-",
          row.trade_count == null ? "-" : String(row.trade_count),
          row.walk_forward_status ?? "-",
          row.failure_reasons.join(", ") || "-",
        ])}
      />
    </div>
  );
}

function ResearchHypothesesPanel({
  hypotheses,
  selectedHypothesis,
  priorityFilter,
  statusFilter,
  loading,
  error,
  generateBusy,
  decideBusy,
  canMutate,
  onPriorityFilter,
  onStatusFilter,
  onSelect,
  onGenerate,
  onDecide,
}: {
  hypotheses: ResearchHypothesis[];
  selectedHypothesis: ResearchHypothesis | null;
  priorityFilter: ResearchHypothesisPriority | "ALL";
  statusFilter: ResearchHypothesisStatus | "ALL";
  loading?: boolean;
  error?: string;
  generateBusy?: boolean;
  decideBusy?: boolean;
  canMutate: boolean;
  onPriorityFilter: (value: ResearchHypothesisPriority | "ALL") => void;
  onStatusFilter: (value: ResearchHypothesisStatus | "ALL") => void;
  onSelect: (id: string | null) => void;
  onGenerate: () => void;
  onDecide: (id: string, decision: ResearchHypothesisStatus) => void;
}) {
  return (
    <Panel title="Research Hypotheses">
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <select
          className="rounded-md border border-border bg-panel px-2 py-2 text-sm"
          value={priorityFilter}
          onChange={(event) =>
            onPriorityFilter(event.target.value as ResearchHypothesisPriority | "ALL")
          }
        >
          {["ALL", "HIGH", "MEDIUM", "LOW"].map((value) => (
            <option key={value} value={value}>
              {value}
            </option>
          ))}
        </select>
        <select
          className="rounded-md border border-border bg-panel px-2 py-2 text-sm"
          value={statusFilter}
          onChange={(event) =>
            onStatusFilter(event.target.value as ResearchHypothesisStatus | "ALL")
          }
        >
          {["ALL", "PROPOSED", "ACCEPTED_FOR_EXPERIMENT", "REJECTED", "ARCHIVED"].map(
            (value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ),
          )}
        </select>
        <ActionButton
          label="Generate"
          onClick={onGenerate}
          busy={generateBusy}
          disabled={!canMutate}
        />
      </div>
      {loading ? <EmptyState label="Loading hypotheses." /> : null}
      {error ? <div className="mb-3 text-sm text-danger">{error}</div> : null}
      <div className="grid gap-4 xl:grid-cols-2">
        <Table
          headers={["Priority", "Status", "Source", "Evidence"]}
          rows={hypotheses.map((hypothesis) => [
            hypothesis.priority,
            hypothesis.status,
            hypothesis.source_type,
            <button
              className="text-left text-accent"
              key={hypothesis.id ?? hypothesis.evidence.summary}
              onClick={() => onSelect(hypothesis.id)}
              type="button"
            >
              {hypothesis.evidence.summary}
            </button>,
          ])}
        />
        <div className="rounded-lg border border-border p-3">
          {selectedHypothesis ? (
            <div className="space-y-3 text-sm">
              <KeyValue
                items={[
                  ["ID", selectedHypothesis.id ?? "-"],
                  ["Strategy", selectedHypothesis.strategy_id ?? "-"],
                  ["Symbol", selectedHypothesis.symbol ?? "-"],
                  ["Timeframe", selectedHypothesis.timeframe ?? "-"],
                  ["Regime", selectedHypothesis.regime ?? "-"],
                  ["Action", selectedHypothesis.proposed_action],
                  ["Expected Effect", selectedHypothesis.expected_effect],
                  ["Risk", selectedHypothesis.risk],
                ]}
              />
              <pre className="max-h-64 overflow-auto rounded-md bg-background p-3 text-xs">
                {JSON.stringify(selectedHypothesis.proposed_experiment_config, null, 2)}
              </pre>
              <div className="flex flex-wrap gap-2">
                <ActionButton
                  label="Accept"
                  onClick={() =>
                    selectedHypothesis.id &&
                    onDecide(selectedHypothesis.id, "ACCEPTED_FOR_EXPERIMENT")
                  }
                  busy={decideBusy}
                  disabled={!canMutate || !selectedHypothesis.id}
                />
                <ActionButton
                  label="Reject"
                  tone="warning"
                  onClick={() =>
                    selectedHypothesis.id && onDecide(selectedHypothesis.id, "REJECTED")
                  }
                  busy={decideBusy}
                  disabled={!canMutate || !selectedHypothesis.id}
                />
                <ActionButton
                  label="Archive"
                  tone="danger"
                  onClick={() =>
                    selectedHypothesis.id && onDecide(selectedHypothesis.id, "ARCHIVED")
                  }
                  busy={decideBusy}
                  disabled={!canMutate || !selectedHypothesis.id}
                />
              </div>
            </div>
          ) : (
            <EmptyState label="No hypothesis selected." />
          )}
        </div>
      </div>
    </Panel>
  );
}

function ResearchCampaignTopCandidatesTable({
  candidates,
  onSelectCandidate,
}: {
  candidates: ResearchBatchCandidateSummary[];
  onSelectCandidate: (candidateId: string) => void;
}) {
  if (!candidates.length) {
    return <EmptyState label="No top candidates." />;
  }

  return (
    <Table
      headers={["Strategy", "Symbol", "TF", "Score", "PnL %", "WF", "Candidate"]}
      rows={candidates.map((candidate) => [
        candidate.strategy_id,
        candidate.symbol,
        candidate.timeframe,
        candidate.score,
        candidate.pnl_pct,
        candidate.robustness_status ?? "-",
        candidate.candidate_id ? (
          <button
            className="font-mono text-accent"
            onClick={() => onSelectCandidate(candidate.candidate_id ?? "")}
            type="button"
          >
            {shortenId(candidate.candidate_id)}
          </button>
        ) : (
          "-"
        ),
      ])}
    />
  );
}

function ResearchBatchDetail({
  batch,
  triage,
  onSelectCandidate,
}: {
  batch: ResearchBatchResult | null;
  triage: ResearchBatchTriage | null;
  onSelectCandidate: (candidateId: string) => void;
}) {
  if (!batch) {
    return <EmptyState label="No research batch selected." />;
  }

  return (
    <div className="space-y-4">
      <KeyValue
        items={[
          ["Batch", shortenId(batch.batch_id)],
          ["Status", batch.status],
          ["Provider", batch.provider_health_summary?.status ?? "-"],
          ["Quality Before", batch.quality_before?.status ?? "-"],
          ["Quality After", batch.quality_after?.status ?? "-"],
          ["Experiments", String(batch.experiment_ids.length)],
          ["Walk-forward", String(batch.walk_forward_run_ids.length)],
          ["Candidates", String(batch.created_candidate_ids.length)],
        ]}
      />
      {triage ? (
        <div className="rounded-xl border border-border bg-surface/50 p-3">
          <div className="flex items-center justify-between gap-3">
            <div className="text-sm font-semibold">Triage</div>
            <span className="rounded-md border border-border px-2 py-1 text-xs uppercase text-slate-200">
              {triage.status}
            </span>
          </div>
          <div className="mt-3 grid gap-2 text-xs text-muted md:grid-cols-4">
            <span>actionable={triage.actionable_count}</span>
            <span>weak={triage.weak_count}</span>
            <span>overfit={triage.overfit_count}</span>
            <span>generated={formatDateTime(triage.generated_at)}</span>
          </div>
          {triage.recommendations.length ? (
            <div className="mt-3 space-y-1 text-sm text-slate-300">
              {triage.recommendations.map((item) => (
                <div key={item.code}>
                  <span className="font-semibold">{item.priority}</span> {item.message}
                </div>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}
      <Table
        headers={["Step", "Status", "Started", "Completed", "Error"]}
        rows={batch.steps.map((step) => [
          step.step_name,
          step.status,
          formatDateTime(step.started_at),
          step.completed_at ? formatDateTime(step.completed_at) : "-",
          step.error ?? "-",
        ])}
      />
      <Table
        headers={["Timeframe", "Run", "WF", "Candidate", "Score", "PnL %", "Robustness"]}
        rows={batch.top_candidates.map((candidate) => [
          candidate.timeframe,
          shortenId(candidate.experiment_run_id),
          candidate.walk_forward_run_id ? shortenId(candidate.walk_forward_run_id) : "-",
          candidate.candidate_id ? shortenId(candidate.candidate_id) : "-",
          candidate.score,
          candidate.pnl_pct,
          candidate.robustness_status ?? "-",
        ])}
      />
      {triage?.candidates.length ? (
        <Table
          headers={["Rank", "Status", "Candidate", "Run", "Score", "PnL %", "WF", "Recommendation"]}
          rows={triage.candidates.map((candidate) => [
            mono(String(candidate.rank)),
            candidate.triage_status,
            <button
              key={candidate.candidate_id}
              className="font-mono text-accent underline-offset-2 hover:underline"
              onClick={() => onSelectCandidate(candidate.candidate_id)}
            >
              {shortenId(candidate.candidate_id)}
            </button>,
            shortenId(candidate.experiment_run_id),
            candidate.experiment_score,
            candidate.experiment_pnl_pct,
            candidate.walk_forward_status ?? "-",
            candidate.walk_forward_recommendation ?? "-",
          ])}
        />
      ) : null}
      {batch.recommendations.length ? (
        <div className="space-y-2">
          {batch.recommendations.map((item) => (
            <div key={item.code} className="rounded-xl border border-border bg-surface/50 p-3 text-sm">
              <span className="font-semibold">{item.severity}</span> {item.message}
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

const DEFAULT_ROBUSTNESS_MATRIX_FORM: StrategyRobustnessMatrixRequest = {
  strategy_ids: ["trend_filter_momentum_v1", "trend_filter_momentum_v2", "range_reversion_v1"],
  symbols: ["BTCUSDT", "ETHUSDT"],
  timeframes: ["5m", "15m"],
  windows: [],
  start_time: "2026-05-01T00:00:00Z",
  end_time: "2026-05-04T00:00:00Z",
  window_hours: 24,
  step_hours: 24,
  config_json_by_strategy: null,
  experiment_run_id: null,
  initial_capital: "1000000",
  fee_bps: "10",
  slippage_bps: "5",
  holding_candles: 10,
  min_trades_per_cell: 5,
  min_profitable_window_ratio: "0.5",
};

function ResearchRobustnessMatrixPanel() {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<StrategyRobustnessMatrixRequest>(
    DEFAULT_ROBUSTNESS_MATRIX_FORM,
  );
  const [lastRun, setLastRun] = useState<StrategyRobustnessMatrixAcceptedResponse | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  const runsQuery = useQuery({
    queryKey: ["strategy-robustness-matrices"],
    queryFn: () => api.getStrategyRobustnessMatrices(10),
  });
  const cellsQuery = useQuery({
    queryKey: ["strategy-robustness-matrix-cells", selectedRunId],
    queryFn: () => api.getStrategyRobustnessMatrixCells(selectedRunId ?? ""),
    enabled: Boolean(selectedRunId),
  });
  const runMutation = useMutation({
    mutationFn: api.runStrategyRobustnessMatrix,
    onSuccess: async (response) => {
      setLastRun(response);
      setSelectedRunId(response.matrix.run_id);
      await queryClient.invalidateQueries({ queryKey: ["strategy-robustness-matrices"] });
    },
  });

  const activeMatrix = lastRun?.matrix ?? runsQuery.data?.matrices[0] ?? null;
  const activeCells = lastRun?.cells ?? cellsQuery.data?.cells ?? [];
  const regimeBreakdown = buildRegimeBreakdown(activeCells);

  return (
    <Panel className="xl:col-span-12" title="Research Robustness Matrix">
      <form
        className="grid gap-3 md:grid-cols-4"
        onSubmit={(event) => {
          event.preventDefault();
          runMutation.mutate(form);
        }}
      >
        <Field
          label="Strategies"
          value={form.strategy_ids.join(",")}
          onChange={(value) => setForm((current) => ({ ...current, strategy_ids: parseStringList(value) }))}
        />
        <Field
          label="Symbols"
          value={form.symbols.join(",")}
          onChange={(value) => setForm((current) => ({ ...current, symbols: parseStringList(value) }))}
        />
        <Field
          label="Timeframes"
          value={form.timeframes.join(",")}
          onChange={(value) => setForm((current) => ({ ...current, timeframes: parseStringList(value) }))}
        />
        <Field
          label="Start"
          value={form.start_time ?? ""}
          onChange={(value) => setForm((current) => ({ ...current, start_time: value || null }))}
        />
        <Field
          label="End"
          value={form.end_time ?? ""}
          onChange={(value) => setForm((current) => ({ ...current, end_time: value || null }))}
        />
        <Field
          label="Window Hours"
          value={String(form.window_hours ?? "")}
          onChange={(value) => setForm((current) => ({ ...current, window_hours: Number(value) || null }))}
        />
        <Field
          label="Step Hours"
          value={String(form.step_hours ?? "")}
          onChange={(value) => setForm((current) => ({ ...current, step_hours: Number(value) || null }))}
        />
        <Field
          label="Initial Capital"
          value={form.initial_capital}
          onChange={(value) => setForm((current) => ({ ...current, initial_capital: value }))}
        />
        <Field
          label="Fee Bps"
          value={form.fee_bps}
          onChange={(value) => setForm((current) => ({ ...current, fee_bps: value }))}
        />
        <Field
          label="Slippage Bps"
          value={form.slippage_bps}
          onChange={(value) => setForm((current) => ({ ...current, slippage_bps: value }))}
        />
        <Field
          label="Holding Candles"
          value={String(form.holding_candles ?? "")}
          onChange={(value) =>
            setForm((current) => ({ ...current, holding_candles: value ? Number(value) : null }))
          }
        />
        <Field
          label="Min Trades"
          value={String(form.min_trades_per_cell ?? 5)}
          onChange={(value) =>
            setForm((current) => ({ ...current, min_trades_per_cell: Number(value) || 0 }))
          }
        />
        <button
          className="rounded-xl border border-accent bg-accent/15 px-4 py-2 text-sm font-medium text-white transition hover:bg-accent/25 disabled:opacity-50 md:self-end"
          type="submit"
          disabled={runMutation.isPending}
        >
          {runMutation.isPending ? "Running..." : "Run Matrix"}
        </button>
      </form>
      <InlineStatus error={getErrorMessage(runMutation.error ?? runsQuery.error)} />

      <div className="mt-5 grid gap-4 xl:grid-cols-3">
        <div className="rounded-lg border border-border/70 p-3">
          <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">Recent Runs</div>
          <div className="space-y-2 text-sm">
            {(runsQuery.data?.matrices ?? []).map((run) => (
              <button
                key={run.run_id}
                className="block w-full rounded-md border border-border/70 px-3 py-2 text-left hover:border-accent"
                type="button"
                onClick={() => {
                  setSelectedRunId(run.run_id);
                  setLastRun(null);
                }}
              >
                <div className="font-mono text-xs text-slate-300">{run.run_id}</div>
                <div className="mt-1 text-xs text-muted">
                  {run.status} cells={run.cell_count}
                </div>
              </button>
            ))}
          </div>
        </div>
        <div className="xl:col-span-2">
          {activeMatrix ? <StrategyRobustnessRankingTable matrix={activeMatrix} /> : null}
        </div>
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-3">
        <div className="rounded-lg border border-border/70 p-3">
          <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">Regime Breakdown</div>
          <div className="space-y-2 text-sm">
            {regimeBreakdown.map((row) => (
              <div key={row.regime} className="flex items-center justify-between gap-3">
                <span>{row.regime}</span>
                <span className="text-muted">
                  cells={row.count} avg={row.avgPnl}
                </span>
              </div>
            ))}
          </div>
        </div>
        <div className="rounded-lg border border-border/70 p-3 xl:col-span-2">
          <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted">Recommendations</div>
          <div className="space-y-2 text-sm text-slate-300">
            {(activeMatrix?.strategy_rankings ?? []).flatMap((summary) =>
              summary.recommendations.map((item) => (
                <div key={`${summary.strategy_id}-${item.code}`} className="rounded-md bg-surface/60 px-3 py-2">
                  {summary.strategy_id}: {item.priority} {item.code} - {item.message}
                </div>
              )),
            )}
          </div>
        </div>
      </div>

      <StrategyRobustnessCellsTable cells={activeCells} />
    </Panel>
  );
}

function StrategyRobustnessRankingTable({ matrix }: { matrix: StrategyRobustnessMatrixResult }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-border/70">
      <table className="min-w-full divide-y divide-border/70 text-sm">
        <thead className="text-xs uppercase tracking-[0.18em] text-muted">
          <tr>
            {["Strategy", "Status", "Score", "Avg PnL", "Median", "Worst", "Best Symbol", "Best Regime"].map((header) => (
              <th key={header} className="px-3 py-2 text-left">{header}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border/50">
          {matrix.strategy_rankings.map((summary) => (
            <tr key={summary.strategy_id}>
              <td className="px-3 py-2 font-mono text-xs">{summary.strategy_id}</td>
              <td className="px-3 py-2">{summary.status}</td>
              <td className="px-3 py-2">{summary.robustness_score}</td>
              <td className="px-3 py-2">{summary.avg_pnl_pct}</td>
              <td className="px-3 py-2">{summary.median_pnl_pct}</td>
              <td className="px-3 py-2">{summary.worst_window_pnl_pct}</td>
              <td className="px-3 py-2">{summary.best_symbol ?? "-"}</td>
              <td className="px-3 py-2">{summary.best_regime ?? "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function StrategyRobustnessCellsTable({ cells }: { cells: StrategyRobustnessMatrixCell[] }) {
  return (
    <div className="mt-4 overflow-x-auto rounded-lg border border-border/70">
      <table className="min-w-full divide-y divide-border/70 text-sm">
        <thead className="text-xs uppercase tracking-[0.18em] text-muted">
          <tr>
            {["Strategy", "Symbol", "Tf", "Window", "Status", "Regime", "Quality", "PnL", "Trades", "Signals", "Drawdown"].map((header) => (
              <th key={header} className="px-3 py-2 text-left">{header}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border/50">
          {cells.slice(0, 80).map((cell) => (
            <tr key={cell.id}>
              <td className="px-3 py-2 font-mono text-xs">{cell.strategy_id}</td>
              <td className="px-3 py-2">{cell.symbol}</td>
              <td className="px-3 py-2">{cell.timeframe}</td>
              <td className="px-3 py-2 text-xs">{cell.window_start.slice(0, 10)}..{cell.window_end.slice(0, 10)}</td>
              <td className="px-3 py-2">{cell.status}</td>
              <td className="px-3 py-2">{cell.regime_label}</td>
              <td className="px-3 py-2">{cell.data_quality_status}</td>
              <td className="px-3 py-2">{cell.pnl_pct}</td>
              <td className="px-3 py-2">{cell.trade_count}</td>
              <td className="px-3 py-2">{cell.raw_signal_count}</td>
              <td className="px-3 py-2">{cell.max_drawdown_pct}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function buildRegimeBreakdown(cells: StrategyRobustnessMatrixCell[]) {
  const groups = new Map<string, { count: number; total: number }>();
  for (const cell of cells) {
    const current = groups.get(cell.regime_label) ?? { count: 0, total: 0 };
    current.count += 1;
    current.total += Number(cell.pnl_pct);
    groups.set(cell.regime_label, current);
  }
  return Array.from(groups.entries())
    .map(([regime, value]) => ({
      regime,
      count: value.count,
      avgPnl: value.count > 0 ? (value.total / value.count).toFixed(5) : "0",
    }))
    .sort((left, right) => left.regime.localeCompare(right.regime));
}

function StrategyExperimentsTable({
  experiments,
  onSelect,
  selectedId,
}: {
  experiments: StrategyExperimentResult[];
  onSelect: (experimentId: string) => void;
  selectedId?: string | null;
}) {
  if (!experiments.length) {
    return <EmptyState label="No strategy experiments." />;
  }

  return (
    <div className="space-y-2">
      {experiments.map((experiment) => (
        <button
          key={experiment.experiment_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left transition",
            selectedId === experiment.experiment_id
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60 hover:border-slate-500",
          )}
          onClick={() => onSelect(experiment.experiment_id)}
        >
          <div className="grid gap-2 md:grid-cols-4">
            <div className="font-mono text-xs">{shortenId(experiment.experiment_id)}</div>
            <div>{experiment.strategy_id}</div>
            <div>{experiment.symbol}</div>
            <div>best {formatNumber(experiment.best_run?.score ?? "0")}</div>
          </div>
        </button>
      ))}
    </div>
  );
}

function StrategyExperimentRunsTable({ runs }: { runs: StrategyExperimentRun[] }) {
  if (!runs.length) {
    return <EmptyState label="No strategy experiment runs." />;
  }

  return (
    <Table
      headers={[
        "Rank",
        "Lookback",
        "Holding",
        "PnL %",
        "Drawdown %",
        "Signals",
        "Trades",
        "Suppressed",
        "Win Rate",
        "Drag %",
        "Score",
        "Warnings",
      ]}
      rows={runs.map((run) => [
        mono(String(run.rank)),
        String(run.candidate.lookback_candles),
        run.candidate.holding_candles ? String(run.candidate.holding_candles) : "-",
        run.pnl_pct,
        run.max_drawdown_pct,
        `${run.raw_signal_count ?? 0}`,
        String(run.executed_trade_count ?? run.trade_count),
        `c${run.cooldown_suppressed_count ?? 0}/p${run.open_position_suppressed_count ?? 0}`,
        run.win_rate,
        run.fee_slippage_drag_pct,
        run.score,
        run.warnings.length ? run.warnings.join(", ") : "-",
      ])}
    />
  );
}

function StrategyWalkForwardRunsTable({
  runs,
  onSelect,
  selectedId,
}: {
  runs: StrategyWalkForwardResult[];
  onSelect: (walkForwardId: string) => void;
  selectedId?: string | null;
}) {
  if (!runs.length) {
    return <EmptyState label="No walk-forward runs." />;
  }

  return (
    <div className="space-y-2">
      {runs.map((run) => (
        <button
          key={run.walk_forward_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left transition",
            selectedId === run.walk_forward_id
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60 hover:border-slate-500",
          )}
          onClick={() => onSelect(run.walk_forward_id)}
        >
          <div className="grid gap-2 md:grid-cols-4">
            <div className="font-mono text-xs">{shortenId(run.walk_forward_id)}</div>
            <div>{run.timeframe}</div>
            <div>{run.robustness_status}</div>
            <div>score {formatNumber(run.consistency_score ?? run.robustness_score)}</div>
            <div>
              {run.profitable_test_windows}/{run.losing_test_windows} windows
            </div>
          </div>
        </button>
      ))}
    </div>
  );
}

function StrategyWalkForwardWindowsTable({
  windows,
}: {
  windows: StrategyWalkForwardWindowResult[];
}) {
  if (!windows.length) {
    return <EmptyState label="No walk-forward windows." />;
  }

  return (
    <Table
      headers={[
        "Window",
        "Train",
        "Test",
        "Status",
        "PnL %",
        "Drawdown %",
        "Signals",
        "Trades",
        "Suppressed",
        "Skip Reason",
      ]}
      rows={windows.map((window) => [
        mono(String(window.window.window_index)),
        `${formatDateTime(window.window.train_start)} -> ${formatDateTime(window.window.train_end)}`,
        `${formatDateTime(window.window.test_start)} -> ${formatDateTime(window.window.test_end)}`,
        window.status,
        window.pnl_pct,
        window.max_drawdown_pct,
        `${window.raw_signal_count ?? 0}`,
        String(window.executed_trade_count ?? window.trade_count),
        `c${window.cooldown_suppressed_count ?? 0}/p${window.open_position_suppressed_count ?? 0}`,
        window.skip_reason ?? "-",
      ])}
    />
  );
}

function RiskDecisionsTable({
  decisions,
  onSelect,
  selectedId,
  loading,
  error,
}: {
  decisions: RiskDecisionRecord[];
  onSelect: (decisionId: string) => void;
  selectedId?: string | null;
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading risk decisions..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!decisions.length) {
    return <EmptyState label="No risk decisions found for this filter." />;
  }

  return (
    <div className="space-y-2">
      {decisions.map((decision) => (
        <button
          key={decision.id}
          className={cn(
            "w-full rounded-xl border p-3 text-left transition",
            selectedId === decision.id
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60 hover:border-slate-500",
          )}
          onClick={() => onSelect(decision.id)}
        >
          <div className="grid gap-2 md:grid-cols-6">
            <div className="font-mono text-xs">{shortenId(decision.id)}</div>
            <div>{decision.symbol ?? "N/A"}</div>
            <div>{decision.strategy_id ?? "N/A"}</div>
            <div>{decision.decision}</div>
            <div>{decision.risk_score ?? "N/A"}</div>
            <div>{formatRelativeAge(decision.created_at)}</div>
          </div>
          <div className="mt-2 text-xs text-slate-300">
            {decision.reasons.join(", ") || "No reasons recorded."}
          </div>
        </button>
      ))}
    </div>
  );
}

function RiskRejectionSummary({
  decision,
  loading,
  error,
  onOpen,
}: {
  decision: RiskDecisionRecord | null;
  loading?: boolean;
  error?: string;
  onOpen: () => void;
}) {
  if (loading) {
    return <EmptyState label="Loading latest rejection..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!decision) {
    return <EmptyState label="No recent risk rejection." />;
  }

  return (
    <div className="space-y-3">
      <KeyValue
        items={[
          ["Decision ID", decision.id],
          ["Symbol", decision.symbol ?? "N/A"],
          ["Strategy", decision.strategy_id ?? "N/A"],
          ["Risk Score", decision.risk_score ?? "N/A"],
          ["Created", formatDateTime(decision.created_at)],
          ["Reasons", decision.reasons.join(", ") || "N/A"],
        ]}
      />
      <ActionButton label="Open Risk Screen" onClick={onOpen} tone="warning" />
    </div>
  );
}

function EventsTable({
  events,
  loading,
  error,
}: {
  events: SystemEventRecord[];
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading events..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!events.length) {
    return <EmptyState label="No events available." />;
  }

  return (
    <Table
      headers={["Type", "Source", "Occurred", "Correlation", "Payload"]}
      rows={events.map((event) => [
        event.event_type,
        event.source,
        formatRelativeAge(event.occurred_at),
        mono(shortenId(event.correlation_id)),
        trimJson(event.payload),
      ])}
    />
  );
}

function Table({
  headers,
  rows,
}: {
  headers: string[];
  rows: Array<Array<React.ReactNode>>;
}) {
  return (
    <div className="overflow-auto rounded-xl border border-border">
      <table className="min-w-full divide-y divide-border text-sm">
        <thead className="bg-surface/90">
          <tr>
            {headers.map((header) => (
              <th
                key={header}
                className="px-3 py-2 text-left text-[11px] uppercase tracking-[0.2em] text-muted"
              >
                {header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border bg-surface/40">
          {rows.map((row, index) => (
            <tr key={`row-${index}`}>
              {row.map((cell, cellIndex) => (
                <td key={`cell-${index}-${cellIndex}`} className="px-3 py-2 align-top text-slate-100">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function summarizeFeedState(feeds: MarketFeedStatusRecord[]) {
  if (!feeds.length) {
    return { label: "UNKNOWN", tone: "danger" as const };
  }

  if (feeds.some((feed) => feed.status === "error" || feed.status === "disconnected")) {
    return { label: "DEGRADED", tone: "danger" as const };
  }
  if (feeds.some((feed) => feed.freshness_status === "stale" || feed.status === "stale")) {
    return { label: "STALE", tone: "warning" as const };
  }
  return { label: "OK", tone: "ok" as const };
}

function computeDataAge(feeds: MarketFeedStatusRecord[], tickTimes: Array<string | undefined>) {
  const timestamps = [
    ...feeds.map((feed) => feed.last_event_at).filter(Boolean),
    ...tickTimes.filter(Boolean),
  ] as string[];
  if (!timestamps.length) {
    return "unknown";
  }
  return formatRelativeAge(
    timestamps.sort((left, right) => new Date(right).getTime() - new Date(left).getTime())[0],
  );
}

function trimJson(payload: unknown) {
  const text = JSON.stringify(payload ?? {});
  if (text.length <= 72) {
    return text;
  }
  return `${text.slice(0, 72)}...`;
}

function badge(value: string) {
  return toTitleCase(value);
}

function regimeStatusBadge(status: ResearchRegimeStrategyStatus) {
  const tone =
    status === "ROBUST" || status === "PROMISING"
      ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-200"
      : status === "OVERFIT" || status === "NEGATIVE"
        ? "border-rose-400/40 bg-rose-500/10 text-rose-200"
        : status === "WEAK"
          ? "border-amber-400/40 bg-amber-500/10 text-amber-200"
          : "border-slate-500/40 bg-slate-500/10 text-slate-200";
  return (
    <span className={cn("inline-flex rounded-md border px-2 py-0.5 text-xs font-medium", tone)}>
      {status}
    </span>
  );
}

function mono(value: string) {
  return <span className="font-mono text-xs">{value}</span>;
}
