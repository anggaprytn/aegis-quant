use aegis_core::{
    CandleAggregationResult, CandleAggregationStatusRow, CrossAssetAcceptShadowPreview,
    CrossAssetCandidateGatePreviewResult, CrossAssetCandidateQualification,
    CrossAssetObservationHealthReport, CrossAssetRelativeStrengthV1Dossier,
    CrossAssetResearchCandidateDossier, CrossAssetResearchResult, CrossAssetRobustnessMatrixResult,
    CrossAssetShadowObservationPreviewResult, CrossAssetShadowObservationRunResult,
    MarketCandleCoverageSummary, MarketDataQualityReport, MarketDataRepairPlan,
    MarketDataRepairRunResult, ResearchBatchResult, ResearchBatchStep, ResearchBatchTriage,
    ResearchCampaignBatchResult, ResearchCampaignFailureAttribution, ResearchCampaignResult,
    ResearchCampaignSummary, ResearchCandidateAcceptForShadowApplyResult,
    ResearchCandidateAcceptForShadowPreviewResult, ResearchCandidateDecisionRejection,
    ResearchCandidateObservationHistoryItem, ResearchCandidateObservationSummaryView,
    ResearchCandidateQualificationChange, ResearchCandidateQualificationEvaluation,
    ResearchCandidateQualificationHistory, ResearchCandidateQualificationResult,
    ResearchCandidateQualificationTrend, ResearchCandidateReview, ResearchCandidateReviewResult,
    ResearchCandidateShadowObserveOnceResult, ResearchCandidateShadowPerformance,
    ResearchCandidateShadowPromotionPreview, ResearchCandidateShadowPromotionResult,
    ResearchCandidateShadowRunLink, ResearchCandidateTestnetReviewDossier,
    ResearchCandidateWalkForwardEvidence, ResearchCandidateWatchlistEntry, ResearchExperimentPlan,
    ResearchExperimentPlanRunResult, ResearchHypothesis, ResearchHypothesisGenerationResult,
    ResearchRegimeCalibrationCandidateResult, ResearchRegimeCalibrationResult,
    ResearchRegimeDatasetResult, ResearchRegimeDiscoveryCandidateWindow,
    ResearchRegimeDiscoveryResult, ResearchRegimeStrategyLeaderboard, ResearchRegimeWindow,
    ResearchShadowPnlAttributionResult, ResearchStaleRunRecoveryResult,
    StrategyRobustnessMatrixCell, StrategyRobustnessMatrixResult, User,
};
use aegis_core::{
    ExchangeTestnetPipelinePreview, PaperTradingPipelineResult, TestnetShadowPromotionPreview,
    TestnetShadowPromotionResult, TestnetShadowRunResult,
};
use colored::Colorize;
use serde::Serialize;

use crate::api::{
    BacktestResult, BacktestRunAcceptedResponse, CandleBackfillRunResponse,
    CandleBackfillRunsResponse, CompressionBreakoutRefinementResponse,
    ExchangePrivateStreamEventRecord, ExchangePrivateStreamListenKeyResponse,
    ExchangePrivateStreamStatusResponse, ExchangeReconciliationMismatchRecord,
    ExchangeReconciliationResult, ExchangeReconciliationRunRecord, ExchangeTestnetBalancesResponse,
    ExchangeTestnetOrderResponse, ExchangeTestnetPipelineSubmitResponse,
    ExchangeTestnetRepairActionRecord, ExchangeTestnetRepairResponse,
    ExchangeTestnetStatusResponse, ExchangeTestnetSymbolsResponse, ExecutionReadinessResponse,
    ExecutionReadinessSnapshotsResponse, FeedStatusResponse, HealthResponse,
    MarketDataRepairRunsResponse, OperatorReportResponse, OperatorReportsListResponse, OrderRecord,
    PaperAccountResponse, PaperClosePositionResponse, PaperEquityResponse, PaperPnlResponse,
    PaperPositionRecord, PaperPositionsResponse, PaperTradeJournalResponse, RecentEventsResponse,
    RiskActionResponse, RiskConfigAuditResponse, RiskConfigResponse, RiskConfigValidationResponse,
    RiskConfigVersionsResponse, RiskDecisionsResponse, RiskStatusResponse, StatusResponse,
    StrategyConfigAuditResponse, StrategyConfigValidationResponse, StrategyConfigVersionsResponse,
    StrategyDecisionBreakdownResponse, StrategyDiagnosticsResponse, StrategyDryRunResponse,
    StrategyExitAttributionResponse, StrategyListResponse, StrategyOpportunityAnalysisResponse,
    StrategyPerformanceRankingsResponse, StrategyPerformanceSummaryResponse,
    StrategySignalFeatureAttributionResponse, StrategyStatusResponse,
    TestnetPromotionFunnelOutcomesResponse, TestnetPromotionFunnelRowsResponse,
    TestnetPromotionFunnelSummaryResponse, TestnetShadowPromotionsResponse,
    TestnetShadowRunnerControlResponse, TestnetShadowRunnerStatusResponse,
    TestnetShadowRunsResponse,
};

pub fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn compact_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

pub fn print_cross_asset_research_run(run: &CrossAssetResearchResult) {
    println!("Cross-asset run: {}", run.run_id);
    println!(
        "Strategy: {}  Status: {}  Recommendation: {}",
        run.strategy_kind.as_str(),
        run.portfolio_status.as_str(),
        run.recommendation.as_str()
    );
    println!(
        "Trades: {}  PnL: {}%  DD: {}%  Win rate: {}%",
        run.total_trades, run.compounded_pnl_pct, run.max_drawdown_pct, run.win_rate
    );
    println!(
        "Worst window: {}%  Median window: {}%  Max symbol concentration: {}%",
        run.worst_window_pnl_pct, run.median_window_pnl_pct, run.max_symbol_concentration_pct
    );
    println!("Symbols: {:?}", run.symbol_distribution);
}

pub fn print_cross_asset_robustness_matrix(matrix: &CrossAssetRobustnessMatrixResult) {
    println!("Cross-asset robustness matrix: {}", matrix.run_id);
    println!(
        "Status: {}  Recommendation: {}",
        matrix.status.as_str(),
        matrix.recommendation.as_str()
    );
    println!(
        "Configs: {}/{} evaluated  Skipped: {}  Cells: {}",
        matrix.evaluated_config_count,
        matrix.full_config_count,
        matrix.skipped_config_count,
        matrix.cell_count
    );
    for ranking in matrix.rankings.iter().take(5) {
        println!(
            "#{:02} config={} score={} status={} trades={} pnl={} dd={} conc={} btc={}",
            ranking.rank,
            ranking.config_index,
            ranking.robustness_score,
            ranking.status.as_str(),
            ranking.total_trades,
            ranking.combined_pnl_pct,
            ranking.max_drawdown_pct,
            ranking.max_symbol_concentration_pct,
            ranking.btc_trade_count
        );
    }
}

pub fn print_cross_asset_relative_strength_v1_dossier(
    dossier: &CrossAssetRelativeStrengthV1Dossier,
) {
    println!("Cross-asset relative strength v1 dossier");
    println!("Strategy: {}", dossier.strategy_identity.strategy_id);
    println!(
        "Scope: {}  paper={} testnet={} live={}",
        dossier.strategy_identity.scope,
        dossier.strategy_identity.paper_executable,
        dossier.strategy_identity.testnet_executable,
        dossier.strategy_identity.live_executable
    );
    println!(
        "Latest run: {}  Latest matrix: {}",
        dossier
            .latest_supporting_run_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        dossier
            .latest_matrix_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "Robustness: {}  Recommendation: {}",
        dossier
            .robustness_status
            .map(|status| status.as_str())
            .unwrap_or("unknown"),
        dossier
            .robustness_recommendation
            .map(|recommendation| recommendation.as_str())
            .unwrap_or("unknown")
    );
    println!(
        "Candidate gate preview: {}  Recommended action: {}",
        dossier
            .candidate_gate_preview_status
            .map(|status| status.as_str())
            .unwrap_or("unknown"),
        dossier
            .candidate_gate_preview_recommended_action
            .as_deref()
            .unwrap_or("unknown")
    );
    if let Some(run) = &dossier.evidence_combined {
        println!(
            "Combined: trades={} pnl={} dd={} status={}",
            run.total_trades,
            run.compounded_pnl_pct,
            run.max_drawdown_pct,
            run.portfolio_status.as_str()
        );
    }
    println!("Blockers: {:?}", dossier.blockers);
    println!("Gate blockers: {:?}", dossier.candidate_gate_blockers);
    println!("Gate warnings: {:?}", dossier.candidate_gate_warnings);
    println!(
        "Candidate creation policy: {}  Next action: {}",
        dossier
            .candidate_creation_policy_status
            .map(|status| status.as_str())
            .unwrap_or("unknown"),
        dossier
            .candidate_creation_policy_recommended_next_action
            .as_deref()
            .unwrap_or("unknown")
    );
    for summary in &dossier.candidate_creation_policy_summaries {
        println!(
            "  {} status={} hard_blockers={} review_warnings={} recommended={}",
            summary.strictness.as_str(),
            summary.status.as_str(),
            summary.hard_blocker_count,
            summary.review_warning_count,
            summary.recommended_action
        );
    }
    if let Some(candidate_id) = dossier.research_candidate_id {
        println!(
            "Research candidate: {} status={} scope={} execution_authority={}",
            candidate_id,
            dossier
                .research_candidate_status
                .as_deref()
                .unwrap_or("unknown"),
            dossier.candidate_scope.as_deref().unwrap_or("unknown"),
            dossier.execution_authority
        );
        println!(
            "Ready: shadow={} paper={} testnet={} live={} next={}",
            dossier.shadow_ready,
            dossier.paper_ready,
            dossier.testnet_ready,
            dossier.live_ready,
            dossier
                .next_action_after_candidate_creation
                .as_deref()
                .unwrap_or("unknown")
        );
    }
    println!("Allowed next actions: {:?}", dossier.allowed_next_actions);
    println!("Forbidden actions: {:?}", dossier.forbidden_actions);
}

pub fn print_cross_asset_candidate_gate_preview(preview: &CrossAssetCandidateGatePreviewResult) {
    println!("Cross-asset candidate gate preview");
    println!(
        "Package: {}  Status: {}  Recommended action: {}",
        preview.package_id,
        preview.status.as_str(),
        preview.recommended_action
    );
    println!(
        "Run: {}  Matrix: {}  Preview only: {}",
        preview
            .run_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        preview
            .matrix_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        preview.preview_only
    );
    println!("Preview only, no candidate created.");
    println!("Blockers:");
    if preview.blockers.is_empty() {
        println!("  none");
    }
    for blocker in &preview.blockers {
        println!(
            "  {} observed={} threshold={} - {}",
            blocker.code,
            blocker.observed_value.as_deref().unwrap_or("n/a"),
            blocker.threshold.as_deref().unwrap_or("n/a"),
            blocker.message
        );
    }
    println!("Warnings:");
    if preview.warnings.is_empty() {
        println!("  none");
    }
    for warning in &preview.warnings {
        println!(
            "  {} observed={} threshold={} - {}",
            warning.code,
            warning.observed_value.as_deref().unwrap_or("n/a"),
            warning.threshold.as_deref().unwrap_or("n/a"),
            warning.message
        );
    }
    println!("Forbidden actions: {:?}", preview.forbidden_actions);
}

pub fn print_cross_asset_candidate_creation_policy_preview(
    preview: &aegis_core::CrossAssetCandidateCreationPolicyPreviewResult,
) {
    println!("Cross-asset candidate creation policy preview");
    println!(
        "Package: {}  Strictness: {}  Status: {}",
        preview.package_id,
        preview.strictness.as_str(),
        preview.status.as_str()
    );
    println!(
        "Run: {}  Matrix: {}",
        preview
            .run_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        preview
            .matrix_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    println!("Recommended action: {}", preview.recommended_action);
    println!("Preview only, no candidate created.");
    let hard_blocks = preview
        .hard_requirements
        .iter()
        .filter(|requirement| {
            requirement.status == aegis_core::CrossAssetCandidateGateCheckStatus::Block
        })
        .count();
    let review_warnings = preview
        .review_requirements
        .iter()
        .filter(|requirement| {
            requirement.status != aegis_core::CrossAssetCandidateGateCheckStatus::Pass
        })
        .count();
    println!(
        "Hard pass/fail: {}/{} blockers={}",
        preview.hard_requirements.len().saturating_sub(hard_blocks),
        preview.hard_requirements.len(),
        hard_blocks
    );
    println!("Review warnings: {}", review_warnings);
    for requirement in &preview.review_requirements {
        if requirement.status != aegis_core::CrossAssetCandidateGateCheckStatus::Pass {
            println!(
                "  {} {} observed={} threshold={} - {}",
                requirement.status.as_str(),
                requirement.code,
                requirement.observed_value.as_deref().unwrap_or("n/a"),
                requirement.threshold.as_deref().unwrap_or("n/a"),
                requirement.message
            );
        }
    }
    println!("Forbidden actions: {:?}", preview.forbidden_actions);
}

pub fn print_cross_asset_candidate_create_preview(
    preview: &aegis_core::CrossAssetResearchCandidateManualCreatePreview,
) {
    println!("Cross-asset research candidate manual create preview");
    println!(
        "Package: {}  Strictness: {}  Policy: {}",
        preview.package_id,
        preview.strictness.as_str(),
        preview.policy_status.as_str()
    );
    println!(
        "Run: {}  Matrix: {}",
        preview
            .run_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        preview
            .matrix_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "Would create: {}  Proposed status: {}  Scope: {}  Execution authority: {}",
        preview.candidate_would_be_created,
        preview.proposed_candidate_status,
        preview.candidate_scope,
        preview.execution_authority
    );
    if let Some(candidate_id) = preview.existing_candidate_id {
        println!("Existing candidate: {candidate_id}");
    }
    println!(
        "Exact confirmation required: {}",
        preview.exact_confirmation_required
    );
    println!("Blockers: {:?}", preview.blockers);
    println!("Warnings: {:?}", preview.warnings);
    println!("Forbidden actions: {:?}", preview.forbidden_actions);
}

pub fn print_cross_asset_candidate_create_result(
    result: &aegis_core::CrossAssetResearchCandidateManualCreateResult,
) {
    println!("Cross-asset research candidate manual create result");
    println!(
        "Candidate: {}  Status: {}  Created: {}  Idempotent: {}",
        result.candidate_id, result.candidate_status, result.created, result.idempotent
    );
    println!(
        "Package: {}  Strictness: {}  Policy: {}",
        result.package_id,
        result.strictness.as_str(),
        result.policy_status.as_str()
    );
    println!(
        "Scope: {}  Implementation research only: {}  Execution authority: {}",
        result.candidate_scope, result.implementation_research_only, result.execution_authority
    );
    println!("Warnings acknowledged: {:?}", result.warnings_acknowledged);
    println!("Forbidden actions: {:?}", result.forbidden_actions);
}

pub fn print_cross_asset_research_candidate_dossier(dossier: &CrossAssetResearchCandidateDossier) {
    println!("Cross-asset research candidate dossier");
    println!(
        "Candidate: {} status={} package={} scope={} execution_authority={}",
        dossier.candidate_id,
        dossier.candidate_status.as_str(),
        dossier.package_id,
        dossier.scope,
        dossier.execution_authority
    );
    println!(
        "Source run: {}  Source matrix: {}",
        dossier
            .source_run_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string()),
        dossier
            .source_matrix_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "Lifecycle: {}  policy_strictness={}",
        dossier.lifecycle_status.as_str(),
        dossier.policy_strictness_used.as_str()
    );
    println!(
        "Ready: candidate_review={} shadow={} paper={} testnet={} live={}",
        dossier.readiness.candidate_review_ready,
        dossier.readiness.shadow_ready,
        dossier.readiness.paper_ready,
        dossier.readiness.testnet_ready,
        dossier.readiness.live_ready
    );
    if let Some(metrics) = &dossier.fixed_run_metrics {
        println!(
            "Fixed run: status={} portfolio={} trades={} pnl={} dd={} recommendation={}",
            metrics.status.as_str(),
            metrics.portfolio_status.as_str(),
            metrics.total_trades,
            metrics.compounded_pnl_pct,
            metrics.max_drawdown_pct,
            metrics.recommendation.as_str()
        );
    }
    if let Some(metrics) = &dossier.matrix_metrics {
        println!(
            "Matrix: status={} recommendation={} cells={} top_trades={}",
            metrics.status.as_str(),
            metrics.recommendation.as_str(),
            metrics.cell_count,
            metrics
                .top_total_trades
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        );
    }
    if let Some(shadow) = &dossier.shadow_performance {
        println!(
            "Shadow observations: total={} independent={} would_select={} no_signal={} skipped={} latest={} latest_selected={}",
            shadow.total_shadow_observations,
            shadow.independent_shadow_observations,
            shadow.would_select_count,
            shadow.no_signal_count,
            shadow.skipped_count,
            shadow
                .latest_evaluated_candle_time
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "none".to_string()),
            shadow.latest_selected_symbol.as_deref().unwrap_or("none")
        );
    }
    println!("Blockers:");
    for blocker in &dossier.blockers {
        println!("  - {}: {}", blocker.code, blocker.message);
    }
    println!("Warnings:");
    for warning in &dossier.warnings {
        println!("  - {}: {}", warning.code, warning.message);
    }
    println!("Allowed next actions: {:?}", dossier.allowed_next_actions);
    println!("Forbidden actions: {:?}", dossier.forbidden_actions);
    println!("This is read-only and does not submit orders.");
}

pub fn print_cross_asset_candidate_qualification(qualification: &CrossAssetCandidateQualification) {
    println!("Cross-asset candidate qualification");
    println!(
        "Candidate: {} package={} status={}",
        qualification.candidate_id,
        qualification.package_id,
        qualification.status.as_str()
    );
    println!(
        "Ready: candidate_review={} shadow={} paper={} testnet={} live={}",
        qualification.readiness.candidate_review_ready,
        qualification.readiness.shadow_ready,
        qualification.readiness.paper_ready,
        qualification.readiness.testnet_ready,
        qualification.readiness.live_ready
    );
    if let Some(shadow) = &qualification.shadow_performance {
        println!(
            "Shadow observations: total={} independent={} would_select={} no_signal={} skipped={} status={}",
            shadow.total_shadow_observations,
            shadow.independent_shadow_observations,
            shadow.would_select_count,
            shadow.no_signal_count,
            shadow.skipped_count,
            shadow.shadow_status.as_deref().unwrap_or("NONE")
        );
    }
    println!("Blockers:");
    for blocker in &qualification.blockers {
        println!("  - {}: {}", blocker.code, blocker.message);
    }
    println!("Warnings:");
    for warning in &qualification.warnings {
        println!("  - {}: {}", warning.code, warning.message);
    }
    println!("Forbidden actions: {:?}", qualification.forbidden_actions);
}

pub fn print_cross_asset_observation_health(report: &CrossAssetObservationHealthReport) {
    println!("{}", report.status.as_str());
    println!(
        "Candidate: {} package={} status={}",
        report.candidate_id,
        report.package_id,
        report.candidate_status.as_str()
    );
    println!(
        "Candles: latest_evaluated={} latest_aligned_4h={}",
        report
            .latest_evaluated_candle
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "none".to_string()),
        report
            .latest_aligned_4h_candle
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "Observations: total={} independent={} WOULD_SELECT={} NO_SIGNAL={} skipped={}",
        report.observation_count,
        report.independent_observation_count,
        report.would_select_count,
        report.no_signal_count,
        report.skipped_count
    );
    println!(
        "Latest selected: {}",
        report.latest_selected_symbol.as_deref().unwrap_or("none")
    );
    println!(
        "Rank snapshot: candle={} top={} second={} rows={} spread_pct={}",
        report
            .latest_rank_snapshot_summary
            .evaluated_candle_time
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "none".to_string()),
        report
            .latest_rank_snapshot_summary
            .top_symbol
            .as_deref()
            .unwrap_or("none"),
        report
            .latest_rank_snapshot_summary
            .second_symbol
            .as_deref()
            .unwrap_or("none"),
        report.latest_rank_snapshot_summary.row_count,
        report
            .latest_rank_snapshot_summary
            .rank_spread_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string())
    );
    println!(
        "Jobs: data_refresh={} enabled={} rs_observation={} enabled={} scheduler_enabled={}",
        report.data_refresh_job_status.status,
        report.data_refresh_job_status.enabled,
        report.rs_observation_job_status.status,
        report.rs_observation_job_status.enabled,
        report.scheduler_enabled
    );
    println!(
        "Readiness: evidence_accumulating={} blocked_reason={}",
        report.readiness.evidence_accumulating,
        report.readiness.blocked_reason.as_deref().unwrap_or("none")
    );
    println!(
        "Execution safety counts: {:?}",
        report.execution_safety_counts
    );
    println!("No mutation: {}", report.no_mutation);
}

pub fn print_cross_asset_accept_shadow_preview(preview: &CrossAssetAcceptShadowPreview) {
    println!("Cross-asset accept-shadow preview");
    println!(
        "Candidate: {} package={} status={} no_mutation={}",
        preview.candidate_id,
        preview.package_id,
        preview.status.as_str(),
        preview.no_mutation
    );
    println!("Reasons:");
    for reason in &preview.reasons {
        println!("  - {}: {}", reason.code, reason.message);
    }
    println!("This preview is read-only and does not create shadow runs.");
}

pub fn print_cross_asset_shadow_observation_preview(
    preview: &CrossAssetShadowObservationPreviewResult,
) {
    println!("Cross-asset shadow observation preview");
    println!(
        "Candidate: {} package={} status={} decision={} no_mutation={}",
        preview.candidate_id,
        preview.package_id,
        preview.candidate_status.as_str(),
        preview.decision.as_str(),
        preview.no_mutation
    );
    println!(
        "Safety: server_observation_only_mode={} no_order_submission={} execution_authority={}",
        preview.server_observation_only_mode,
        preview.no_order_submission,
        preview.execution_authority
    );
    println!(
        "Latest aligned candle: {}  latest evaluated: {}",
        preview
            .latest_available_aligned_candle_time
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "none".to_string()),
        preview
            .latest_evaluated_candle_time
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "Selected: {}  rank_spread={}  market_filter={}  vol_filter={}",
        preview.selected_symbol.as_deref().unwrap_or("none"),
        preview
            .rank_spread_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        preview.market_filter_passed,
        preview.vol_filter_passed
    );
    println!("Reason: {}", preview.reason);
    println!("Ranking:");
    for row in &preview.ranking_snapshot {
        println!(
            "  #{} {} score={} return={} return_24h={} vol_24h={}",
            row.rank,
            row.symbol,
            row.score,
            row.return_pct,
            row.return_24h_pct
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            row.realized_vol_24h_pct
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        );
    }
    println!("Warnings: {:?}", preview.warnings);
    println!("This preview is read-only and does not create orders.");
}

pub fn print_cross_asset_shadow_observation_run(result: &CrossAssetShadowObservationRunResult) {
    println!("Cross-asset shadow observation run");
    println!(
        "Candidate: {} package={} status={} decision={} observation_created={} duplicate_same_candle={}",
        result.candidate_id,
        result.package_id,
        result.status.as_str(),
        result.decision.as_str(),
        result.observation_created,
        result.duplicate_same_candle
    );
    println!(
        "Safety: server_observation_only_mode={} research_observation_only_acknowledged={} no_order_submission={} execution_authority={}",
        result.server_observation_only_mode,
        result.research_observation_only_acknowledged,
        result.no_order_submission,
        result.execution_authority
    );
    println!(
        "Observation: {}  evaluated_candle={}",
        result
            .observation_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        result
            .evaluated_candle_time
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "Selected: {}  reason={}",
        result.selected_symbol.as_deref().unwrap_or("none"),
        result.reason
    );
    println!("Warnings: {:?}", result.warnings);
    println!("No orders, paper positions, fills, testnet orders, or live orders are created.");
}

pub fn print_research_state_snapshot(response: &serde_json::Value) {
    let snapshot = &response["snapshot"];
    println!("Research State Snapshot");
    println!(
        "Generated: {}",
        snapshot["generated_at"].as_str().unwrap_or("unknown")
    );
    println!(
        "Execution authority: {}",
        snapshot["platform_state"]["research_execution_authority"]
            .as_str()
            .unwrap_or("NONE")
    );
    println!(
        "Shadow observation only: {}",
        snapshot["platform_state"]["shadow_observation_only"]
            .as_bool()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    let active_candidates = snapshot["active_candidates"]
        .as_array()
        .or_else(|| snapshot["active_research_candidates"].as_array());
    let imported_candidates = snapshot["imported_candidates_provenance_only"]
        .as_array()
        .or_else(|| snapshot["imported_research_candidates"].as_array());
    let execution_eligible_candidates = snapshot["execution_eligible_candidates"].as_array();
    println!(
        "Imported provenance-only candidates: {}",
        imported_candidates.map(|items| items.len()).unwrap_or(0)
    );
    let cross_asset_candidates = snapshot["cross_asset_research_candidates"].as_array();
    println!(
        "Cross-asset research candidates: {}",
        cross_asset_candidates.map(|items| items.len()).unwrap_or(0)
    );
    println!(
        "Active candidates: {}",
        active_candidates.map(|items| items.len()).unwrap_or(0)
    );
    println!(
        "Execution eligible candidates: {}",
        execution_eligible_candidates
            .map(|items| items.len())
            .unwrap_or(0)
    );

    if let Some(rs) = snapshot["rs_observation"].as_object() {
        println!("\nRS observation:");
        println!(
            "  candidate: {}",
            rs.get("candidate_id")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown")
        );
        println!(
            "  status: {}",
            rs.get("health_status")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown")
        );
        println!(
            "  observations: {}/30",
            rs.get("independent_observation_count")
                .and_then(|value| value.as_i64())
                .unwrap_or(0)
        );
        println!(
            "  would_select: {}",
            rs.get("would_select_count")
                .and_then(|value| value.as_i64())
                .unwrap_or(0)
        );
        println!(
            "  latest evaluated: {}",
            rs.get("latest_evaluated_candle")
                .and_then(|value| value.as_str())
                .unwrap_or("none")
        );
        println!(
            "  latest aligned: {}",
            rs.get("latest_aligned_4h_candle")
                .and_then(|value| value.as_str())
                .unwrap_or("none")
        );
        let data_refresh = &rs["data_refresh_job_status"];
        let observation = &rs["rs_observation_job_status"];
        println!(
            "  jobs: data_refresh={} observation={}",
            data_refresh["status"].as_str().unwrap_or("unknown"),
            observation["status"].as_str().unwrap_or("unknown")
        );
    }

    let derivatives = &snapshot["derivatives_context"];
    if derivatives.is_object() {
        println!("\nDerivatives context:");
        println!(
            "  status: {}",
            derivatives["status"].as_str().unwrap_or("unknown")
        );
        println!(
            "  funding latest: {}",
            compact_json(&derivatives["latest_by_metric"]["funding"])
        );
        println!(
            "  oi latest: {}",
            compact_json(&derivatives["latest_by_metric"]["open_interest"])
        );
        println!(
            "  positioning latest: global={} taker={}",
            compact_json(&derivatives["latest_by_metric"]["global_long_short"]),
            compact_json(&derivatives["latest_by_metric"]["taker_buy_sell"])
        );
        println!(
            "  warning: {}",
            derivatives["warning"].as_str().unwrap_or("")
        );
    }

    println!("\nActive candidates:");
    if let Some(candidates) = active_candidates {
        if candidates.is_empty() {
            println!("  none");
        }
        for candidate in candidates {
            let progress = &candidate["evidence_progress"];
            println!(
                "  {} {} {} {} status={} shadow={}/{} would_submit={}/{} qualification={} dossier={} next={}",
                candidate["candidate_id"].as_str().unwrap_or("unknown"),
                candidate["strategy"].as_str().unwrap_or("unknown"),
                candidate["symbol"].as_str().unwrap_or("unknown"),
                candidate["timeframe"].as_str().unwrap_or("unknown"),
                candidate["status"].as_str().unwrap_or("unknown"),
                progress["independent_shadow_observation_count"].as_i64().unwrap_or(0),
                progress["independent_shadow_observation_threshold"].as_i64().unwrap_or(0),
                progress["would_submit_count"].as_i64().unwrap_or(0),
                progress["would_submit_threshold"].as_i64().unwrap_or(0),
                candidate["qualification"].as_str().unwrap_or("unknown"),
                candidate["dossier"].as_str().unwrap_or("unknown"),
                candidate["recommended_next_action"].as_str().unwrap_or("unknown")
            );
        }
    }

    println!("\nImported provenance-only candidates:");
    if let Some(candidates) = imported_candidates {
        if candidates.is_empty() {
            println!("  none");
        }
        for candidate in candidates {
            println!(
                "  {} {} {} {} schema={} provenance={} reconciliation={} next={} lifecycle_allowed={} execution_allowed={} config={}",
                candidate["candidate_id"].as_str().unwrap_or("unknown"),
                candidate["strategy"].as_str().unwrap_or("unknown"),
                candidate["symbol"].as_str().unwrap_or("unknown"),
                candidate["timeframe"].as_str().unwrap_or("unknown"),
                candidate["bundle_schema_version"].as_str().unwrap_or("unknown"),
                candidate["provenance_status"].as_str().unwrap_or("unknown"),
                candidate["reconciliation_status"].as_str().unwrap_or("unknown"),
                candidate["recommended_next_action"].as_str().unwrap_or("unknown"),
                candidate["lifecycle_allowed"]
                    .as_bool()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "false".to_string()),
                candidate["execution_allowed"]
                    .as_bool()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "false".to_string()),
                candidate["config_fingerprint"].as_str().unwrap_or("unknown")
            );
            if let Some(reason) = candidate["reason"].as_str() {
                println!("    reason: {reason}");
            }
        }
    }
    if let Some(warnings) = snapshot["imported_candidate_warnings"].as_array() {
        for warning in warnings {
            println!("  warning: {}", warning.as_str().unwrap_or(""));
        }
    }

    println!("\nExecution eligible candidates:");
    if let Some(candidates) = execution_eligible_candidates {
        if candidates.is_empty() {
            println!("  none");
        }
    } else {
        println!("  none");
    }

    println!("\nExecution safety counts:");
    let counts = &snapshot["execution_safety_counts"];
    for key in [
        "orders",
        "paper_positions",
        "paper_fills",
        "exchange_testnet_orders",
        "exchange_testnet_order_lifecycle_events",
        "testnet_shadow_promotions",
    ] {
        println!("  {key}: {}", counts[key].as_i64().unwrap_or(0));
    }

    println!("\nRejected/blocked decisions:");
    if let Some(decisions) = snapshot["decision_ledger"].as_array() {
        for decision in decisions {
            println!(
                "  {}: {} ({})",
                decision["family"].as_str().unwrap_or("unknown"),
                decision["decision"].as_str().unwrap_or("unknown"),
                decision["confidence"].as_str().unwrap_or("unknown")
            );
        }
    }

    println!("\nNext actions:");
    if let Some(actions) = snapshot["recommended_next_actions"].as_array() {
        for action in actions {
            println!("  - {}", action.as_str().unwrap_or(""));
        }
    }
}

pub fn print_research_batch(batch: &ResearchBatchResult) {
    println!("Batch: {}", batch.batch_id);
    println!("Status: {}", batch.status.as_str());
    println!("Created: {}", batch.created_at);
    println!("Completed: {}", display_option(batch.completed_at));
    println!("Experiments: {}", batch.experiment_ids.len());
    println!(
        "Config grid: total={} executed={} skipped_invalid={}",
        batch.total_candidate_configs,
        batch.executed_config_count,
        batch.skipped_invalid_config_count
    );
    println!("Walk-forward runs: {}", batch.walk_forward_run_ids.len());
    println!("Candidates created: {}", batch.created_candidate_ids.len());
    println!("Steps:");
    print_research_batch_steps(&batch.steps);
    if !batch.top_candidates.is_empty() {
        println!("Top candidates:");
        for candidate in &batch.top_candidates {
            println!(
                "  {} {} run={} score={} pnl_pct={} wf={} candidate={}",
                candidate.symbol,
                candidate.timeframe,
                candidate.experiment_run_id,
                candidate.score,
                candidate.pnl_pct,
                display_option(candidate.walk_forward_run_id),
                display_option(candidate.candidate_id)
            );
        }
    }
    if !batch.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &batch.recommendations {
            println!(
                "  {} {}: {}",
                recommendation.severity, recommendation.code, recommendation.message
            );
        }
    }
}

pub fn print_research_batches(batches: &[ResearchBatchResult]) {
    println!("Research batches:");
    for batch in batches {
        println!(
            "  {} status={} experiments={} skipped_invalid={} wf={} candidates={} created={}",
            batch.batch_id,
            batch.status.as_str(),
            batch.experiment_ids.len(),
            batch.skipped_invalid_config_count,
            batch.walk_forward_run_ids.len(),
            batch.created_candidate_ids.len(),
            batch.created_at
        );
    }
}

pub fn print_research_batch_steps(steps: &[ResearchBatchStep]) {
    for step in steps {
        println!(
            "  {} status={} started={} completed={} error={}",
            step.step_name,
            step.status.as_str(),
            step.started_at,
            display_option(step.completed_at),
            step.error.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_research_batch_triage(triage: &ResearchBatchTriage) {
    println!("Batch: {}", triage.batch_id);
    println!("Triage status: {}", triage.status.as_str());
    println!("Candidates: {}", triage.candidate_count);
    println!("Actionable: {}", triage.actionable_count);
    println!("Weak: {}", triage.weak_count);
    println!("Overfit: {}", triage.overfit_count);
    if !triage.candidates.is_empty() {
        println!("Top candidates:");
        for candidate in triage.candidates.iter().take(10) {
            println!(
                "  #{} {} {} candidate={} run={} score={} pnl_pct={} status={} wf={} recommendation={}",
                candidate.rank,
                candidate.symbol,
                candidate.timeframe,
                candidate
                    .candidate_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                candidate.experiment_run_id,
                candidate.experiment_score,
                candidate.experiment_pnl_pct,
                candidate.triage_status.as_str(),
                candidate.walk_forward_status.as_deref().unwrap_or("-"),
                candidate.walk_forward_recommendation.as_deref().unwrap_or("-")
            );
        }
    }
    if !triage.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &triage.recommendations {
            println!(
                "  {} {}: {}",
                recommendation.priority, recommendation.code, recommendation.message
            );
        }
    }
}

pub fn print_research_campaign(campaign: &ResearchCampaignResult) {
    println!("Campaign: {}", campaign.campaign_id);
    println!("Status: {}", campaign.status.as_str());
    println!("Created: {}", campaign.created_at);
    println!("Completed: {}", display_option(campaign.completed_at));
    print_research_campaign_summary(&campaign.summary);
}

pub fn print_research_regime_dataset(dataset: &ResearchRegimeDatasetResult) {
    println!("Regime dataset: {}", dataset.dataset_id);
    println!("Status: {}", dataset.status.as_str());
    println!(
        "Scope: {} {} {} -> {}",
        dataset.request.symbol,
        dataset.request.timeframe,
        dataset.request.start_time,
        dataset.request.end_time
    );
    println!(
        "Windows: candidates={} selected={} data_quality_blocked={} insufficient_candles={}",
        dataset.summary.total_candidate_windows,
        dataset.summary.selected_windows,
        dataset.summary.data_quality_blocked_windows,
        dataset.summary.insufficient_candle_windows
    );
    print_regime_counts(dataset);
    if !dataset.summary.missing_regimes.is_empty() {
        println!(
            "Missing regimes: {}",
            dataset
                .summary
                .missing_regimes
                .iter()
                .map(|regime| regime.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!("Top windows:");
    for window in dataset.windows.iter().take(10) {
        println!(
            "  {} {} -> {} confidence={} return_pct={} vol={} quality={} candles={}",
            window.regime_label.as_str(),
            window.start_time,
            window.end_time,
            window.confidence,
            window.return_pct,
            window.realized_volatility,
            window.data_quality_status.as_str(),
            window.candle_count
        );
    }
    if !dataset.summary.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &dataset.summary.recommendations {
            println!(
                "  {} {}: {}",
                recommendation.priority, recommendation.code, recommendation.message
            );
        }
    }
}

pub fn print_research_regime_datasets(datasets: &[ResearchRegimeDatasetResult]) {
    println!("Regime datasets:");
    for dataset in datasets {
        println!(
            "  {} status={} {} {} selected={} missing={} created={}",
            dataset.dataset_id,
            dataset.status.as_str(),
            dataset.request.symbol,
            dataset.request.timeframe,
            dataset.summary.selected_windows,
            dataset.summary.missing_regimes.len(),
            dataset.created_at
        );
    }
}

pub fn print_research_regime_windows(windows: &[ResearchRegimeWindow]) {
    println!("Regime windows:");
    for window in windows {
        println!(
            "  {} {} {} -> {} confidence={} return_pct={} vol={} range={} chop={} quality={} candles={}",
            window.regime_label.as_str(),
            window.symbol,
            window.start_time,
            window.end_time,
            window.confidence,
            window.return_pct,
            window.realized_volatility,
            window.avg_range_pct,
            window.choppiness_proxy,
            window.data_quality_status.as_str(),
            window.candle_count
        );
        println!(
            "    explanation label={} confidence={} alternates={} conditions={}",
            window.explanation.final_label.as_str(),
            window.explanation.confidence,
            window
                .explanation
                .alternate_labels_considered
                .iter()
                .map(|regime| regime.as_str())
                .collect::<Vec<_>>()
                .join(","),
            window.explanation.conditions.len()
        );
    }
}

pub fn print_research_regime_discovery(discovery: &ResearchRegimeDiscoveryResult) {
    println!("Regime discovery: {}", discovery.discovery_id);
    println!("Status: {}", discovery.status.as_str());
    println!(
        "Scope: {} {} {} -> {}",
        discovery.symbol, discovery.timeframe, discovery.scan_start, discovery.scan_end
    );
    println!(
        "Windows: scanned={} selected={} data_quality_blocked={} insufficient_data={}",
        discovery.total_windows_scanned,
        discovery.summary.selected_window_count,
        discovery.data_quality_blocked_count,
        discovery.summary.insufficient_data_count
    );
    println!("Regime counts:");
    for (regime, count) in &discovery.counts_by_regime {
        println!("  {}: {}", regime.as_str(), count);
    }
    if !discovery.missing_regimes.is_empty() {
        println!(
            "Missing regimes: {}",
            discovery
                .missing_regimes
                .iter()
                .map(|regime| regime.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!("Top windows:");
    for window in discovery.selected_windows.iter().take(10) {
        println!(
            "  {} {} -> {} confidence={} return_pct={} vol={} quality={} candles={}",
            window.regime_label.as_str(),
            window.start_time,
            window.end_time,
            window.confidence,
            window.return_pct,
            window.realized_volatility,
            window.data_quality_status.as_str(),
            window.candle_count
        );
        println!(
            "    explanation label={} confidence={} alternates={} conditions={}",
            window.explanation.final_label.as_str(),
            window.explanation.confidence,
            window
                .explanation
                .alternate_labels_considered
                .iter()
                .map(|regime| regime.as_str())
                .collect::<Vec<_>>()
                .join(","),
            window.explanation.conditions.len()
        );
    }
}

pub fn print_research_regime_discoveries(discoveries: &[ResearchRegimeDiscoveryResult]) {
    println!("Regime discoveries:");
    for discovery in discoveries {
        println!(
            "  {} status={} {} {} selected={} missing={} created={}",
            discovery.discovery_id,
            discovery.status.as_str(),
            discovery.symbol,
            discovery.timeframe,
            discovery.summary.selected_window_count,
            discovery.missing_regimes.len(),
            discovery.created_at
        );
    }
}

pub fn print_research_regime_discovery_windows(windows: &[ResearchRegimeDiscoveryCandidateWindow]) {
    println!("Regime discovery windows:");
    for window in windows {
        println!(
            "  {} {} -> {} confidence={} return_pct={} vol={} range={} chop={} quality={} candles={}",
            window.regime_label.as_str(),
            window.start_time,
            window.end_time,
            window.confidence,
            window.return_pct,
            window.realized_volatility,
            window.avg_range_pct,
            window.choppiness_proxy,
            window.data_quality_status.as_str(),
            window.candle_count
        );
    }
}

pub fn print_research_regime_calibration(calibration: &ResearchRegimeCalibrationResult) {
    println!("Regime calibration: {}", calibration.calibration_id);
    println!("Status: {}", calibration.status.as_str());
    println!(
        "Scope: {} {} {} -> {}",
        calibration.request.symbol,
        calibration.request.timeframe,
        calibration.request.scan_start,
        calibration.request.scan_end
    );
    if let Some(candidate_id) = &calibration.recommended_candidate_id {
        println!("Recommended candidate: {candidate_id}");
    }
    if let Some(config) = &calibration.recommended_config {
        println!(
            "Recommended config: trend_return={} trend_slope={} range_return_max={} range_chop_min={} high_vol={} low_vol={} min_confidence={} priority={}",
            config.trend_return_threshold_pct,
            config.trend_slope_threshold,
            config.range_return_max_pct,
            config.range_choppiness_min,
            config.high_volatility_threshold_pct,
            config.low_volatility_threshold_pct,
            config.min_confidence,
            config
                .priority_order
                .iter()
                .map(|regime| regime.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    if !calibration.missing_regimes.is_empty() {
        println!(
            "Missing regimes: {}",
            calibration
                .missing_regimes
                .iter()
                .map(|regime| regime.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!("Top configs:");
    for candidate in calibration.candidates.iter().take(5) {
        println!(
            "  {} score={} diversity={} balance={} avg_confidence={} dominant_share={} counts={}",
            candidate.candidate_id,
            candidate.total_score,
            candidate.diversity_score,
            candidate.balance_score,
            candidate.avg_confidence,
            candidate.dominant_regime_share,
            candidate
                .counts_by_regime
                .iter()
                .map(|(regime, count)| format!("{}={}", regime.as_str(), count))
                .collect::<Vec<_>>()
                .join(",")
        );
        if !candidate.warnings.is_empty() {
            println!("    warnings={}", candidate.warnings.join(","));
        }
    }
    if let Some(sample) = calibration
        .candidates
        .first()
        .and_then(|candidate| candidate.explanation_samples.first())
    {
        println!(
            "Explanation sample: label={} confidence={} return_pct={} vol={} range={} slope={} chop={} alternates={}",
            sample.final_label.as_str(),
            sample.confidence,
            sample.return_pct,
            sample.realized_volatility,
            sample.avg_range_pct,
            sample.trend_slope,
            sample.choppiness_proxy,
            sample
                .alternate_labels_considered
                .iter()
                .map(|regime| regime.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
}

pub fn print_research_regime_calibration_candidates(
    candidates: &[ResearchRegimeCalibrationCandidateResult],
) {
    println!("Regime calibration candidates:");
    for (index, candidate) in candidates.iter().enumerate() {
        println!(
            "  #{} {} score={} counts={} missing={}",
            index + 1,
            candidate.candidate_id,
            candidate.total_score,
            candidate
                .counts_by_regime
                .iter()
                .map(|(regime, count)| format!("{}={}", regime.as_str(), count))
                .collect::<Vec<_>>()
                .join(","),
            candidate
                .missing_regimes
                .iter()
                .map(|regime| regime.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
}

fn print_regime_counts(dataset: &ResearchRegimeDatasetResult) {
    println!("Regime counts:");
    for (regime, count) in &dataset.summary.regime_counts {
        println!("  {}: {}", regime.as_str(), count);
    }
}

pub fn print_research_campaigns(campaigns: &[ResearchCampaignResult]) {
    println!("Research campaigns:");
    for campaign in campaigns {
        println!(
            "  {} status={} planned={} completed={} failed={} actionable={} overfit={} weak={} candidates={} created={}",
            campaign.campaign_id,
            campaign.status.as_str(),
            campaign.summary.total_batches_planned,
            campaign.summary.total_batches_completed,
            campaign.summary.total_batches_failed,
            campaign.summary.actionable_batches,
            campaign.summary.overfit_only_batches,
            campaign.summary.weak_batches,
            campaign.summary.candidates_created,
            campaign.created_at
        );
    }
}

pub fn print_research_campaign_batches(batches: &[ResearchCampaignBatchResult]) {
    println!("Campaign batches:");
    for batch in batches {
        println!(
            "  #{} {} {} {} {} -> {} batch={} status={} triage={} candidates={} skipped_invalid={} error={}",
            batch.plan.plan_index,
            batch.plan.strategy_id,
            batch.plan.symbol,
            batch.plan.timeframe,
            batch.plan.start_time,
            batch.plan.end_time,
            display_option(batch.research_batch_id),
            batch
                .batch_status
                .map(|status| status.as_str())
                .unwrap_or("-"),
            batch.triage_status.as_str(),
            batch.candidates_created,
            batch.skipped_invalid_config_count,
            batch.error.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_research_stale_run_recovery(result: &ResearchStaleRunRecoveryResult) {
    println!(
        "Stale research recovery: scanned={} stale={} recovered={} skipped={}",
        result.scanned_count, result.stale_count, result.recovered_count, result.skipped_count
    );
    if result.targets.is_empty() {
        println!("No stale research artifacts found.");
    } else {
        println!("Targets:");
        for target in &result.targets {
            println!(
                "  {} {} current={} age_minutes={} proposed={} reason={}",
                target.target_type.as_str(),
                target.target_id,
                target.current_status,
                target.age_minutes,
                target.proposed_status,
                target.reason
            );
        }
    }
    if !result.actions.is_empty() {
        println!("Actions:");
        for action in &result.actions {
            println!(
                "  {} {} {} {}->{} status={} reason={}",
                action.target_type.as_str(),
                action.target_id,
                action.action,
                action.from_status,
                action.to_status,
                action.status.as_str(),
                action.reason
            );
        }
    }
    if !result.warnings.is_empty() {
        println!("Warnings:");
        for warning in &result.warnings {
            println!("  - {warning}");
        }
    }
}

pub fn print_research_campaign_summary(summary: &ResearchCampaignSummary) {
    println!(
        "Batches: planned={} completed={} failed={}",
        summary.total_batches_planned,
        summary.total_batches_completed,
        summary.total_batches_failed
    );
    println!(
        "Triage: actionable={} overfit={} weak={} data_quality_blocked={} no_candidates={}",
        summary.actionable_batches,
        summary.overfit_only_batches,
        summary.weak_batches,
        summary.data_quality_blocked_batches,
        summary.no_candidate_batches
    );
    println!("Candidates created: {}", summary.candidates_created);
    println!(
        "Config grid: total={} executed={} skipped_invalid={}",
        summary.total_candidate_configs,
        summary.executed_config_count,
        summary.skipped_invalid_config_count
    );
    println!(
        "Best strategy/symbol/timeframe: {}",
        summary
            .best_strategy_symbol_timeframe
            .as_deref()
            .unwrap_or("-")
    );
    if !summary.per_regime_performance.is_empty() {
        println!("Per-regime performance:");
        for regime in &summary.per_regime_performance {
            println!(
                "  {} planned={} completed={} failed={} actionable={} weak={} candidates={}",
                regime.regime_label.as_str(),
                regime.planned_batches,
                regime.completed_batches,
                regime.failed_batches,
                regime.actionable_batches,
                regime.weak_batches,
                regime.candidates_created
            );
        }
    }
    if !summary.top_candidates.is_empty() {
        println!("Top candidates:");
        for candidate in summary.top_candidates.iter().take(10) {
            println!(
                "  {} {} {} run={} score={} pnl_pct={} wf={} candidate={}",
                candidate.strategy_id,
                candidate.symbol,
                candidate.timeframe,
                candidate.experiment_run_id,
                candidate.score,
                candidate.pnl_pct,
                candidate
                    .robustness_status
                    .map(|status| status.as_str())
                    .unwrap_or("-"),
                display_option(candidate.candidate_id)
            );
        }
    }
    if !summary.findings.is_empty() {
        println!("Findings:");
        for finding in &summary.findings {
            println!(
                "  {} {}: {}",
                finding.severity, finding.code, finding.message
            );
        }
    }
    if !summary.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &summary.recommendations {
            println!(
                "  {} {}: {}",
                recommendation.priority, recommendation.code, recommendation.message
            );
        }
    }
}

pub fn print_research_campaign_failure_attribution(
    attribution: &ResearchCampaignFailureAttribution,
) {
    println!("Campaign failure attribution: {}", attribution.campaign_id);
    println!(
        "Top failure reasons: {}",
        attribution
            .overall_failure_reasons
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Some(top_regime) = attribution.regime_summary.first() {
        println!(
            "Regime summary: top={} windows={} candidates={} avg_return_pct={} avg_volatility={}",
            top_regime.label.as_str(),
            top_regime.window_count,
            top_regime.candidate_count,
            top_regime.avg_return_pct,
            top_regime.avg_realized_volatility
        );
    }
    if let Some(worst) = attribution.candidate_failure_table.first() {
        println!(
            "Worst failure source: {} {} {} candidate={} reasons={}",
            worst.strategy_id,
            worst.symbol,
            worst.timeframe,
            display_option(worst.candidate_id),
            worst
                .failure_reasons
                .iter()
                .map(|reason| reason.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    if let Some(best) = attribution
        .candidate_failure_table
        .iter()
        .max_by(|left, right| left.pnl_pct.cmp(&right.pnl_pct))
    {
        println!(
            "Best less-bad area: {} {} {} pnl_pct={} regime={}",
            best.strategy_id,
            best.symbol,
            best.timeframe,
            best.pnl_pct
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            best.regime_label.as_str()
        );
    }
    if !attribution.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &attribution.recommendations {
            println!(
                "  {} {}: {}",
                recommendation.priority, recommendation.code, recommendation.message
            );
        }
    }
}

pub fn print_research_regime_strategy_leaderboard(leaderboard: &ResearchRegimeStrategyLeaderboard) {
    println!("Regime strategy leaderboard: {}", leaderboard.campaign_id);
    if let Some(top) = leaderboard
        .overall_best
        .as_ref()
        .or_else(|| leaderboard.overall_rankings.first())
    {
        println!(
            "Overall best strategy: {} {} {} status={} median_pnl_pct={} robustness_score={}",
            top.strategy_id,
            top.symbol,
            top.timeframe,
            top.status.as_str(),
            top.median_pnl_pct,
            top.robustness_score
        );
    } else {
        println!("Overall best strategy: -");
    }
    if let Some(top) = &leaderboard.overall_promising {
        println!(
            "Overall promising strategy: {} {} {} status={} median_pnl_pct={} robustness_score={}",
            top.strategy_id,
            top.symbol,
            top.timeframe,
            top.status.as_str(),
            top.median_pnl_pct,
            top.robustness_score
        );
    } else {
        println!("Overall promising strategy: none");
    }
    if let Some(top) = &leaderboard.overall_least_bad {
        println!(
            "Overall least-bad strategy: {} {} {} status={} median_pnl_pct={} robustness_score={}",
            top.strategy_id,
            top.symbol,
            top.timeframe,
            top.status.as_str(),
            top.median_pnl_pct,
            top.robustness_score
        );
    } else {
        println!("Overall least-bad strategy: none");
    }
    if !leaderboard.best_strategy_by_regime.is_empty() {
        println!("Per-regime best strategy:");
        for selection in &leaderboard.best_strategy_by_regime {
            println!(
                "  {} {} {} {} status={} promising={} least_bad={} median_pnl_pct={} score={} reason={}",
                selection.regime_label.as_str(),
                selection.strategy_id,
                selection.symbol,
                selection.timeframe,
                selection.status.as_str(),
                selection.is_promising,
                selection.is_least_bad,
                selection.median_pnl_pct,
                selection.score,
                selection.reason
            );
        }
    }
    if !leaderboard.per_regime.is_empty() {
        println!("Per-regime weak/negative/overfit counts:");
        for cell in &leaderboard.per_regime {
            let weak = cell
                .rankings
                .iter()
                .filter(|ranking| ranking.status.as_str() == "WEAK")
                .count();
            let negative = cell
                .rankings
                .iter()
                .filter(|ranking| ranking.status.as_str() == "NEGATIVE")
                .count();
            let overfit = cell
                .rankings
                .iter()
                .filter(|ranking| ranking.status.as_str() == "OVERFIT")
                .count();
            println!(
                "  {} weak={} negative={} overfit={}",
                cell.regime_label.as_str(),
                weak,
                negative,
                overfit
            );
        }
    }
    if !leaderboard.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &leaderboard.recommendations {
            println!(
                "  {} {}: {}",
                recommendation.priority, recommendation.code, recommendation.message
            );
        }
    }
}

pub fn print_research_hypothesis_generation(result: &ResearchHypothesisGenerationResult) {
    println!(
        "Research hypotheses: generated={} persisted={}",
        result.generated_count, result.persisted_count
    );
    print_research_hypotheses(&result.hypotheses);
}

pub fn print_research_hypotheses(hypotheses: &[ResearchHypothesis]) {
    if hypotheses.is_empty() {
        println!("No research hypotheses.");
        return;
    }
    for hypothesis in hypotheses {
        print_research_hypothesis(hypothesis);
    }
}

pub fn print_research_hypothesis(hypothesis: &ResearchHypothesis) {
    println!(
        "Hypothesis {} priority={} status={} source={}",
        display_option(hypothesis.id),
        hypothesis.priority.as_str(),
        hypothesis.status.as_str(),
        hypothesis.source_type.as_str()
    );
    println!(
        "  scope: strategy={} symbol={} timeframe={} regime={}",
        hypothesis.strategy_id.as_deref().unwrap_or("-"),
        hypothesis.symbol.as_deref().unwrap_or("-"),
        hypothesis.timeframe.as_deref().unwrap_or("-"),
        hypothesis.regime.map(|value| value.as_str()).unwrap_or("-")
    );
    println!("  evidence: {}", hypothesis.evidence.summary);
    println!("  proposed action: {}", hypothesis.proposed_action);
    println!(
        "  proposed experiment config: {}",
        serde_json::to_string(&hypothesis.proposed_experiment_config)
            .unwrap_or_else(|_| "{}".to_string())
    );
    println!("  expected effect: {}", hypothesis.expected_effect);
    println!("  risk: {}", hypothesis.risk);
}

pub fn print_research_experiment_plans(plans: &[ResearchExperimentPlan]) {
    if plans.is_empty() {
        println!("No research experiment plans.");
        return;
    }
    for plan in plans {
        print_research_experiment_plan(plan);
    }
}

pub fn print_research_experiment_plan(plan: &ResearchExperimentPlan) {
    println!(
        "Experiment plan {} status={} validation={} type={} hypothesis={}",
        display_option(plan.id),
        plan.status.as_str(),
        plan.validation_status.as_str(),
        plan.plan_type.as_str(),
        plan.hypothesis_id
    );
    println!(
        "  scope: strategy={} symbol={} timeframe={} source_campaign={}",
        plan.strategy_id,
        plan.symbol.as_deref().unwrap_or("-"),
        plan.timeframe.as_deref().unwrap_or("-"),
        plan.source_campaign_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  recommendation: {} - {}",
        plan.recommendation.code, plan.recommendation.action
    );
    if !plan.validation_issues.is_empty() {
        println!("  validation issues: {}", plan.validation_issues.join("; "));
    }
    println!(
        "  proposed request: {}",
        serde_json::to_string(&plan.proposed_request).unwrap_or_else(|_| "{}".to_string())
    );
}

pub fn print_research_experiment_plan_run(result: &ResearchExperimentPlanRunResult) {
    println!(
        "Experiment plan run plan={} status={} mode={} type={} validation={}",
        result.plan_id,
        result.status.as_str(),
        result.mode.as_str(),
        result.plan_type.as_str(),
        result.validation_status.as_str()
    );
    println!("  hypothesis: {}", result.hypothesis_id);
    println!("  recommendation: {}", result.recommendation);
    if result.mode.as_str() == "PREVIEW" {
        println!("  preview semantics: persisted plan-run history only; no downstream research artifact created");
    } else {
        println!(
            "  run semantics: creates the explicit research artifact only after exact confirmation"
        );
    }
    if result.artifact_ids.is_empty() {
        println!("  artifacts created: none");
    } else {
        println!(
            "  artifacts created: {}",
            result
                .artifact_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    for artifact in &result.created_artifacts {
        if let (Some(kind), Some(id)) = (artifact.artifact_type(), artifact.artifact_id()) {
            println!("  {}: {}", kind, id);
        } else if let Some(kind) = artifact.artifact_type() {
            println!("  would create: {}", kind);
        }
    }
    if !result.warnings.is_empty() {
        println!("  warnings: {}", result.warnings.join("; "));
    }
    if !result.blockers.is_empty() {
        println!("  blockers: {}", result.blockers.join("; "));
    }
}

pub fn print_status(
    health: &HealthResponse,
    status: &StatusResponse,
    risk: &RiskStatusResponse,
    feed: &FeedStatusResponse,
) {
    println!(
        "API: {}  Service: {}  Env: {}",
        paint_state(&health.status, health.status.eq_ignore_ascii_case("ok")),
        health.service,
        health.environment
    );
    println!(
        "Mode: {}  Kill switch: {}  Paper allowed: {}  Live allowed: {}",
        status.market_mode,
        if risk.kill_switch.enabled {
            "ACTIVE".red().bold().to_string()
        } else {
            "inactive".green().to_string()
        },
        bool_word(risk.paper_trading_allowed),
        bool_word(risk.live_trading_allowed)
    );
    println!(
        "Dependencies: db={} event_bus={} execution={}",
        status.dependencies.database.status,
        status.dependencies.event_bus.status,
        status.dependencies.exchange_execution.status
    );

    if risk.kill_switch.enabled {
        println!(
            "{} {}",
            "WARNING:".red().bold(),
            risk.kill_switch
                .reason
                .as_deref()
                .unwrap_or("kill switch active")
        );
    }

    let degraded: Vec<_> = feed
        .feeds
        .iter()
        .filter(|item| {
            !item.freshness_status.eq_ignore_ascii_case("fresh")
                || !item.status.eq_ignore_ascii_case("connected")
        })
        .collect();

    println!("Feeds: {}", summarize_feeds(feed));
    if degraded.is_empty() {
        println!("Feed warnings: none");
    } else {
        println!(
            "{} {}",
            "WARNING:".red().bold(),
            degraded
                .iter()
                .map(|item| {
                    format!("{} {} {}", item.symbol, item.status, item.freshness_status)
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}

pub fn print_auth_login(user: &User) {
    println!("Logged in as {} ({})", user.email, user.role.as_str());
}

pub fn print_auth_me(user: &User) {
    println!("User ID: {}", user.id);
    println!("Email: {}", user.email);
    println!("Role: {}", user.role.as_str());
    println!("Status: {}", user.status.as_str());
}

pub fn print_auth_logout() {
    println!("Logged out.");
}

pub fn print_operator_report(response: &OperatorReportResponse) {
    if let Some(markdown) = response.report.markdown.as_deref() {
        println!("{markdown}");
        return;
    }

    println!("Report ID: {}", response.report.report_id);
    println!("Status: {}", response.report.status.as_str());
    println!(
        "Window: {} -> {}",
        response.report.window_start, response.report.window_end
    );
    println!("Generated At: {}", response.report.generated_at);
    println!("Findings: {}", response.report.findings.len());
    for finding in &response.report.findings {
        println!(
            "- {} {}: {}",
            finding.severity.as_str(),
            finding.section,
            finding.title
        );
    }
}

pub fn print_operator_report_list(response: &OperatorReportsListResponse) {
    for report in &response.reports {
        println!(
            "{}  {}  {} -> {}  created_at={}",
            report.report_id,
            report.status,
            report.window_start,
            report.window_end,
            report.created_at
        );
    }
}

pub fn print_execution_readiness(response: &ExecutionReadinessResponse) {
    let readiness = &response.readiness;
    println!(
        "Readiness: {}  Target: {}  Score: {}",
        readiness.status.as_str(),
        readiness.target.as_str(),
        readiness.score
    );
    println!("Computed: {}", readiness.computed_at);
    println!("ID: {}", readiness.readiness_id);

    if readiness.blocking_reasons.is_empty() {
        println!("Blockers: none");
    } else {
        println!("Blockers:");
        for reason in &readiness.blocking_reasons {
            println!("  - {:?}", reason);
        }
    }

    if readiness.warnings.is_empty() {
        println!("Warnings: none");
    } else {
        println!("Warnings:");
        for warning in &readiness.warnings {
            println!(
                "  - [{}] {}",
                format!("{:?}", warning.severity),
                warning.summary
            );
        }
    }

    if readiness.recommendations.is_empty() {
        println!("Recommendations: none");
    } else {
        println!("Recommendations:");
        for item in &readiness.recommendations {
            println!("  - {}", item.message());
        }
    }
}

pub fn print_execution_readiness_snapshots(response: &ExecutionReadinessSnapshotsResponse) {
    if response.snapshots.is_empty() {
        println!("No readiness snapshots found.");
        return;
    }

    for snapshot in &response.snapshots {
        println!(
            "{}  {}  score={}  {}",
            snapshot.id,
            snapshot.target.as_str(),
            snapshot.score,
            snapshot.status.as_str()
        );
    }
}

pub fn print_exchange_testnet_status(response: &ExchangeTestnetStatusResponse) {
    println!("Exchange: {}", response.exchange);
    println!("Environment: {}", response.environment);
    println!("Configured: {}", response.configured);
    println!("Request mode: {}", response.request_mode);
    println!("REST base URL: {}", response.rest_base_url);
    println!("WS base URL: {}", response.ws_base_url);
}

pub fn print_exchange_private_stream_status(response: &ExchangePrivateStreamStatusResponse) {
    let state = &response.state;
    println!("Exchange: {}", state.exchange);
    println!("Environment: {}", state.environment);
    println!("Status: {}", state.status);
    println!(
        "Listen key hash: {}",
        state.listen_key_hash.as_deref().unwrap_or("-")
    );
    println!(
        "Connected at: {}",
        state
            .connected_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Last event at: {}",
        state
            .last_event_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Reconnect count: {}", state.reconnect_count);
    println!("Last error: {}", state.last_error.as_deref().unwrap_or("-"));
    println!("Stale: {}", state.is_stale);
}

pub fn print_exchange_private_stream_events(events: &[ExchangePrivateStreamEventRecord]) {
    for event in events {
        println!(
            "{} type={} client_order_id={} status={} received_at={}",
            event.id,
            event.event_type,
            event.client_order_id.as_deref().unwrap_or("-"),
            event.order_status.as_deref().unwrap_or("-"),
            event.received_at
        );
    }
}

pub fn print_exchange_private_stream_listen_key(response: &ExchangePrivateStreamListenKeyResponse) {
    println!("Listen key status: {}", response.listen_key_status);
    println!(
        "Listen key: {}",
        response.listen_key_masked.as_deref().unwrap_or("-")
    );
    print_exchange_private_stream_status(&ExchangePrivateStreamStatusResponse {
        state: response.state.clone(),
        request_id: response.request_id.clone(),
        correlation_id: response.correlation_id.clone(),
        timestamp: response.timestamp,
    });
}

pub fn print_exchange_testnet_symbols(response: &ExchangeTestnetSymbolsResponse) {
    for symbol in &response.symbols {
        println!(
            "{}  {} / {}  status={}",
            symbol.symbol, symbol.base_asset, symbol.quote_asset, symbol.status
        );
    }
}

pub fn print_exchange_testnet_balances(response: &ExchangeTestnetBalancesResponse) {
    for balance in &response.balances {
        println!(
            "{}  free={} locked={}",
            balance.asset, balance.free, balance.locked
        );
    }
}

pub fn print_exchange_testnet_order(response: &ExchangeTestnetOrderResponse) {
    let order = &response.order;
    println!("Client order ID: {}", order.client_order_id);
    println!(
        "Exchange order ID: {}",
        order.exchange_order_id.as_deref().unwrap_or("-")
    );
    println!("Symbol: {}", order.symbol);
    println!("Side: {}", order.side);
    println!("Type: {}", order.order_type);
    println!("Status: {}", order.status);
    println!("Execution state: {}", order.execution_state);
    println!(
        "Requested quantity: {}",
        order.requested_qty.as_deref().unwrap_or("-")
    );
    println!(
        "Requested quote notional: {}",
        order.requested_notional.as_deref().unwrap_or("-")
    );
}

pub fn print_exchange_testnet_pipeline_preview(preview: &ExchangeTestnetPipelinePreview) {
    println!("Risk decision ID: {}", preview.risk_decision_id);
    println!(
        "Signal ID: {}",
        preview
            .signal_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Strategy ID: {}",
        preview.strategy_id.as_deref().unwrap_or("-")
    );
    println!("Symbol: {}", preview.symbol);
    println!("Side: {}", preview.side.as_str());
    println!("Order type: {}", preview.order_type.as_str());
    println!("Quantity: {}", preview.quantity);
    println!("Quote notional: {}", preview.quote_notional);
    println!("Reference price: {}", preview.reference_price);
    println!("Confirmation: {}", preview.confirmation_text);
}

pub fn print_exchange_testnet_pipeline_submit(response: &ExchangeTestnetPipelineSubmitResponse) {
    print_exchange_testnet_pipeline_preview(&response.preview);
    println!();
    println!("Submitted order:");
    println!("Client order ID: {}", response.order.client_order_id);
    println!(
        "Exchange order ID: {}",
        response.order.exchange_order_id.as_deref().unwrap_or("-")
    );
    println!("Status: {}", response.order.status);
    println!("Execution state: {}", response.order.execution_state);
}

pub fn print_testnet_shadow_run(run: &TestnetShadowRunResult) {
    println!("Run ID: {}", run.run_id);
    println!("Strategy ID: {}", run.strategy_id);
    println!("Symbol: {}", run.symbol);
    println!("Timeframe: {}", run.timeframe);
    println!("Decision: {}", run.decision.as_str());
    println!(
        "Signal ID: {}",
        run.signal_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Risk decision ID: {}",
        run.risk_decision_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Resolved price: {}",
        run.resolved_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Price source: {}",
        run.price_source.as_deref().unwrap_or("-")
    );
    if let Some(intent) = &run.would_submit_order {
        println!(
            "Would submit: {} {} type={} quote_notional={} quantity={}",
            intent.symbol,
            intent.side.as_str(),
            intent.order_type.as_str(),
            intent
                .quote_notional
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            intent
                .quantity
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
    if !run.reasons.is_empty() {
        println!(
            "Reasons: {}",
            run.reasons
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!("Correlation ID: {}", run.correlation_id);
}

pub fn print_testnet_shadow_runs(response: &TestnetShadowRunsResponse) {
    for run in &response.runs {
        println!(
            "{} {} {} {} signal={} risk={} price={}",
            run.created_at,
            run.strategy_id,
            run.symbol,
            run.decision.as_str(),
            run.signal_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            run.risk_decision_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            run.resolved_price
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

pub fn print_testnet_shadow_promotion(promotion: &TestnetShadowPromotionPreview) {
    println!("Promotion ID: {}", promotion.promotion_id);
    println!("Shadow Run ID: {}", promotion.shadow_run_id);
    println!("Status: {}", promotion.status.as_str());
    println!("Strategy: {}", promotion.strategy_id);
    println!("Symbol: {}", promotion.symbol);
    println!("Timeframe: {}", promotion.timeframe);
    println!(
        "Signal ID: {}",
        promotion
            .signal_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Risk Decision ID: {}", promotion.risk_decision_id);
    println!(
        "Resolved Price: {}",
        promotion
            .resolved_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Price Source: {}",
        promotion.price_source.as_deref().unwrap_or("-")
    );
    println!("Expires At: {}", promotion.expires_at);
    println!(
        "Client Order ID: {}",
        promotion.client_order_id.as_deref().unwrap_or("-")
    );
    println!(
        "Reasons: {}",
        promotion
            .reasons
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Correlation ID: {}", promotion.correlation_id);
}

pub fn print_testnet_shadow_promotions(response: &TestnetShadowPromotionsResponse) {
    for promotion in &response.promotions {
        println!(
            "{} {} {} {} expires={} client_order_id={}",
            promotion.created_at,
            promotion.shadow_run_id,
            promotion.symbol,
            promotion.status.as_str(),
            promotion.expires_at,
            promotion.client_order_id.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_testnet_shadow_promotion_submit(result: &TestnetShadowPromotionResult) {
    println!("Promotion ID: {}", result.promotion_id);
    println!("Shadow Run ID: {}", result.shadow_run_id);
    println!("Testnet Order ID: {}", result.testnet_order_id);
    println!("Client Order ID: {}", result.client_order_id);
    println!("Execution State: {}", result.execution_state.as_str());
    println!("Correlation ID: {}", result.correlation_id);
}

pub fn print_testnet_shadow_runner_status(response: &TestnetShadowRunnerStatusResponse) {
    println!(
        "Status: {}  Enabled: {}  Interval: {}s  Tick total: {}  Run total: {}",
        response.state.status.as_str(),
        response.config.enabled,
        response.config.interval_seconds,
        response.state.total_ticks,
        response.state.total_runs
    );
    println!(
        "Last tick: {}  Last success: {}",
        response
            .state
            .last_tick_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        response
            .state
            .last_success_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Strategies: {}  Symbols: {}  Timeframe: {}  Max runs/tick: {}",
        response.config.strategies.join(","),
        response.config.symbols.join(","),
        response.config.timeframe,
        response.config.max_runs_per_tick
    );
    println!(
        "Stale feed policy: {}  Last error: {}",
        response.config.stale_feed_policy.as_str(),
        response.state.last_error.as_deref().unwrap_or("-")
    );
}

pub fn print_testnet_shadow_runner_config(config: &aegis_core::TestnetShadowRunnerConfig) {
    println!("Enabled: {}", config.enabled);
    println!("Interval seconds: {}", config.interval_seconds);
    println!("Strategies: {}", config.strategies.join(","));
    println!("Symbols: {}", config.symbols.join(","));
    println!("Timeframe: {}", config.timeframe);
    println!("Max runs per tick: {}", config.max_runs_per_tick);
    println!("Stale feed policy: {}", config.stale_feed_policy.as_str());
    println!("Notes: {}", config.notes.as_deref().unwrap_or("-"));
    println!(
        "Updated by: {}  Updated at: {}",
        config
            .updated_by
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        config.updated_at
    );
}

pub fn print_testnet_shadow_runner_control(response: &TestnetShadowRunnerControlResponse) {
    println!("Status: {}", response.state.status.as_str());
    println!("Total ticks: {}", response.state.total_ticks);
    println!("Total runs: {}", response.state.total_runs);
    println!(
        "Last error: {}",
        response.state.last_error.as_deref().unwrap_or("-")
    );
    if let Some(tick) = &response.tick {
        println!(
            "Tick: {} attempted={} completed={} failed={} correlation={}",
            tick.status.as_str(),
            tick.attempted_runs,
            tick.completed_runs,
            tick.failed_runs,
            tick.correlation_id
        );
        if let Some(message) = &tick.message {
            println!("Tick message: {}", message);
        }
    }
}

pub fn print_exchange_testnet_order_lifecycle(
    response: &crate::api::ExchangeTestnetOrderLifecycleResponse,
) {
    println!("Client order ID: {}", response.client_order_id);
    println!("Current state: {}", response.current_state);
    for event in &response.events {
        println!(
            "{} {} -> {} source={} reason={}",
            event.created_at,
            event.previous_state.as_deref().unwrap_or("-"),
            event.next_state,
            event.transition_source,
            event.reason.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_exchange_testnet_repair(response: &ExchangeTestnetRepairResponse) {
    println!("Client order ID: {}", response.client_order_id);
    println!("Action: {}", response.action);
    println!("Status: {}", response.status);
    println!(
        "Previous state: {}",
        response.previous_state.as_deref().unwrap_or("-")
    );
    println!(
        "Next state: {}",
        response.next_state.as_deref().unwrap_or("-")
    );
    println!("Correlation ID: {}", response.correlation_id);
    for issue in &response.issues {
        println!("Issue: {} {}", issue.code, issue.message);
    }
}

pub fn print_exchange_testnet_repairs(repairs: &[ExchangeTestnetRepairActionRecord]) {
    for repair in repairs {
        println!(
            "{} action={} status={} {} -> {} reason={}",
            repair.created_at,
            repair.action,
            repair.status,
            repair.previous_state.as_deref().unwrap_or("-"),
            repair.next_state.as_deref().unwrap_or("-"),
            repair.reason.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_exchange_reconciliation_result(result: &ExchangeReconciliationResult) {
    println!("Run ID: {}", result.run_id);
    println!("Status: {}", result.status);
    println!("Checked orders: {}", result.checked_orders);
    println!("Matched orders: {}", result.matched_orders);
    println!("Mismatched orders: {}", result.mismatched_orders);
    println!("Unknown orders: {}", result.unknown_orders);
    println!("Correlation ID: {}", result.correlation_id);
}

pub fn print_exchange_reconciliation_runs(runs: &[ExchangeReconciliationRunRecord]) {
    for run in runs {
        println!(
            "{} status={} checked={} matched={} mismatched={} unknown={} started_at={}",
            run.id,
            run.status,
            run.checked_orders,
            run.matched_orders,
            run.mismatched_orders,
            run.unknown_orders,
            run.started_at
        );
    }
}

pub fn print_exchange_reconciliation_run(run: &ExchangeReconciliationRunRecord) {
    println!("Run ID: {}", run.id);
    println!("Exchange: {}", run.exchange);
    println!("Environment: {}", run.environment);
    println!("Status: {}", run.status);
    println!("Checked orders: {}", run.checked_orders);
    println!("Matched orders: {}", run.matched_orders);
    println!("Mismatched orders: {}", run.mismatched_orders);
    println!("Unknown orders: {}", run.unknown_orders);
    println!(
        "Failed reason: {}",
        run.failed_reason.as_deref().unwrap_or("-")
    );
    println!("Started at: {}", run.started_at);
    println!(
        "Completed at: {}",
        run.completed_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Correlation ID: {}", run.correlation_id);
}

pub fn print_exchange_reconciliation_mismatches(
    mismatches: &[ExchangeReconciliationMismatchRecord],
) {
    for mismatch in mismatches {
        println!(
            "{} client_order_id={} local_status={} exchange_status={} kind={} action={}",
            mismatch.id,
            mismatch.client_order_id,
            mismatch.local_status.as_deref().unwrap_or("-"),
            mismatch.exchange_status.as_deref().unwrap_or("-"),
            mismatch.mismatch_kind,
            mismatch.action
        );
    }
}

pub fn print_risk_action(response: &RiskActionResponse) {
    println!("Status: {}", paint_state(&response.status, true));
    println!("Message: {}", response.message);
    println!(
        "Kill switch: {}",
        if response.kill_switch.enabled {
            "ACTIVE".red().bold().to_string()
        } else {
            "inactive".green().to_string()
        }
    );
    println!("Correlation ID: {}", response.correlation_id);
}

pub fn print_risk_config(response: &RiskConfigResponse) {
    let config = &response.config;
    println!("Risk config ID: {}", config.config_id);
    println!("Max open positions: {}", config.max_open_positions);
    println!("Max daily loss %: {}", config.max_daily_loss_pct);
    println!("Max weekly loss %: {}", config.max_weekly_loss_pct);
    println!("Max position notional: {}", config.max_position_notional);
    println!("Max slippage %: {}", config.max_slippage_pct);
    println!("Max consecutive losses: {}", config.max_consecutive_losses);
    println!("Cooldown seconds: {}", config.cooldown_seconds);
    println!("Max signal age ms: {}", config.max_signal_age_ms);
    println!(
        "Stale feed threshold seconds: {}",
        config.stale_feed_threshold_seconds
    );
    println!("Config version: {}", config.config_version);
}

pub fn print_risk_config_validation(response: &RiskConfigValidationResponse) {
    println!("Valid: {}", response.validation.valid);
    for issue in &response.validation.issues {
        println!(
            "{}  {} {} {}",
            issue.severity.as_str(),
            issue.code,
            issue.field,
            issue.message
        );
    }
}

pub fn print_risk_config_versions(response: &RiskConfigVersionsResponse) {
    for version in &response.versions {
        println!(
            "v{}  config_id={} max_open_positions={} max_notional={}",
            version.version,
            version.config_id,
            version.config.max_open_positions,
            version.config.max_position_notional
        );
    }
}

pub fn print_risk_config_audit(response: &RiskConfigAuditResponse) {
    for entry in &response.audit {
        println!(
            "{}  config_id={} version={} issues={}",
            entry.created_at.to_rfc3339(),
            entry.config_id,
            entry
                .version
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            entry.validation_issues.len()
        );
    }
}

pub fn print_pipeline_result(result: &PaperTradingPipelineResult) {
    println!(
        "Pipeline decision: {}",
        if pipeline_decision_label(result) == "PAPER_ORDER_CREATED"
            || pipeline_decision_label(result) == "PAPER_ORDER_REUSED"
        {
            pipeline_decision_label(result).green().bold().to_string()
        } else {
            format!("WARNING: {}", pipeline_decision_label(result))
                .yellow()
                .bold()
                .to_string()
        }
    );
    println!("Signal ID: {}", display_option(result.signal_id));
    println!(
        "Risk decision ID: {}",
        display_option(result.risk_decision_id)
    );
    println!("Paper order ID: {}", display_option(result.paper_order_id));
    println!("Reasons: {}", display_vec(&result.reasons));
    println!("Correlation ID: {}", result.correlation_id);
}

pub fn print_strategy_list(response: &StrategyListResponse) {
    for strategy in &response.strategies {
        println!(
            "{}  enabled={} mode={} timeframe={} symbols={} notional={} lookback={} version={}",
            strategy.strategy_id,
            strategy.enabled,
            strategy.mode,
            strategy.timeframe,
            strategy.symbols.join(","),
            strategy.suggested_notional,
            strategy.lookback_candles,
            strategy.config_version
        );
    }
}

pub fn print_strategy_status(response: &StrategyStatusResponse) {
    let strategy = &response.strategy;
    println!("Strategy ID: {}", strategy.strategy_id);
    println!("Enabled: {}", strategy.enabled);
    println!("Mode: {}", strategy.mode);
    println!("Timeframe: {}", strategy.timeframe);
    println!("Symbols: {}", strategy.symbols.join(", "));
    println!("Suggested notional: {}", strategy.suggested_notional);
    println!("Lookback candles: {}", strategy.lookback_candles);
    println!("Max signal age ms: {}", strategy.max_signal_age_ms);
    println!("Cooldown seconds: {}", strategy.cooldown_seconds);
    println!("Config version: {}", strategy.config_version);
    println!(
        "Last evaluated at: {}",
        strategy
            .last_evaluated_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Last signal ID: {}",
        display_option(strategy.last_signal_id)
    );
}

pub fn print_strategy_config_validation(response: &StrategyConfigValidationResponse) {
    println!("Strategy ID: {}", response.validation.strategy_id);
    println!("Valid: {}", response.validation.valid);
    for issue in &response.validation.issues {
        println!(
            "{}  {} {} {}",
            issue.severity.as_str(),
            issue.code,
            issue.field,
            issue.message
        );
    }
}

pub fn print_strategy_config_versions(response: &StrategyConfigVersionsResponse) {
    for version in &response.versions {
        println!(
            "v{}  strategy={} mode={} enabled={} timeframe={} symbols={}",
            version.version,
            version.strategy_id,
            version.config.mode.as_str(),
            version.config.enabled,
            version.config.timeframe.as_str(),
            version
                .config
                .symbols
                .iter()
                .map(|symbol| symbol.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
}

pub fn print_strategy_config_audit(response: &StrategyConfigAuditResponse) {
    for entry in &response.audit {
        println!(
            "{}  strategy={} version={} issues={}",
            entry.created_at.to_rfc3339(),
            entry.strategy_id,
            entry
                .version
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            entry.validation_issues.len()
        );
    }
}

pub fn print_strategy_dry_run(response: &StrategyDryRunResponse) {
    let result = &response.result;
    println!("Strategy ID: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Config valid: {}", result.config_valid);
    println!("Would generate signal: {}", result.would_generate_signal);
    println!("Reason: {}", result.reason);
    println!(
        "Confidence: {}",
        result
            .confidence
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
}

pub fn print_strategy_diagnostics(response: &StrategyDiagnosticsResponse) {
    let result = &response.result;
    println!("Strategy ID: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Decision: {}", result.final_decision.as_str());
    println!("Enabled: {}", result.strategy_enabled);
    println!("Config valid: {}", result.config_valid);
    println!("Summary: {}", result.summary);
    println!(
        "Latest closed candle: {}",
        result
            .data_health
            .latest_closed_candle_time
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Closed candles: {} / required {}",
        result.data_health.available_closed_candles, result.data_health.required_closed_candles
    );
    if let Some(reason) = result.no_signal_reason {
        println!("No-signal reason: {}", reason.as_str());
    }
    if !result.validation_issues.is_empty() {
        println!("Validation issues:");
        for issue in &result.validation_issues {
            println!(
                "  - {} {}: {}",
                issue.severity.as_str(),
                issue.field,
                issue.message
            );
        }
    }
    println!("Condition checks:");
    for check in &result.condition_checks {
        println!(
            "  - [{}] {}: {}",
            check.severity.as_str(),
            check.name,
            check.message
        );
    }
    if !result.data_health.latest_closes.is_empty() {
        println!("Latest closes:");
        for close in &result.data_health.latest_closes {
            println!("  - {}", close);
        }
    }
}

pub fn print_strategy_opportunity_analysis(response: &StrategyOpportunityAnalysisResponse) {
    let result = &response.result;
    println!("Strategy ID: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Status: {}", result.recommendation.status.as_str());
    println!("Data quality: {}", result.data_quality_status.as_str());
    println!(
        "Closed candles: {} | evaluable windows: {}",
        result.total_closed_candles, result.evaluable_windows
    );
    println!(
        "Signals: {} | no-signal: {} | signal rate: {}%",
        result.would_signal_count,
        result.no_signal_count,
        result.signal_rate_pct.round_dp(4)
    );
    println!("Top blocking conditions:");
    for row in &result.top_blocking_conditions {
        println!(
            "  - {}: {} failures ({}%)",
            row.condition,
            row.failed_count,
            row.failure_rate_pct.round_dp(2)
        );
    }
    println!("Condition pass rates:");
    for row in &result.condition_pass_rates {
        println!(
            "  - {}: {} passed / {} failed ({}%)",
            row.condition,
            row.passed_count,
            row.failed_count,
            row.pass_rate_pct.round_dp(2)
        );
    }
    println!("Recommendations:");
    for message in &result.recommendation.messages {
        println!("  - {}", message);
    }
    if !result.example_fail_windows.is_empty() {
        println!("Sample fail windows:");
        for window in &result.example_fail_windows {
            println!(
                "  - {} blocker={}",
                window.source_candle_open_time.to_rfc3339(),
                window.blocking_condition.as_deref().unwrap_or("-")
            );
        }
    }
    if !result.example_pass_windows.is_empty() {
        println!("Sample pass windows:");
        for window in &result.example_pass_windows {
            println!("  - {}", window.source_candle_open_time.to_rfc3339());
        }
    }
}

pub fn print_strategy_exit_attribution(response: &StrategyExitAttributionResponse) {
    let result = &response.result;
    println!("Strategy ID: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Status: {}", result.status.as_str());
    println!("Recommendation: {}", result.recommendation.as_str());
    println!(
        "Signals: raw={} executable={}",
        result.total_raw_signals, result.total_executable_signals
    );
    println!(
        "Best holding window: {}",
        result
            .best_holding_window
            .map(|value| value.to_string())
            .unwrap_or_else(|| "N/A".to_string())
    );
    println!(
        "Worst holding window: {}",
        result
            .worst_holding_window
            .map(|value| value.to_string())
            .unwrap_or_else(|| "N/A".to_string())
    );
    println!("Suppression breakdown:");
    for row in &result.suppression_breakdown {
        println!("  - {}: {}", row.reason.as_str(), row.count);
    }
    println!("Holding windows:");
    for row in &result.per_holding_window {
        println!(
            "  - {} candles: trades={} win_rate={} avg={} median={} total={} best={} worst={} fee_drag={} recommendation={}",
            row.holding_candles,
            row.trade_count,
            row.win_rate.round_dp(2),
            row.avg_net_pnl_pct.round_dp(4),
            row.median_net_pnl_pct.round_dp(4),
            row.total_net_pnl_pct.round_dp(4),
            row.best_net_pnl_pct.round_dp(4),
            row.worst_net_pnl_pct.round_dp(4),
            row.fee_drag_pct.round_dp(4),
            row.recommendation.as_str()
        );
    }
}

pub fn print_strategy_signal_feature_attribution(
    response: &StrategySignalFeatureAttributionResponse,
) {
    let result = &response.result;
    println!("Strategy ID: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Status: {}", result.status.as_str());
    println!(
        "Signals: raw={} executable={} attributed={} insufficient_forward_data={}",
        result.total_raw_signals,
        result.executable_signals,
        result.attributed_signals,
        result.insufficient_forward_data_count
    );
    println!("Best buckets:");
    for bucket in &result.best_buckets {
        println!(
            "  - {}={}: samples={} win_rate={} avg={} median={} total={} recommendation={}",
            bucket.feature_name,
            bucket.bucket_label,
            bucket.sample_count,
            bucket.win_rate.round_dp(2),
            bucket.avg_net_pnl_pct.round_dp(4),
            bucket.median_net_pnl_pct.round_dp(4),
            bucket.total_net_pnl_pct.round_dp(4),
            bucket.recommendation.as_str()
        );
    }
    println!("Worst buckets:");
    for bucket in &result.worst_buckets {
        println!(
            "  - {}={}: samples={} win_rate={} avg={} median={} total={} recommendation={}",
            bucket.feature_name,
            bucket.bucket_label,
            bucket.sample_count,
            bucket.win_rate.round_dp(2),
            bucket.avg_net_pnl_pct.round_dp(4),
            bucket.median_net_pnl_pct.round_dp(4),
            bucket.total_net_pnl_pct.round_dp(4),
            bucket.recommendation.as_str()
        );
    }
    println!("Recommendations:");
    if result.recommendations.is_empty() {
        println!("  - N/A");
    } else {
        for recommendation in &result.recommendations {
            println!("  - {}", recommendation);
        }
    }
}

pub fn print_compression_breakout_refinement(response: &CompressionBreakoutRefinementResponse) {
    let result = &response.result;
    println!("Compression Breakout Refinement");
    println!("Strategy ID: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Status: {}", result.status.as_str());
    println!(
        "Candles/windows: {}/{}",
        result.total_closed_candles, result.total_windows
    );
    println!(
        "Top bottleneck: {}",
        result.top_bottleneck_condition.as_deref().unwrap_or("N/A")
    );
    println!("Funnel:");
    for row in &result.funnel {
        println!(
            "  - {}: reached={} passed={} failed={} pass_rate={} drop_off={}",
            row.condition,
            row.reached_count,
            row.passed_count,
            row.failed_count,
            row.pass_rate_pct.round_dp(2),
            row.drop_off_pct.round_dp(2)
        );
    }
    println!("Best sensitivity configs:");
    for row in &result.best_sensitivity_configs {
        println!(
            "  - c={} b={} min={} max_ext={} vol={} hold={} signals={} exec={} avg={} median={} win={} status={}",
            row.compression_lookback,
            row.breakout_lookback,
            row.min_breakout_pct,
            row.max_breakout_extension_pct,
            row.min_volume_expansion_ratio,
            row.holding_candles,
            row.signal_count,
            row.executable_count,
            row.avg_forward_net_pnl_pct.round_dp(4),
            row.median_forward_net_pnl_pct.round_dp(4),
            row.win_rate.round_dp(2),
            row.status.as_str()
        );
    }
    println!("Worst sensitivity configs:");
    for row in &result.worst_sensitivity_configs {
        println!(
            "  - c={} b={} min={} max_ext={} vol={} hold={} signals={} exec={} avg={} median={} win={} status={}",
            row.compression_lookback,
            row.breakout_lookback,
            row.min_breakout_pct,
            row.max_breakout_extension_pct,
            row.min_volume_expansion_ratio,
            row.holding_candles,
            row.signal_count,
            row.executable_count,
            row.avg_forward_net_pnl_pct.round_dp(4),
            row.median_forward_net_pnl_pct.round_dp(4),
            row.win_rate.round_dp(2),
            row.status.as_str()
        );
    }
    println!("Recommendations:");
    for recommendation in &result.recommendations {
        println!(
            "  - {} [{}]: {}",
            recommendation.code,
            recommendation.status.as_str(),
            recommendation.message
        );
    }
    println!("Warning: {}", result.no_promotion_warning);
}

pub fn print_orders(orders: &[OrderRecord]) {
    for order in orders {
        println!(
            "{}  {} {} qty={} status={} exec={} strategy={} signal={}",
            order.order_id,
            order.symbol,
            order.side,
            order.quantity,
            paint_order_status(&order.status),
            order.execution_state,
            order.strategy_id.as_deref().unwrap_or("-"),
            display_option(order.signal_id)
        );
    }
}

pub fn print_order_detail(order: &OrderRecord) {
    println!("Order ID: {}", order.order_id);
    println!("Client order ID: {}", order.client_order_id);
    println!(
        "Strategy ID: {}",
        order.strategy_id.as_deref().unwrap_or("-")
    );
    println!("Signal ID: {}", display_option(order.signal_id));
    println!("Risk decision ID: {}", order.risk_decision_id);
    println!("Symbol: {}", order.symbol);
    println!("Side: {}", order.side);
    println!("Status: {}", paint_order_status(&order.status));
    println!("Execution state: {}", order.execution_state);
    println!(
        "Requested notional: {}",
        order.requested_notional.as_deref().unwrap_or("-")
    );
    println!("Quantity: {}", order.quantity);
    println!("Filled quantity: {}", order.filled_qty);
    println!(
        "Filled price: {}",
        order.filled_price.as_deref().unwrap_or("-")
    );
    println!(
        "Average fill price: {}",
        order.avg_fill_price.as_deref().unwrap_or("-")
    );
    println!(
        "Status reason: {}",
        order.status_reason.as_deref().unwrap_or("-")
    );
    println!("Correlation ID: {}", order.correlation_id);
}

pub fn print_paper_account(response: &PaperAccountResponse) {
    let account = &response.account;
    println!("Account: {} ({})", account.name, account.id);
    println!("Base currency: {}", account.base_currency);
    println!("Initial equity: {}", account.initial_equity);
    println!("Current equity: {}", account.current_equity);
    println!("Realized PnL: {}", account.realized_pnl);
    println!("Unrealized PnL: {}", account.unrealized_pnl);
    println!("Status: {}", account.status);
}

pub fn print_paper_positions(response: &PaperPositionsResponse) {
    for position in &response.positions {
        print_paper_position(position);
    }
}

pub fn print_paper_position(position: &PaperPositionRecord) {
    println!(
        "{} {} qty={} entry={} mark={} unrealized={} realized={} status={} strategy={} signal={}",
        position.symbol,
        position.side,
        position.quantity,
        position.entry_price,
        position.mark_price.as_deref().unwrap_or("-"),
        position.unrealized_pnl,
        position.realized_pnl,
        position.status,
        position.strategy_id.as_deref().unwrap_or("-"),
        display_option(position.signal_id)
    );
}

pub fn print_paper_pnl(response: &PaperPnlResponse) {
    let pnl = &response.pnl;
    println!("Equity: {}", pnl.equity);
    println!("Realized PnL: {}", pnl.realized_pnl);
    println!("Unrealized PnL: {}", pnl.unrealized_pnl);
    println!("Daily PnL: {}", pnl.daily_pnl);
    println!("Drawdown %: {}", pnl.drawdown_pct);
    println!("Price status: {}", pnl.price_status);
    println!("Open positions: {}", pnl.open_positions_count);
}

pub fn print_paper_close(response: &PaperClosePositionResponse) {
    println!("Status: {}", response.status);
    println!("Position ID: {}", response.position_id);
    println!("Symbol: {}", response.symbol);
    println!("Quantity: {}", response.quantity);
    println!("Entry price: {}", response.entry_price);
    println!("Exit price: {}", response.exit_price);
    println!("Realized PnL: {}", response.realized_pnl);
    println!("Fee: {}", response.fee);
    println!("Slippage: {}", response.slippage_cost);
    println!("Close fill ID: {}", response.close_fill_id);
    println!("Journal entry ID: {}", response.journal_entry_id);
    println!("Correlation ID: {}", response.correlation_id);
}

pub fn print_paper_equity(response: &PaperEquityResponse) {
    for point in &response.equity {
        println!(
            "{} equity={} realized={} unrealized={} drawdown_pct={}",
            point.snapshot_at.to_rfc3339(),
            point.equity,
            point.realized_pnl,
            point.unrealized_pnl,
            point.drawdown_pct
        );
    }
}

pub fn print_paper_journal(response: &PaperTradeJournalResponse) {
    for entry in &response.journal {
        println!(
            "{} {} symbol={} pnl={} corr={}",
            entry.created_at.to_rfc3339(),
            entry.event_type,
            entry.symbol.as_deref().unwrap_or("-"),
            entry.pnl.as_deref().unwrap_or("-"),
            entry.correlation_id
        );
    }
}

pub fn print_events(response: &RecentEventsResponse) {
    for event in &response.events {
        println!(
            "{}  {}  {}  corr={}  event_id={}",
            event.occurred_at.to_rfc3339(),
            event.event_type,
            event.source,
            event.correlation_id,
            event.event_id
        );
    }
}

pub fn print_risk_decisions(response: &RiskDecisionsResponse) {
    for decision in &response.decisions {
        let label = if decision.decision.eq_ignore_ascii_case("rejected") {
            format!("WARNING: {}", decision.decision)
                .red()
                .bold()
                .to_string()
        } else {
            decision.decision.clone()
        };
        println!(
            "{}  decision={} symbol={} strategy={} signal={} reasons={}",
            decision.id,
            label,
            decision.symbol.as_deref().unwrap_or("-"),
            decision.strategy_id.as_deref().unwrap_or("-"),
            display_option(decision.signal_id),
            display_vec(&decision.reasons)
        );
    }
}

pub fn print_backtest_accepted(response: &BacktestRunAcceptedResponse) {
    println!("Run ID: {}", response.run_id);
    println!("Status: {}", response.status);
    println!("Strategy: {}", response.strategy_id);
    println!("Symbol: {}", response.symbol);
    println!("Trade count: {}", response.trade_count);
    println!(
        "Signals: raw={} executed={} cooldown_suppressed={} open_position_suppressed={}",
        response.raw_signal_count,
        response.executed_trade_count,
        response.cooldown_suppressed_count,
        response.open_position_suppressed_count
    );
    println!("PnL: {} ({}%)", response.pnl, response.pnl_pct);
    println!("Max drawdown %: {}", response.max_drawdown_pct);
    println!("Win rate: {}", response.win_rate);
    println!("Fee paid: {}", response.fee_paid);
    println!("Slippage cost: {}", response.slippage_cost);
    println!(
        "Correlation ID: {}",
        response
            .correlation_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
}

pub fn print_backtest_runs(runs: &[BacktestResult]) {
    for run in runs {
        println!(
            "{}  {} {} {} status={} pnl={} pnl_pct={} raw_signals={} trades={} cooldown={} open_position={}",
            run.run_id,
            run.strategy_id,
            run.symbol,
            run.timeframe,
            run.status,
            run.pnl,
            run.pnl_pct,
            run.raw_signal_count,
            run.executed_trade_count,
            run.cooldown_suppressed_count,
            run.open_position_suppressed_count
        );
    }
}

pub fn print_backtest_run(run: &BacktestResult) {
    println!("Run ID: {}", run.run_id);
    println!("Status: {}", run.status);
    println!("Strategy: {}", run.strategy_id);
    println!("Symbol: {}", run.symbol);
    println!("Timeframe: {}", run.timeframe);
    println!(
        "Window: {} -> {}",
        run.start_time.to_rfc3339(),
        run.end_time.to_rfc3339()
    );
    println!("Initial capital: {}", run.initial_capital);
    println!("Final equity: {}", run.final_equity);
    println!("PnL: {} ({}%)", run.pnl, run.pnl_pct);
    println!("Max drawdown %: {}", run.max_drawdown_pct);
    println!("Win rate: {}", run.win_rate);
    println!(
        "Trade breakdown: total={} wins={} losses={}",
        run.trade_count, run.winning_trades, run.losing_trades
    );
    println!(
        "Signal accounting: raw={} executed={} cooldown_suppressed={} open_position_suppressed={}",
        run.raw_signal_count,
        run.executed_trade_count,
        run.cooldown_suppressed_count,
        run.open_position_suppressed_count
    );
    println!("Fee paid: {}", run.fee_paid);
    println!("Slippage cost: {}", run.slippage_cost);
}

pub fn print_strategy_experiments(experiments: &[aegis_core::StrategyExperimentResult]) {
    for experiment in experiments {
        println!(
            "{}  {} {} {} status={} runs={} skipped_invalid={} best={} worst={}",
            experiment.experiment_id,
            experiment.strategy_id,
            experiment.symbol,
            experiment.timeframe,
            experiment.status.as_str(),
            experiment.run_count,
            experiment.skipped_invalid_config_count,
            experiment
                .best_run
                .as_ref()
                .map(|run| run.score.to_string())
                .unwrap_or_else(|| "-".to_string()),
            experiment
                .worst_run
                .as_ref()
                .map(|run| run.score.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

pub fn print_strategy_experiment(experiment: &aegis_core::StrategyExperimentResult) {
    println!("Experiment ID: {}", experiment.experiment_id);
    println!("Status: {}", experiment.status.as_str());
    println!("Strategy: {}", experiment.strategy_id);
    println!("Symbol: {}", experiment.symbol);
    println!("Timeframe: {}", experiment.timeframe);
    println!(
        "Window: {} -> {}",
        experiment.start_time.to_rfc3339(),
        experiment.end_time.to_rfc3339()
    );
    println!("Initial capital: {}", experiment.initial_capital);
    println!("Fee bps: {}", experiment.fee_bps);
    println!("Slippage bps: {}", experiment.slippage_bps);
    println!("Run count: {}", experiment.run_count);
    println!(
        "Config grid: total={} executed={} skipped_invalid={}",
        experiment.total_candidate_configs,
        experiment.executed_config_count,
        experiment.skipped_invalid_config_count
    );
    println!(
        "Best run: {}",
        experiment
            .best_run
            .as_ref()
            .map(|run| run.id.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Worst run: {}",
        experiment
            .worst_run
            .as_ref()
            .map(|run| run.id.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
}

pub fn print_strategy_experiment_runs(runs: &[aegis_core::StrategyExperimentRun]) {
    for run in runs {
        println!(
            "{}  rank={} lookback={} holding={} pnl_pct={} drawdown={} raw_signals={} trades={} cooldown={} open_position={} win_rate={} drag={} score={} warnings={}",
            run.id,
            run.rank,
            run.candidate.lookback_candles,
            run.candidate
                .holding_candles
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            run.pnl_pct,
            run.max_drawdown_pct,
            run.raw_signal_count,
            run.executed_trade_count,
            run.cooldown_suppressed_count,
            run.open_position_suppressed_count,
            run.win_rate,
            run.fee_slippage_drag_pct,
            run.score,
            if run.warnings.is_empty() {
                "-".to_string()
            } else {
                run.warnings.join(",")
            }
        );
    }
}

pub fn print_multi_timeframe_strategy_experiment(
    comparison: &aegis_core::StrategyMultiTimeframeExperimentResult,
) {
    println!("Experiment Group ID: {}", comparison.experiment_group_id);
    println!("Status: {}", comparison.status.as_str());
    println!("Strategy: {}", comparison.strategy_id);
    println!("Symbol: {}", comparison.symbol);
    println!("Timeframes: {}", comparison.requested_timeframes.join(", "));
    println!(
        "Config grid: total={} executed={} skipped_invalid={}",
        comparison.total_candidate_configs,
        comparison.executed_config_count,
        comparison.skipped_invalid_config_count
    );
    if let Some(best) = comparison.global_ranking.ranked_runs.first() {
        println!(
            "Best Global Candidate: timeframe={} run={} pnl_pct={} drawdown={} trades={} win_rate={} drag={} score={}",
            best.timeframe,
            best.run.id,
            best.run.pnl_pct,
            best.run.max_drawdown_pct,
            best.run.trade_count,
            best.run.win_rate,
            best.run.fee_slippage_drag_pct,
            best.run.score
        );
    } else {
        println!("Best Global Candidate: N/A");
    }

    println!("Per-timeframe best:");
    for timeframe in &comparison.timeframe_comparisons {
        if let Some(best_run) = &timeframe.best_run {
            println!(
                "  {} experiment={} candles={} best_run={} pnl_pct={} drawdown={} trades={} warnings={}",
                timeframe.candidate.timeframe,
                timeframe
                    .experiment_id
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                timeframe.candidate.candle_count,
                best_run.id,
                best_run.pnl_pct,
                best_run.max_drawdown_pct,
                best_run.trade_count,
                if timeframe.warnings.is_empty() {
                    "-".to_string()
                } else {
                    timeframe.warnings.join(",")
                }
            );
        }
    }

    let skipped: Vec<_> = comparison
        .timeframe_comparisons
        .iter()
        .filter(|item| item.skipped_reason.is_some())
        .collect();
    if skipped.is_empty() {
        println!("Skipped timeframes: none");
    } else {
        println!("Skipped timeframes:");
        for timeframe in skipped {
            println!(
                "  {} reason={}",
                timeframe.candidate.timeframe,
                timeframe.skipped_reason.as_deref().unwrap_or("-")
            );
        }
    }

    println!("Global ranking:");
    for entry in &comparison.global_ranking.ranked_runs {
        println!(
            "  {} run={} pnl_pct={} drawdown={} trades={} win_rate={} drag={} score={} warnings={}",
            entry.timeframe,
            entry.run.id,
            entry.run.pnl_pct,
            entry.run.max_drawdown_pct,
            entry.run.trade_count,
            entry.run.win_rate,
            entry.run.fee_slippage_drag_pct,
            entry.run.score,
            if entry.warnings.is_empty() {
                "-".to_string()
            } else {
                entry.warnings.join(",")
            }
        );
    }

    println!(
        "Warnings: {}",
        if comparison.warnings.is_empty() {
            "none".to_string()
        } else {
            comparison.warnings.join(", ")
        }
    );
}

pub fn print_strategy_walk_forward(result: &aegis_core::StrategyWalkForwardResult) {
    println!("Walk-forward ID: {}", result.walk_forward_id);
    println!("Status: {}", result.status.as_str());
    println!("Strategy: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Robustness status: {}", result.robustness_status.as_str());
    println!("Robustness score: {}", result.robustness_score);
    println!(
        "Windows: total={} completed={} skipped={}",
        result.total_windows, result.completed_windows, result.skipped_windows
    );
    println!(
        "Profitable vs losing: {} / {}",
        result.profitable_test_windows, result.losing_test_windows
    );
    println!(
        "PnL %: avg={} median={} worst={} best={}",
        result.avg_test_pnl_pct,
        result.median_test_pnl_pct,
        result.worst_test_pnl_pct,
        result.best_test_pnl_pct
    );
    println!("Avg drawdown %: {}", result.avg_max_drawdown_pct);
    println!(
        "Recommendation: {} - {}",
        result.recommendation.action, result.recommendation.reason
    );
}

pub fn print_strategy_walk_forward_runs(runs: &[aegis_core::StrategyWalkForwardResult]) {
    for run in runs {
        println!(
            "{}  {} {} {} status={} robustness={} score={} windows={}/{} pnl_avg={} pnl_worst={} pnl_best={} recommendation={}",
            run.walk_forward_id,
            run.strategy_id,
            run.symbol,
            run.timeframe,
            run.status.as_str(),
            run.robustness_status.as_str(),
            run.robustness_score,
            run.completed_windows,
            run.total_windows,
            run.avg_test_pnl_pct,
            run.worst_test_pnl_pct,
            run.best_test_pnl_pct,
            run.recommendation.action
        );
    }
}

pub fn print_strategy_walk_forward_windows(
    windows: &[aegis_core::StrategyWalkForwardWindowResult],
) {
    for window in windows {
        println!(
            "window={} train={}..{} test={}..{} status={} pnl_pct={} drawdown={} raw_signals={} trades={} cooldown={} open_position={} reason={}",
            window.window.window_index,
            window.window.train_start.to_rfc3339(),
            window.window.train_end.to_rfc3339(),
            window.window.test_start.to_rfc3339(),
            window.window.test_end.to_rfc3339(),
            window.status.as_str(),
            window.pnl_pct,
            window.max_drawdown_pct,
            window.raw_signal_count,
            window.executed_trade_count,
            window.cooldown_suppressed_count,
            window.open_position_suppressed_count,
            window.skip_reason.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_strategy_robustness_matrix(result: &StrategyRobustnessMatrixResult) {
    println!("Robustness matrix ID: {}", result.run_id);
    println!("Status: {}", result.status.as_str());
    println!("Cells: {}", result.cell_count);
    println!("Strategy ranking:");
    for (index, summary) in result.strategy_rankings.iter().enumerate() {
        println!(
            "{}. {} status={} score={} profitable_ratio={} avg_pnl={} median_pnl={} worst_pnl={} avg_trades={} regime_consistency={} data_penalty={} best_symbol={} worst_symbol={} best_regime={} worst_regime={}",
            index + 1,
            summary.strategy_id,
            summary.status.as_str(),
            summary.robustness_score,
            summary.profitable_window_ratio,
            summary.avg_pnl_pct,
            summary.median_pnl_pct,
            summary.worst_window_pnl_pct,
            summary.avg_trade_count,
            summary.regime_consistency,
            summary.data_quality_penalty,
            summary.best_symbol.as_deref().unwrap_or("-"),
            summary.worst_symbol.as_deref().unwrap_or("-"),
            summary.best_regime.map(|value| value.as_str()).unwrap_or("-"),
            summary.worst_regime.map(|value| value.as_str()).unwrap_or("-")
        );
        for finding in &summary.findings {
            println!(
                "  warning {} {}: {}",
                finding.severity, finding.code, finding.message
            );
        }
        for recommendation in &summary.recommendations {
            println!(
                "  recommendation {} {}: {}",
                recommendation.priority, recommendation.code, recommendation.message
            );
        }
    }
    for finding in &result.findings {
        println!(
            "Finding {} {}: {}",
            finding.severity, finding.code, finding.message
        );
    }
}

pub fn print_strategy_robustness_matrices(results: &[StrategyRobustnessMatrixResult]) {
    for result in results {
        let best = result
            .strategy_rankings
            .first()
            .map(|summary| summary.strategy_id.as_str())
            .unwrap_or("-");
        println!(
            "{} status={} cells={} best={} created_at={}",
            result.run_id,
            result.status.as_str(),
            result.cell_count,
            best,
            result.created_at.to_rfc3339()
        );
    }
}

pub fn print_strategy_robustness_matrix_cells(cells: &[StrategyRobustnessMatrixCell]) {
    for cell in cells {
        println!(
            "{} {} {} {}..{} status={} regime={} quality={} pnl_pct={} trades={} raw_signals={} executed={} cooldown={} win_rate={} drawdown={} fee_drag={}",
            cell.strategy_id,
            cell.symbol,
            cell.timeframe,
            cell.window_start.to_rfc3339(),
            cell.window_end.to_rfc3339(),
            cell.status.as_str(),
            cell.regime_label.as_str(),
            cell.data_quality_status.as_str(),
            cell.pnl_pct,
            cell.trade_count,
            cell.raw_signal_count,
            cell.executed_trade_count,
            cell.cooldown_suppressed_count,
            cell.win_rate,
            cell.max_drawdown_pct,
            cell.fee_drag
        );
    }
}

pub fn print_backfill_result(result: &aegis_core::CandleBackfillResult) {
    println!("Run ID: {}", result.run_id);
    println!("Status: {}", result.status.as_str());
    println!("Exchange: {}", result.exchange.as_str());
    println!("Symbol: {}", result.symbol);
    println!("Interval: {}", result.interval);
    println!(
        "Selected provider: {}",
        result.selected_provider.as_deref().unwrap_or("-")
    );
    if !result.provider_attempts.is_empty() {
        println!("Provider attempts:");
        for attempt in &result.provider_attempts {
            println!(
                "  {} {} success={} status={} kind={}",
                attempt.provider,
                attempt.base_url,
                attempt.success,
                attempt
                    .http_status
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                attempt
                    .error_kind
                    .map(|value| value.as_str().to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
        }
    }
    println!("Fetched candles: {}", result.fetched_candles);
    println!("Inserted candles: {}", result.inserted_candles);
    println!("Updated candles: {}", result.updated_candles);
    println!("Skipped candles: {}", result.skipped_candles);
    println!("Correlation ID: {}", result.correlation_id);
    if let Some(reason) = &result.failed_reason {
        println!("Failure reason: {}", reason);
    }
    if let Some(diagnostic) = &result.failure_diagnostic {
        println!("Error kind: {}", diagnostic.error_kind.as_str());
        println!("Retryable: {}", diagnostic.retryable);
    }
    if let Some(recommendation) = &result.recommendation {
        println!("Recommendation: {}", recommendation);
    }
}

pub fn print_backfill_runs(response: &CandleBackfillRunsResponse) {
    for run in &response.runs {
        println!(
            "{}  {} {} {} status={} fetched={} inserted={} updated={} skipped={}",
            run.run_id,
            run.exchange.as_str(),
            run.symbol,
            run.interval,
            run.status.as_str(),
            run.fetched_candles,
            run.inserted_candles,
            run.updated_candles,
            run.skipped_candles
        );
    }
}

pub fn print_backfill_run(response: &CandleBackfillRunResponse) {
    print_backfill_result(&response.run);
}

pub fn print_provider_health(response: &crate::api::ProviderHealthResponse) {
    let health = &response.health;
    println!("Provider: {}", health.provider);
    println!("Status: {}", health.status);
    println!("Base URL: {}", health.base_url);
    println!("Endpoint: {}", health.endpoint);
    println!(
        "Latency: {} ms",
        health
            .latency_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Error kind: {}",
        health
            .error_kind
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Recommendation: {}",
        health.recommendation.as_deref().unwrap_or("-")
    );
    println!("Fallback available: {}", health.fallback_available);
    if !health.attempts.is_empty() {
        println!("Provider attempts:");
        for attempt in &health.attempts {
            println!(
                "  {} {} success={} status={} kind={}",
                attempt.provider,
                attempt.base_url,
                attempt.success,
                attempt
                    .http_status
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                attempt
                    .error_kind
                    .map(|value| value.as_str().to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
        }
    }
}

pub fn print_candle_aggregation_result(result: &CandleAggregationResult) {
    println!("Exchange: {}", result.exchange.as_str());
    println!("Symbol: {}", result.symbol);
    println!(
        "Intervals: {} -> {}",
        result.source_interval, result.target_interval
    );
    println!("Window: {} -> {}", result.start_time, result.end_time);
    println!("Source candles: {}", result.source_candles);
    println!("Aggregated candles: {}", result.aggregated_candles);
    println!("Inserted: {}", result.inserted);
    println!("Updated: {}", result.updated);
    println!("Skipped incomplete: {}", result.skipped_incomplete);
    if let Some(correlation_id) = result.correlation_id {
        println!("Correlation ID: {}", correlation_id);
    }
}

pub fn print_candle_coverage(coverage: &MarketCandleCoverageSummary) {
    println!("Exchange: {}", coverage.exchange.as_str());
    println!("Symbol: {}", coverage.symbol);
    for interval in &coverage.intervals {
        println!("{}: {}", interval.interval, interval.candle_count);
    }
}

pub fn print_candle_aggregation_status(rows: &[CandleAggregationStatusRow]) {
    if rows.is_empty() {
        println!("No candle aggregation status rows.");
        return;
    }
    println!("Market candle aggregation status:");
    for row in rows {
        println!(
            "{} {}<-{} status={} lag={} latest_source={} latest_target={} inserted={} updated={} recommendation={}",
            row.symbol,
            row.target_interval,
            row.source_interval,
            row.status.as_str(),
            row.lag_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.latest_source_closed_candle
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
            row.latest_target_closed_candle
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
            row.inserted_last_tick
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.updated_last_tick
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.recommendation
        );
    }
}

pub fn print_market_data_quality_report(report: &MarketDataQualityReport) {
    println!("Status: {}", report.status.as_str());
    println!("Exchange: {}", report.exchange.as_str());
    println!("Symbol: {}", report.symbol);
    println!("Interval: {}", report.interval);
    println!("Window: {} -> {}", report.window_start, report.window_end);
    println!(
        "Candles: expected={} actual={} closed={} open={} missing={} coverage={}%",
        report.expected_candle_count,
        report.actual_candle_count,
        report.closed_candle_count,
        report.open_candle_count,
        report.missing_candle_count,
        report.coverage_pct
    );
    println!("Gap count: {}", report.gap_count);
    println!("Largest gap seconds: {}", report.largest_gap_seconds);
    if !report.gaps.is_empty() {
        println!("Gaps:");
        for gap in &report.gaps {
            println!(
                "- {} -> {} missing={} seconds={}",
                gap.start_time, gap.end_time, gap.missing_candle_count, gap.gap_seconds
            );
        }
    }
    if !report.findings.is_empty() {
        println!("Findings:");
        for finding in &report.findings {
            println!(
                "- [{}] {}: {}",
                finding.severity, finding.code, finding.message
            );
        }
    }
    if !report.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &report.recommendations {
            println!("- {}: {}", recommendation.code, recommendation.message);
        }
    }
}

pub fn print_market_data_repair_plan(plan: &MarketDataRepairPlan) {
    println!("Status: {}", plan.status.as_str());
    println!("Before quality: {}", plan.initial_quality_status.as_str());
    println!("Exchange: {}", plan.exchange.as_str());
    println!("Symbol: {}", plan.symbol);
    println!("Interval: {}", plan.interval);
    println!("Window: {} -> {}", plan.start_time, plan.end_time);
    println!("Gap count: {}", plan.gap_count);
    println!(
        "Source interval: {}  Reaggregate: {}",
        plan.estimated_source_interval.as_deref().unwrap_or("-"),
        plan.reaggregate_derived_intervals
    );
    println!("Repair ranges: {}", plan.repair_ranges.len());
    for range in &plan.repair_ranges {
        println!(
            "- {} {} -> {} missing={}",
            range.source_interval, range.start_time, range.end_time, range.missing_candle_count
        );
    }
    if !plan.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &plan.recommendations {
            println!("- {}: {}", recommendation.code, recommendation.message);
        }
    }
}

pub fn print_market_data_repair_run(run: &MarketDataRepairRunResult) {
    println!("Run ID: {}", run.run_id);
    println!("Status: {}", run.status.as_str());
    println!(
        "Quality: {} -> {}",
        run.before_quality_status.as_str(),
        run.after_quality_status.as_str()
    );
    println!("Gaps: {} -> {}", run.gap_count_before, run.gap_count_after);
    println!("Ranges repaired: {}", run.attempted_ranges.len());
    println!(
        "Candles: inserted={} updated={} skipped={} failed_ranges={}",
        run.inserted_candles, run.updated_candles, run.skipped_candles, run.failed_ranges
    );
    println!(
        "Provider: {}",
        run.selected_provider.as_deref().unwrap_or("-")
    );
    if let Some(aggregation) = &run.aggregation_result {
        println!(
            "Aggregation: source={} aggregated={} inserted={} updated={} skipped_incomplete={}",
            aggregation.source_candles,
            aggregation.aggregated_candles,
            aggregation.inserted,
            aggregation.updated,
            aggregation.skipped_incomplete
        );
    }
    if !run.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &run.recommendations {
            println!("- {}: {}", recommendation.code, recommendation.message);
        }
    }
}

pub fn print_market_data_repair_runs(response: &MarketDataRepairRunsResponse) {
    for run in &response.runs {
        println!(
            "{}  {} {} {} status={} quality={}->{} gaps={}->{} inserted={} updated={} failed_ranges={}",
            run.run_id,
            run.plan.symbol,
            run.plan.interval,
            run.plan.start_time,
            run.status.as_str(),
            run.before_quality_status.as_str(),
            run.after_quality_status.as_str(),
            run.gap_count_before,
            run.gap_count_after,
            run.inserted_candles,
            run.updated_candles,
            run.failed_ranges
        );
    }
}

pub fn print_research_data_coverage(coverage: &aegis_core::ResearchDataCoverageResult) {
    println!("Exchange: {}", coverage.exchange.as_str());
    println!("Symbol: {}", coverage.symbol);
    println!(
        "Window: {} -> {}",
        coverage.window_start, coverage.window_end
    );
    println!("Readiness: {}", coverage.status.as_str());
    for interval in &coverage.per_interval {
        println!(
            "{}: status={} coverage={} expected={} actual={} missing_ranges={}",
            interval.interval,
            interval.status.as_str(),
            interval.coverage_pct,
            interval.expected_candles,
            interval.actual_candles,
            interval.missing_ranges.len()
        );
    }
}

pub fn print_research_dataset_builds(builds: &[aegis_core::ResearchDatasetBuildResult]) {
    for build in builds {
        println!(
            "{}  {} {} status={} readiness={} intervals={}",
            build.build_id,
            build.exchange.as_str(),
            build.symbol,
            build.status.as_str(),
            build.coverage_after.status.as_str(),
            build.requested_intervals.join(",")
        );
    }
}

pub fn print_research_dataset_build(build: &aegis_core::ResearchDatasetBuildResult) {
    println!("Build ID: {}", build.build_id);
    println!("Status: {}", build.status.as_str());
    println!("Exchange: {}", build.exchange.as_str());
    println!("Symbol: {}", build.symbol);
    println!("Window: {} -> {}", build.start_time, build.end_time);
    println!("Intervals: {}", build.requested_intervals.join(", "));
    println!(
        "Readiness before: {}",
        build.coverage_before.status.as_str()
    );
    println!("Readiness after: {}", build.coverage_after.status.as_str());
    println!("Build steps:");
    for step in &build.steps {
        println!(
            "{} status={} started={} completed={}",
            step.step,
            step.status.as_str(),
            step.started_at,
            step.completed_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "-".to_string())
        );
    }
    println!("Final coverage:");
    for interval in &build.coverage_after.per_interval {
        println!(
            "{} status={} coverage={} expected={} actual={} missing_ranges={}",
            interval.interval,
            interval.status.as_str(),
            interval.coverage_pct,
            interval.expected_candles,
            interval.actual_candles,
            interval.missing_ranges.len()
        );
    }
    if let Some(reason) = &build.failed_reason {
        println!("Failure reason: {}", reason);
    }
}

pub fn print_research_candidates(candidates: &[aegis_core::ResearchCandidate]) {
    for candidate in candidates {
        println!(
            "{}  {} {} {} score={} status={}",
            candidate.id,
            candidate.strategy_id,
            candidate.symbol,
            candidate.timeframe,
            candidate
                .score
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            candidate.status.as_str()
        );
    }
}

pub fn print_research_candidate(candidate: &aegis_core::ResearchCandidate) {
    println!("Candidate ID: {}", candidate.id);
    println!("Strategy: {}", candidate.strategy_id);
    println!("Symbol: {}", candidate.symbol);
    println!("Timeframe: {}", candidate.timeframe);
    println!("Status: {}", candidate.status.as_str());
    println!(
        "Score: {}",
        candidate
            .score
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Metrics: pnl_pct={:?} max_drawdown_pct={:?} win_rate={:?} trade_count={:?} fee_drag={:?}",
        candidate.pnl_pct,
        candidate.max_drawdown_pct,
        candidate.win_rate,
        candidate.trade_count,
        candidate.fee_drag
    );
    if let Some(reason) = &candidate.rejection_reason {
        println!("Rejection reason: {}", reason);
    }
    if let Some(notes) = &candidate.notes {
        println!("Notes: {}", notes);
    }
    println!(
        "Config: {}",
        serde_json::to_string_pretty(&candidate.config).unwrap_or_else(|_| "{}".to_string())
    );
}

pub fn print_research_candidate_evidence_provenance(
    provenance: Option<&aegis_core::ResearchCandidateEvidenceProvenance>,
) {
    let Some(provenance) = provenance else {
        return;
    };
    println!(
        "Evidence provenance: experiment={:?} walk_forward={:?} robustness_matrix={:?} robustness_cell={:?} batch={:?} campaign={:?} proposal={:?}",
        provenance.source_experiment_run_id,
        provenance.source_walk_forward_run_id,
        provenance.source_robustness_matrix_run_id,
        provenance.source_robustness_matrix_cell_id,
        provenance.source_batch_id,
        provenance.source_campaign_id.or(provenance.campaign_id),
        provenance.source_proposal_id
    );
    if let Some(mode) = &provenance.candidate_creation_mode {
        println!("Candidate creation mode: {}", mode);
    }
    if let Some(status) = &provenance.gate_status {
        println!("Gate status: {}", status);
    }
    if let Some(fingerprint) = &provenance.config_fingerprint {
        println!("Config fingerprint: {}", fingerprint);
    }
}

pub fn print_research_candidate_walk_forward_evidence(
    latest: Option<&ResearchCandidateWalkForwardEvidence>,
    evidence: &[ResearchCandidateWalkForwardEvidence],
) {
    match latest {
        Some(item) => {
            println!("Latest walk-forward: {}", item.walk_forward_run_id);
            println!("Robustness: {}", item.robustness_status.as_str());
            println!(
                "Windows: total={} completed={} profitable={} losing={}",
                item.total_windows,
                item.completed_windows,
                item.profitable_windows,
                item.losing_windows
            );
            println!(
                "PnL pct: avg={} worst={} best={}",
                item.avg_pnl_pct, item.worst_pnl_pct, item.best_pnl_pct
            );
            println!(
                "Scores: robustness={} consistency={}",
                item.robustness_score, item.consistency_score
            );
            println!(
                "Recommendation: {}",
                item.recommendation_reason
                    .as_deref()
                    .or(item.recommendation_action.as_deref())
                    .unwrap_or("NONE")
            );
        }
        None => println!("Latest walk-forward: none"),
    }
    println!("Linked walk-forward runs: {}", evidence.len());
}

pub fn print_research_candidate_events(events: &[aegis_core::ResearchCandidateLifecycleEvent]) {
    for event in events {
        println!(
            "{}  {} -> {} decision={} reason={}",
            event.created_at.to_rfc3339(),
            event
                .previous_status
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "-".to_string()),
            event.next_status.as_str(),
            event.decision.as_str(),
            event.reason.clone().unwrap_or_else(|| "-".to_string())
        );
    }
}

pub fn print_research_candidate_reviews(reviews: &[ResearchCandidateReview]) {
    if reviews.is_empty() {
        println!("No research candidate reviews found.");
        return;
    }

    for review in reviews {
        println!(
            "{}  action={} status={} before={} after={} reason={} qualification_evaluation_id={}",
            review.created_at.to_rfc3339(),
            review.action.as_str(),
            review.status.as_str(),
            review.previous_candidate_status.as_str(),
            review
                .next_candidate_status
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| review.previous_candidate_status.as_str().to_string()),
            review.reason.clone().unwrap_or_else(|| "-".to_string()),
            review
                .qualification_evaluation_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

pub fn print_research_candidate_review_result(result: &ResearchCandidateReviewResult) {
    println!("Action: {}", result.review.action.as_str());
    println!("Review Status: {}", result.review.status.as_str());
    println!(
        "Candidate Status: {} -> {}",
        result.candidate_status_before.as_str(),
        result.candidate_status_after.as_str()
    );
    println!(
        "Reason: {}",
        result
            .review
            .reason
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("-")
    );
    println!(
        "Notes: {}",
        result
            .review
            .notes
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("-")
    );
}

pub fn print_research_candidate_observation(
    observation: &aegis_core::StrategyCandidateObservationResult,
) {
    println!("Observation ID: {}", observation.observation_id);
    println!("Candidate ID: {}", observation.candidate_id);
    println!("Strategy: {}", observation.strategy_id);
    println!(
        "Symbol / Timeframe: {} / {}",
        observation.symbol, observation.timeframe
    );
    println!(
        "Window: {} -> {}",
        observation.summary.window_start, observation.summary.window_end
    );
    println!(
        "Decision: {}  Status: {}",
        observation.decision.as_str(),
        observation.status.as_str()
    );
    println!(
        "Last observed at: {}",
        observation.last_observed_at.to_rfc3339()
    );
    println!(
        "Observation freshness: max_age={}s expires_at={}",
        observation
            .observation_max_age_seconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        observation
            .observation_expires_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Shadow runs: {}  Would-submit: {}  No-signal: {}  Risk-rejected: {}  Skipped: {}",
        observation.summary.shadow_runs,
        observation.summary.would_submit_count,
        observation.summary.no_signal_count,
        observation.summary.risk_rejected_count,
        observation.summary.skipped_count
    );
    println!(
        "Readiness: {} / {}",
        observation
            .summary
            .latest_readiness_status
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "UNKNOWN".to_string()),
        observation
            .summary
            .latest_readiness_score
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Rates: risk_rejection={} no_signal={}",
        observation.summary.risk_rejection_rate, observation.summary.no_signal_rate
    );
    println!(
        "Runner alignment: matches={} enabled={} status={}",
        observation.runner_alignment.strategy_config_matches_runner,
        observation.runner_alignment.runner_enabled,
        observation.runner_alignment.runner_status
    );
    println!(
        "Runner config: timeframe={} strategies={} symbols={}",
        observation.runner_alignment.runner_timeframe,
        observation.runner_alignment.runner_strategies.join(","),
        observation.runner_alignment.runner_symbols.join(",")
    );
    if !observation.runner_alignment.mismatch_reasons.is_empty() {
        println!("Runner mismatch reasons:");
        for reason in &observation.runner_alignment.mismatch_reasons {
            println!("- {}", reason);
        }
    }
    println!("Findings:");
    for finding in &observation.summary.findings {
        println!(
            "- {} [{}] {}",
            finding.code,
            if finding.blocking { "blocking" } else { "info" },
            finding.message
        );
    }
    if !observation.summary.recommendations.is_empty() {
        println!("Recommendations:");
        for recommendation in &observation.summary.recommendations {
            println!("- {}", recommendation);
        }
    }
    let next_action = match observation.decision {
        aegis_core::StrategyCandidateObservationDecision::Pass => {
            "Review candidate for testnet promotion readiness."
        }
        aegis_core::StrategyCandidateObservationDecision::Fail => {
            "Investigate failed findings before any promotion review."
        }
        aegis_core::StrategyCandidateObservationDecision::ContinueObserving => {
            "Keep shadow runner active until the observation window is satisfied."
        }
        aegis_core::StrategyCandidateObservationDecision::InsufficientData => {
            "Collect more shadow data before promotion review."
        }
    };
    println!("Next action: {}", next_action);
}

pub fn print_research_candidate_observations(history: &[ResearchCandidateObservationHistoryItem]) {
    if history.is_empty() {
        println!("No candidate observations found.");
        return;
    }

    for item in history {
        let observation = &item.observation;
        println!(
            "{}  status={} decision={} readiness={} runner={} freshness={} drifted={} eligible={}",
            observation.last_observed_at.to_rfc3339(),
            observation.status.as_str(),
            observation.decision.as_str(),
            observation
                .summary
                .latest_readiness_status
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string()),
            if observation.runner_alignment.strategy_config_matches_runner {
                "ALIGNED"
            } else {
                "MISMATCH"
            },
            item.freshness_status.as_str(),
            item.runner_config_drifted,
            item.accept_for_shadow_eligible
        );
        if !observation.summary.recommendations.is_empty() {
            println!(
                "Recommendations: {}",
                observation.summary.recommendations.join(" | ")
            );
        }
        if !observation.runner_alignment.mismatch_reasons.is_empty() {
            println!(
                "Mismatch reasons: {}",
                observation.runner_alignment.mismatch_reasons.join(" | ")
            );
        }
    }
}

pub fn print_research_candidate_observation_summary(
    summary: &ResearchCandidateObservationSummaryView,
) {
    println!("Candidate ID: {}", summary.candidate_id);
    println!("Total observations: {}", summary.total_observations);
    println!(
        "Latest status: {}",
        summary
            .latest_observation_status
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "NONE".to_string())
    );
    println!(
        "Latest readiness: {}",
        summary
            .latest_readiness_status
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "UNKNOWN".to_string())
    );
    println!(
        "Latest runner alignment: {}",
        summary
            .latest_runner_alignment
            .as_ref()
            .map(|value| if value.strategy_config_matches_runner {
                "ALIGNED".to_string()
            } else {
                "MISMATCH".to_string()
            })
            .unwrap_or_else(|| "UNKNOWN".to_string())
    );
    println!(
        "Last observed at: {}",
        summary
            .last_observed_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Latest formal observation: at={} stale={}",
        summary
            .latest_formal_observation_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string()),
        summary.formal_observation_stale
    );
    println!(
        "Latest linked shadow: id={} decision={} status={} at={}",
        summary
            .latest_linked_shadow_run_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        summary
            .latest_linked_shadow_decision
            .as_deref()
            .unwrap_or("-"),
        summary
            .latest_linked_shadow_status
            .as_deref()
            .unwrap_or("-"),
        summary
            .latest_linked_shadow_run_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Latest valid shadow: id={} decision={} status={} at={}",
        summary
            .latest_valid_shadow_run_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        summary
            .latest_valid_shadow_decision
            .as_deref()
            .unwrap_or("-"),
        summary.latest_valid_shadow_status.as_deref().unwrap_or("-"),
        summary
            .latest_valid_shadow_run_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Counts: stale={} mismatch={} drift={} shadow_completed={} no_signal={} would_submit={} risk_rejected={} skipped={}",
        summary.stale_count,
        summary.alignment_mismatch_count,
        summary.runner_config_drift_count,
        summary.linked_shadow_completed_count,
        summary.linked_shadow_no_signal_count,
        summary.linked_shadow_would_submit_count,
        summary.linked_shadow_risk_rejected_count,
        summary.linked_shadow_skipped_count
    );
    println!(
        "Current accept_for_shadow eligibility: {}",
        if summary.current_accept_for_shadow_eligible {
            "ELIGIBLE"
        } else {
            "NOT_ELIGIBLE"
        }
    );
    if !summary.current_accept_for_shadow_blockers.is_empty() {
        println!(
            "Eligibility blockers: {}",
            summary.current_accept_for_shadow_blockers.join(", ")
        );
    }
    if !summary.latest_recommendations.is_empty() {
        println!(
            "Latest recommendations: {}",
            summary.latest_recommendations.join(" | ")
        );
    }
}

pub fn print_research_candidate_shadow_performance(
    performance: &ResearchCandidateShadowPerformance,
) {
    println!("Candidate ID: {}", performance.candidate_id);
    println!(
        "Window: {} -> {}",
        performance.window_start.to_rfc3339(),
        performance.window_end.to_rfc3339()
    );
    println!("Total runs: {}", performance.total_shadow_runs);
    println!(
        "Outcome breakdown: would_submit={} no_signal={} risk_rejected={} skipped={} error={}",
        performance.would_submit_count,
        performance.no_signal_count,
        performance.risk_rejected_count,
        performance.skipped_count,
        performance.error_count
    );
    println!("Would-submit rate: {}%", performance.would_submit_rate_pct);
    println!(
        "Risk rejection rate: {}%",
        performance.risk_rejection_rate_pct
    );
    println!(
        "Last run time: {}",
        performance
            .last_shadow_run_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Runner alignment: {}",
        if performance.runner_alignment_current {
            "COVERED"
        } else {
            "NOT_COVERED"
        }
    );
    println!("Status: {}", performance.status.as_str());
    println!("Recommendation: {}", performance.recommendation.as_str());
}

pub fn print_research_candidate_shadow_observe_once(
    result: &ResearchCandidateShadowObserveOnceResult,
) {
    println!("Candidate ID: {}", result.candidate_id);
    println!("Strategy ID: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Decision: {}", result.decision.as_str());
    println!("Reason: {}", result.reason.as_deref().unwrap_or("-"));
    println!(
        "Latest evaluated candle: {}",
        result
            .latest_evaluated_candle_open_time
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Latest available candle: {}",
        result
            .latest_available_candle_open_time
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Independent evidence created: {}",
        result.independent_evidence_created
    );
    if let Some(run) = &result.shadow_run {
        println!("Shadow run ID: {}", run.run_id);
        println!("Shadow decision: {}", run.decision.as_str());
        println!("Shadow status: {}", run.status.as_str());
        println!(
            "Observed candle: {}",
            run.evaluated_candle_open_time
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

pub fn print_research_candidate_shadow_pnl_attribution(
    attribution: &ResearchShadowPnlAttributionResult,
) {
    println!("Candidate ID: {}", attribution.candidate_id);
    println!(
        "Candidate: {}/{}/{}",
        attribution.strategy_id, attribution.symbol, attribution.timeframe
    );
    println!(
        "Research-only. This does not create orders. fee_bps={} slippage_bps={}",
        attribution.fee_bps, attribution.slippage_bps
    );
    println!(
        "Total attributed runs: {}",
        attribution.summary.total_attributed_runs
    );
    println!(
        "Insufficient forward data: {}",
        attribution.summary.insufficient_forward_data_count
    );
    println!(
        "Extreme PnL count: {}",
        attribution.summary.extreme_pnl_count
    );
    println!("Gap count: {}", attribution.summary.gap_detected_count);
    for warning in &attribution.summary.warnings {
        println!("Warning: {warning}");
    }
    println!(
        "Best holding window: {} avg_net_pnl_pct={}",
        attribution
            .best_holding_window
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        attribution
            .best_avg_net_pnl_pct
            .map(|value| value.round_dp(4).to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Recommendation: {}",
        attribution.latest_shadow_pnl_status.as_str()
    );
    println!("Holding windows:");
    for window in &attribution.summary.per_holding_window {
        println!(
            "  {} candles: trades={} win_rate={} avg_net={} median_net={} best={} worst={} total_net={} fee_drag={} recommendation={}",
            window.holding_window,
            window.trade_count,
            window.win_rate.round_dp(2),
            window.avg_net_pnl_pct.round_dp(4),
            window.median_net_pnl_pct.round_dp(4),
            window.best_net_pnl_pct.round_dp(4),
            window.worst_net_pnl_pct.round_dp(4),
            window.total_net_pnl_pct.round_dp(4),
            window.fee_drag_pct.round_dp(4),
            window.recommendation.as_str()
        );
    }
    println!("Top attributed trades:");
    for trade in attribution.trades.iter().take(5) {
        let best_window = trade
            .holding_windows
            .iter()
            .filter(|window| window.net_pnl_pct.is_some())
            .max_by(|left, right| {
                left.net_pnl_pct
                    .unwrap_or_default()
                    .cmp(&right.net_pnl_pct.unwrap_or_default())
            });
        if let Some(window) = best_window {
            println!(
                "  shadow_run={} window={} status={} entry={} @ {} exit={} @ {} net_pnl={}%",
                trade.shadow_run_id,
                window.holding_window,
                window.attribution_status.as_str(),
                trade
                    .entry_price
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                trade
                    .entry_candle_open_time
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| "-".to_string()),
                window
                    .exit_price
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                window
                    .exit_candle_close_time
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| "-".to_string()),
                window
                    .net_pnl_pct
                    .map(|value| value.round_dp(4).to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
        }
    }
}

pub fn print_research_candidate_qualification(
    qualification: &ResearchCandidateQualificationResult,
) {
    println!(
        "Status: {}  Score: {}",
        qualification.status.as_str(),
        qualification.score
    );
    println!(
        "Readiness: {}  Penalty: -{}",
        qualification
            .latest_readiness_status
            .map(|value| value.as_str())
            .unwrap_or("UNKNOWN"),
        qualification.readiness_penalty_points
    );
    println!(
        "Walk-forward: status={} run={} score={} consistency={}",
        qualification
            .walk_forward_status
            .map(|value| value.as_str())
            .unwrap_or("MISSING"),
        qualification
            .walk_forward_run_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        qualification
            .walk_forward_score
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        qualification
            .walk_forward_consistency_score
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Runner alignment: {}  Fresh observation: {}",
        if qualification.runner_alignment_valid {
            "VALID"
        } else {
            "INVALID"
        },
        if qualification.fresh_observation {
            "YES"
        } else {
            "NO"
        }
    );
    println!(
        "Formal observation: status={} stale={}",
        qualification
            .formal_observation_status
            .map(|value| value.as_str())
            .unwrap_or("MISSING"),
        qualification.formal_observation_stale
    );
    println!(
        "Linked shadow evidence: completed={} no_signal={} would_submit={} risk_rejected={} skipped={} latest_valid={} latest_skipped={}",
        qualification.linked_shadow_completed_count,
        qualification.linked_shadow_no_signal_count,
        qualification.linked_shadow_would_submit_count,
        qualification.linked_shadow_risk_rejected_count,
        qualification.linked_shadow_skipped_count,
        qualification
            .latest_valid_shadow_run_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string()),
        qualification
            .latest_skipped_shadow_run_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Evidence interpretation: {}",
        qualification.evidence_interpretation
    );
    if qualification.threshold_override_below_default {
        println!(
            "Threshold override warning: Qualification threshold override is below default; treat result as exploratory. (-{} points)",
            qualification.threshold_override_penalty_points
        );
    }
    if qualification.blockers.is_empty() {
        println!("Blockers: none");
    } else {
        println!("Blockers:");
        for blocker in &qualification.blockers {
            println!("  - {blocker}");
        }
    }
    if qualification.warnings.is_empty() {
        println!("Warnings: none");
    } else {
        println!("Warnings:");
        for warning in &qualification.warnings {
            println!("  - {warning}");
        }
    }
    if qualification.recommendations.is_empty() {
        println!("Recommendations: none");
    } else {
        println!("Recommendations:");
        for recommendation in &qualification.recommendations {
            println!("  - {}", recommendation.message());
        }
    }
    if let Some(performance) = &qualification.shadow_performance {
        println!(
            "Shadow summary: runs={} would_submit={} risk_rejected={} skipped={} error={}",
            performance.total_shadow_runs,
            performance.would_submit_count,
            performance.risk_rejected_count,
            performance.skipped_count,
            performance.error_count
        );
    } else {
        println!("Shadow summary: none");
    }
    let thresholds = &qualification.thresholds;
    println!(
        "Thresholds: min_shadow_runs={} min_would_submit_count={} max_risk_rejection_rate_pct={} max_error_or_skipped_rate_pct={}",
        thresholds.min_shadow_runs,
        thresholds.min_would_submit_count,
        thresholds.max_risk_rejection_rate_pct,
        thresholds.max_error_or_skipped_rate_pct
    );
    println!("Score explanation:");
    for item in &qualification.score_explanation {
        println!("  - {item}");
    }
}

pub fn print_research_candidate_qualification_evaluation(
    evaluation: &ResearchCandidateQualificationEvaluation,
    change: Option<&ResearchCandidateQualificationChange>,
    trend: ResearchCandidateQualificationTrend,
) {
    println!(
        "Status: {}  Score: {}  Trend: {}",
        evaluation.status.as_str(),
        evaluation.score,
        trend.as_str()
    );
    if let Some(change) = change {
        println!(
            "Previous: {} / {}  Delta: {}",
            change
                .previous_status
                .map(|value| value.as_str())
                .unwrap_or("UNKNOWN"),
            change
                .previous_score
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            change.score_delta
        );
        println!(
            "Changed: status={} material_score={} newly_qualified={} lost_qualification={}",
            change.status_changed,
            change.material_score_change,
            change.newly_qualified,
            change.lost_qualification
        );
    } else {
        println!("Previous: none");
    }
    println!(
        "Readiness: {}  Runs: {}  Would-submit: {}  Risk rejection: {}",
        evaluation
            .latest_readiness_status
            .map(|value| value.as_str())
            .unwrap_or("UNKNOWN"),
        evaluation.total_shadow_runs,
        evaluation.would_submit_count,
        evaluation
            .risk_rejection_rate_pct
            .map(|value| format!("{value}%"))
            .unwrap_or_else(|| "-".to_string())
    );
    if evaluation.blockers.is_empty() {
        println!("Blockers: none");
    } else {
        println!("Blockers:");
        for blocker in &evaluation.blockers {
            println!("  - {blocker}");
        }
    }
    if evaluation.warnings.is_empty() {
        println!("Warnings: none");
    } else {
        println!("Warnings:");
        for warning in &evaluation.warnings {
            println!("  - {warning}");
        }
    }
    if evaluation.recommendations.is_empty() {
        println!("Recommendations: none");
    } else {
        println!("Recommendations:");
        for recommendation in &evaluation.recommendations {
            println!("  - {}", recommendation.message());
        }
    }
}

pub fn print_research_candidate_testnet_review_dossier(
    dossier: &ResearchCandidateTestnetReviewDossier,
) {
    println!("Status: {}", dossier.status.as_str());
    println!("Candidate: {}", dossier.candidate_id);
    println!(
        "Scope: {} {} {}",
        dossier.strategy_id, dossier.symbol, dossier.timeframe
    );
    println!(
        "Candidate status: {}",
        dossier
            .candidate_status
            .map(|value| value.as_str())
            .unwrap_or("UNKNOWN")
    );
    println!(
        "Latest review action: {}",
        dossier
            .evidence
            .latest_review_action
            .as_ref()
            .map(|value| value.action.as_str())
            .unwrap_or("NONE")
    );
    println!(
        "Qualification: {}  Trend: {}",
        dossier
            .evidence
            .latest_qualification_evaluation
            .as_ref()
            .map(|value| value.status.as_str())
            .unwrap_or("UNKNOWN"),
        dossier.evidence.qualification_trend.as_str()
    );
    if let Some(performance) = &dossier.evidence.shadow_performance_summary {
        println!(
            "Shadow summary: runs={} would_submit={} risk_rejected={} skipped={} error={}",
            performance.total_shadow_runs,
            performance.would_submit_count,
            performance.risk_rejected_count,
            performance.skipped_count,
            performance.error_count
        );
    } else {
        println!("Shadow summary: none");
    }
    if let Some(walk_forward) = &dossier.evidence.walk_forward_evidence {
        println!(
            "Walk-forward: run={} robustness={} avg_pnl_pct={} consistency={}",
            walk_forward.walk_forward_run_id,
            walk_forward.robustness_status.as_str(),
            walk_forward.avg_pnl_pct,
            walk_forward.consistency_score
        );
    } else {
        println!("Walk-forward: none");
    }
    if dossier.blockers.is_empty() {
        println!("Blockers: none");
    } else {
        println!("Blockers:");
        for blocker in &dossier.blockers {
            println!("  - {blocker}");
        }
    }
    if dossier.warnings.is_empty() {
        println!("Warnings: none");
    } else {
        println!("Warnings:");
        for warning in &dossier.warnings {
            println!("  - {warning}");
        }
    }
    println!("Checklist:");
    for item in &dossier.checklist {
        println!(
            "  - [{}] {}: {}",
            if item.passed { "x" } else { " " },
            item.name,
            item.summary
        );
    }
    println!("Recommendations:");
    for recommendation in &dossier.recommendations {
        println!("  - {}", recommendation.message());
    }
    println!("This does not submit orders.");
}

pub fn print_research_candidate_qualification_history(
    history: &ResearchCandidateQualificationHistory,
) {
    println!(
        "Candidate: {}  Evaluations: {}  Trend: {}",
        history.candidate_id,
        history.evaluations.len(),
        history.latest_trend.as_str()
    );
    if let Some(change) = &history.latest_change {
        println!(
            "Latest change: {} -> {}  score {} -> {}",
            change
                .previous_status
                .map(|value| value.as_str())
                .unwrap_or("UNKNOWN"),
            change.current_status.as_str(),
            change
                .previous_score
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            change.current_score
        );
    }
    for evaluation in &history.evaluations {
        println!(
            "{}  status={} score={} readiness={} runs={} would_submit={}",
            evaluation.evaluated_at.to_rfc3339(),
            evaluation.status.as_str(),
            evaluation.score,
            evaluation
                .latest_readiness_status
                .map(|value| value.as_str())
                .unwrap_or("UNKNOWN"),
            evaluation.total_shadow_runs,
            evaluation.would_submit_count
        );
    }
}

pub fn print_research_candidate_watchlist(watchlist: &[ResearchCandidateWatchlistEntry]) {
    if watchlist.is_empty() {
        println!("No watchlist entries found.");
        return;
    }

    for entry in watchlist {
        let latest = entry.latest_evaluation.as_ref();
        println!(
            "{} {} {}  candidate_status={} eval_status={} score={} trend={} watchlist={} last_evaluated={}",
            entry.strategy_id,
            entry.symbol,
            entry.timeframe,
            entry.candidate_status.as_str(),
            latest
                .map(|value| value.status.as_str())
                .unwrap_or("UNKNOWN"),
            latest
                .map(|value| value.score.to_string())
                .unwrap_or_else(|| "-".to_string()),
            entry.trend.as_str(),
            entry.watchlist_status.as_str(),
            latest
                .map(|value| value.evaluated_at.to_rfc3339())
                .unwrap_or_else(|| "-".to_string())
        );
        if let Some(change) = &entry.latest_change {
            println!(
                "  previous={} previous_score={} delta={}",
                change
                    .previous_status
                    .map(|value| value.as_str())
                    .unwrap_or("UNKNOWN"),
                change
                    .previous_score
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                change.score_delta
            );
        }
    }
}

pub fn print_research_candidate_shadow_runs(runs: &[ResearchCandidateShadowRunLink]) {
    if runs.is_empty() {
        println!("No linked shadow runs found.");
        return;
    }

    for run in runs {
        println!(
            "{}  run={} decision={} status={} linked_at={}",
            run.shadow_created_at.to_rfc3339(),
            run.shadow_run_id,
            run.decision,
            run.status,
            run.linked_at.to_rfc3339()
        );
    }
}

pub fn print_research_candidate_shadow_promotion_preview(
    preview: &ResearchCandidateShadowPromotionPreview,
) {
    println!("Candidate ID: {}", preview.candidate_id);
    println!(
        "Target: {} {} {}",
        preview.strategy_id, preview.symbol, preview.timeframe
    );
    println!("Candidate status: {}", preview.candidate_status.as_str());
    println!("Mode: {}", preview.mode.as_str());
    println!("Status: {}", preview.status.as_str());
    println!("Recommendation: {}", preview.recommendation);
    println!(
        "Confirmation required: {}",
        if preview.confirmation_required {
            "yes"
        } else {
            "no"
        }
    );
    println!("Correlation ID: {}", preview.correlation_id);
    println!(
        "Allow add missing runner alignment: {}",
        preview.allow_missing_runner_alignment
    );
    println!(
        "Current runner config: {}",
        serde_json::to_string_pretty(&preview.current_runner_config)
            .unwrap_or_else(|_| "{}".to_string())
    );
    println!(
        "Proposed runner config: {}",
        serde_json::to_string_pretty(&preview.proposed_runner_config)
            .unwrap_or_else(|_| "{}".to_string())
    );
    println!(
        "Changes: {}",
        if preview.changes.is_empty() {
            "none".to_string()
        } else {
            preview.changes.join(" | ")
        }
    );
    println!(
        "Diff: {}",
        serde_json::to_string_pretty(&preview.diff).unwrap_or_else(|_| "{}".to_string())
    );
    println!(
        "Blockers: {}",
        if preview.blockers.is_empty() {
            "none".to_string()
        } else {
            preview.blockers.join(" | ")
        }
    );
    println!(
        "Warnings: {}",
        if preview.warnings.is_empty() {
            "none".to_string()
        } else {
            preview.warnings.join(" | ")
        }
    );
    println!(
        "Reasons: {}",
        if preview.reasons.is_empty() {
            "none".to_string()
        } else {
            preview.reasons.join(" | ")
        }
    );
    println!("This does not start the runner or create orders.");
}

pub fn print_research_candidate_accept_shadow_preview(
    preview: &ResearchCandidateAcceptForShadowPreviewResult,
) {
    println!("Candidate ID: {}", preview.candidate_id);
    println!(
        "Target: {} {} {}",
        preview.strategy_id, preview.symbol, preview.timeframe
    );
    println!("Current status: {}", preview.current_status.as_str());
    println!("Preview status: {}", preview.status.as_str());
    println!("Recommended action: {}", preview.recommended_action);
    println!("No mutation: {}", preview.no_mutation);
    println!(
        "Evidence: pnl_pct={} score={} trades={} data_quality={} walk_forward={} robustness_matrix={}",
        preview
            .evidence_summary
            .candidate_pnl_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "N/A".to_string()),
        preview
            .evidence_summary
            .candidate_score
            .map(|value| value.to_string())
            .unwrap_or_else(|| "N/A".to_string()),
        preview
            .evidence_summary
            .candidate_trade_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "N/A".to_string()),
        preview
            .evidence_summary
            .data_quality_status
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "N/A".to_string()),
        preview
            .evidence_summary
            .walk_forward_status
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "N/A".to_string()),
        preview
            .evidence_summary
            .robustness_matrix_status
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "N/A".to_string())
    );
    println!(
        "Runner alignment: {}",
        if preview.runner_alignment.strategy_config_matches_runner {
            "aligned"
        } else {
            "not aligned"
        }
    );
    if !preview.runner_alignment.mismatch_reasons.is_empty() {
        println!(
            "Runner mismatch: {}",
            preview.runner_alignment.mismatch_reasons.join(" | ")
        );
    }
    println!(
        "Required runner config changes: {}",
        if preview.required_runner_config_change.is_empty() {
            "none".to_string()
        } else {
            preview.required_runner_config_change.join(" | ")
        }
    );
    println!(
        "Blockers: {}",
        if preview.blockers.is_empty() {
            "none".to_string()
        } else {
            preview.blockers.join(" | ")
        }
    );
    println!(
        "Warnings: {}",
        if preview.warnings.is_empty() {
            "none".to_string()
        } else {
            preview.warnings.join(" | ")
        }
    );
    if !preview.checks.is_empty() {
        println!("Checks:");
        for check in &preview.checks {
            println!(
                "  {} passed={} blocking={} summary={}",
                check.code, check.passed, check.blocking, check.summary
            );
        }
    }
}

pub fn print_research_candidate_accept_shadow_apply(
    result: &ResearchCandidateAcceptForShadowApplyResult,
) {
    println!("Candidate ID: {}", result.candidate_id);
    println!(
        "Target: {} {} {}",
        result.strategy_id, result.symbol, result.timeframe
    );
    println!("Previous status: {}", result.previous_status.as_str());
    println!("New status: {}", result.new_status.as_str());
    println!("Warnings acknowledged: {}", result.warnings_acknowledged);
    println!("Lifecycle event ID: {}", result.lifecycle_event_id);
    println!(
        "Runner config unchanged: {}",
        result.runner_config_unchanged
    );
    println!("Shadow runs created: {}", result.shadow_runs_created);
    println!(
        "Execution tables mutated: {}",
        result.execution_tables_mutated
    );
    println!(
        "Recommended next action: {}",
        result.recommended_next_action
    );
    if !result.warnings.is_empty() {
        println!("Warnings: {}", result.warnings.join(" | "));
    }
}

pub fn print_research_candidate_shadow_promotion_result(
    result: &ResearchCandidateShadowPromotionResult,
) {
    println!("Candidate ID: {}", result.candidate_id);
    println!(
        "Target: {} {} {}",
        result.strategy_id, result.symbol, result.timeframe
    );
    println!("Candidate status: {}", result.candidate_status.as_str());
    println!("Mode: {}", result.mode.as_str());
    println!("Status: {}", result.status.as_str());
    println!("Applied: {}", result.applied);
    println!("Recommendation: {}", result.recommendation);
    println!(
        "Current runner config: {}",
        serde_json::to_string_pretty(&result.current_runner_config)
            .unwrap_or_else(|_| "{}".to_string())
    );
    println!(
        "Proposed runner config: {}",
        serde_json::to_string_pretty(&result.proposed_runner_config)
            .unwrap_or_else(|_| "{}".to_string())
    );
    println!(
        "Changes: {}",
        if result.changes.is_empty() {
            "none".to_string()
        } else {
            result.changes.join(" | ")
        }
    );
    println!(
        "Diff: {}",
        serde_json::to_string_pretty(&result.diff).unwrap_or_else(|_| "{}".to_string())
    );
    println!(
        "Blockers: {}",
        if result.blockers.is_empty() {
            "none".to_string()
        } else {
            result.blockers.join(" | ")
        }
    );
    println!(
        "Warnings: {}",
        if result.warnings.is_empty() {
            "none".to_string()
        } else {
            result.warnings.join(" | ")
        }
    );
    println!(
        "Reasons: {}",
        if result.reasons.is_empty() {
            "none".to_string()
        } else {
            result.reasons.join(" | ")
        }
    );
    println!("This does not start the runner or create orders.");
}

pub fn print_research_candidate_decision_rejection(
    rejection: &ResearchCandidateDecisionRejection,
    message: &str,
) {
    println!("Decision: rejected");
    println!("Reason: {}", rejection.reason_code);
    println!("Message: {}", message);
    println!("Recommendation: {}", rejection.recommendation);
    println!(
        "Last observed at: {}",
        rejection
            .last_observed_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Observation age: {}",
        rejection
            .observation_age_seconds
            .map(|value| format!("{value}s"))
            .unwrap_or_else(|| "-".to_string())
    );
}

pub fn print_strategy_performance_summary(response: &StrategyPerformanceSummaryResponse) {
    let summary = &response.summary;
    println!(
        "Mode: {}  Strategy: {}  Symbol: {}  Timeframe: {}",
        summary.mode.as_str(),
        summary.strategy_id.as_deref().unwrap_or("ALL"),
        summary.symbol.as_deref().unwrap_or("ALL"),
        summary.timeframe.as_deref().unwrap_or("ALL")
    );
    println!("Window: {} -> {}", summary.window_start, summary.window_end);
    println!(
        "Runs: {}  Signals: {}  Approved risk: {}  Rejected risk: {}  Rejection rate: {}",
        summary.total_runs,
        summary.total_signals,
        summary.approved_risk_decisions,
        summary.rejected_risk_decisions,
        summary.risk_rejection_rate
    );
    println!(
        "Shadow would-submit: {}  No-signal: {}  Shadow risk-rejected: {}",
        summary.shadow_would_submit_count,
        summary.shadow_no_signal_count,
        summary.shadow_risk_rejected_count
    );
    println!(
        "Paper orders: {}  Opened: {}  Closed: {}",
        summary.paper_orders_count, summary.paper_positions_opened, summary.paper_positions_closed
    );
    println!(
        "Realized PnL: {}  Unrealized PnL: {}  Win rate: {}",
        summary.realized_pnl,
        summary.unrealized_pnl,
        summary
            .win_rate
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Backtests: {}  Best: {}  Worst: {}  Avg: {}",
        summary.backtest_runs_count,
        summary
            .best_backtest_pnl_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        summary
            .worst_backtest_pnl_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        summary
            .avg_backtest_pnl_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
}

pub fn print_strategy_performance_rankings(response: &StrategyPerformanceRankingsResponse) {
    for ranking in &response.rankings {
        println!(
            "{} mode={} realized={} would_submit={} rejected={} backtest_avg={}",
            ranking.strategy_id,
            ranking.mode.as_str(),
            ranking.realized_pnl,
            ranking.shadow_would_submit_count,
            ranking.rejected_risk_decisions,
            ranking
                .avg_backtest_pnl_pct
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

pub fn print_strategy_decision_breakdown(response: &StrategyDecisionBreakdownResponse) {
    let breakdown = &response.breakdown;
    println!(
        "Strategy: {}  Symbol: {}  Timeframe: {}",
        breakdown.strategy_id,
        breakdown.symbol.as_deref().unwrap_or("ALL"),
        breakdown.timeframe.as_deref().unwrap_or("ALL")
    );
    println!(
        "Window: {} -> {}",
        breakdown.window_start, breakdown.window_end
    );
    println!(
        "Runs: {}  Would-submit: {}  No-signal: {}  Risk-rejected: {}  Skipped: {}  Error: {}",
        breakdown.total_runs,
        breakdown.would_submit_count,
        breakdown.no_signal_count,
        breakdown.risk_rejected_count,
        breakdown.skipped_count,
        breakdown.error_count
    );
}

pub fn print_testnet_promotion_funnel_summary(response: &TestnetPromotionFunnelSummaryResponse) {
    let summary = &response.summary;
    println!(
        "Strategy: {}  Symbol: {}  Timeframe: {}",
        summary.strategy_id.as_deref().unwrap_or("ALL"),
        summary.symbol.as_deref().unwrap_or("ALL"),
        summary.timeframe.as_deref().unwrap_or("ALL")
    );
    println!(
        "Window: {} -> {}",
        summary
            .window_start
            .map(|value| value.to_string())
            .unwrap_or_else(|| "ALL".to_string()),
        summary
            .window_end
            .map(|value| value.to_string())
            .unwrap_or_else(|| "ALL".to_string())
    );
    println!(
        "Shadow: {}  Previewed: {}  Submitted: {}  Orders: {}  Acked: {}  Filled: {}",
        summary.shadow_would_submit_count,
        summary.promotion_previewed_count,
        summary.promotion_submitted_count,
        summary.testnet_orders_created_count,
        summary.acked_count,
        summary.filled_count
    );
    println!(
        "Rejected promos: {}  Expired promos: {}  Duplicate rejected: {}",
        summary.promotion_rejected_count,
        summary.promotion_expired_count,
        summary.promotion_duplicate_rejected_count
    );
    println!(
        "Cancelled: {}  Rejected orders: {}  Expired orders: {}  Reconciliation required: {}  Unknown: {}  Failed: {}",
        summary.cancelled_count,
        summary.rejected_count,
        summary.expired_count,
        summary.reconciliation_required_count,
        summary.unknown_exchange_state_count,
        summary.failed_count
    );
    println!(
        "Preview rate: {}%  Submit rate: {}%  Ack rate: {}%  Fill rate: {}%  Reconciliation required rate: {}%",
        summary.preview_rate_pct,
        summary.submit_rate_pct,
        summary.ack_rate_pct,
        summary.fill_rate_pct,
        summary.reconciliation_required_rate_pct
    );
}

pub fn print_testnet_promotion_outcomes(response: &TestnetPromotionFunnelOutcomesResponse) {
    println!("Outcomes:");
    for outcome in &response.outcomes {
        println!(
            "{} count={} rate={}%",
            outcome.outcome, outcome.count, outcome.rate_pct
        );
    }
    println!("Lifecycle:");
    for item in &response.lifecycle {
        println!(
            "{} count={} rate={}%",
            item.execution_state, item.count, item.rate_pct
        );
    }
}

pub fn print_testnet_promotion_rows(response: &TestnetPromotionFunnelRowsResponse) {
    for row in &response.rows {
        println!(
            "{} promotion={} strategy={} symbol={} status={} client_order_id={} execution_state={} previewed_at={} submitted_at={}",
            row.shadow_run_id,
            row.promotion_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.strategy_id,
            row.symbol,
            row.promotion_status.as_deref().unwrap_or("-"),
            row.client_order_id.as_deref().unwrap_or("-"),
            row.execution_state
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.promotion_created_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.submitted_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

fn summarize_feeds(feed: &FeedStatusResponse) -> String {
    if feed.feeds.is_empty() {
        return "none".to_string();
    }

    feed.feeds
        .iter()
        .map(|item| format!("{}:{}/{}", item.symbol, item.status, item.freshness_status))
        .collect::<Vec<_>>()
        .join(", ")
}

fn bool_word(value: bool) -> String {
    if value {
        "yes".green().to_string()
    } else {
        "no".red().to_string()
    }
}

fn paint_state(value: &str, ok: bool) -> String {
    if ok {
        value.green().bold().to_string()
    } else {
        value.red().bold().to_string()
    }
}

fn paint_order_status(value: &str) -> String {
    if value.eq_ignore_ascii_case("rejected") || value.eq_ignore_ascii_case("cancelled") {
        value.red().bold().to_string()
    } else {
        value.to_string()
    }
}

fn display_option<T: ToString>(value: Option<T>) -> String {
    value
        .map(|inner| inner.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn display_vec(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}

fn pipeline_decision_label(result: &PaperTradingPipelineResult) -> &'static str {
    match result.pipeline_decision {
        aegis_core::PipelineDecision::NoSignal => "NO_SIGNAL",
        aegis_core::PipelineDecision::RiskRejected => "RISK_REJECTED",
        aegis_core::PipelineDecision::PaperOrderCreated => "PAPER_ORDER_CREATED",
        aegis_core::PipelineDecision::PaperOrderReused => "PAPER_ORDER_REUSED",
        aegis_core::PipelineDecision::StrategyDisabled => "STRATEGY_DISABLED",
        aegis_core::PipelineDecision::SafetyStopped => "SAFETY_STOPPED",
    }
}
