ALTER TABLE scheduled_research_jobs
    DROP CONSTRAINT IF EXISTS scheduled_research_jobs_kind_safe;

ALTER TABLE scheduled_research_jobs
    ADD CONSTRAINT scheduled_research_jobs_kind_safe CHECK (
        kind IN (
            'PROVIDER_HEALTH',
            'MARKET_DATA_QUALITY',
            'AGGREGATION_STATUS',
            'CANDIDATE_SHADOW_OBSERVE_ONCE',
            'CROSS_ASSET_CANDIDATE_SHADOW_OBSERVE_ONCE',
            'RESEARCH_BATCH',
            'RESEARCH_CAMPAIGN',
            'REGIME_DISCOVERY',
            'ROBUSTNESS_MATRIX',
            'OPERATOR_REPORT'
        )
    );
