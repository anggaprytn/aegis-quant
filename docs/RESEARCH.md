# Research Workflows

## Regime Calibration Precedence

Regime discovery accepts both an inline `classifier_config` and a saved `calibration_id`.

The deterministic precedence rule is:

1. If `classifier_config` is present, discovery uses it.
2. If `classifier_config` is absent and `calibration_id` is present, discovery loads the saved calibration's `recommended_config`.
3. If neither is present, discovery uses the default classifier config.

This keeps ad hoc threshold experiments explicit while making persisted calibration reusable by ID.
