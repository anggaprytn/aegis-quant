# Security Policy

## Supported scope

The supported security scope is the current repository and its default local/containerized deployment shape. Aegis Quant is experimental infrastructure and is not a hosted service. Only the current main development line is treated as supported.

The most important boundaries are:

- live trading and production exchange private trading endpoints are not implemented;
- public Binance market-data access is separate from authenticated Spot Testnet actions;
- paper, shadow, testnet, research, and backtest state are isolated by design;
- the kill switch, role checks, typed confirmations, and audit/event records protect dangerous paths;
- testnet credentials, when used, belong only in backend environment variables and must never be exposed to the dashboard or CLI.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use the repository host's private vulnerability-reporting feature when it is available. If it is not enabled, contact a repository maintainer privately through the hosting service before disclosing details publicly.

Include:

- affected commit or release;
- a concise description of the impact;
- reproducible steps or a minimal proof of concept;
- affected route, command, worker, or migration;
- any mitigations you have already tried.

Redact passwords, tokens, cookies, API keys, private URLs, account identifiers, and production data. Do not send real exchange credentials.

## Disclosure

Maintainers will acknowledge a report when practical, investigate it, and coordinate a fix or mitigation before public disclosure when the issue warrants private handling. There is no security bounty or guaranteed response time.

For implementation details and operator precautions, see [security model](SECURITY.md) and [security checklist](SECURITY_CHECKLIST.md).
