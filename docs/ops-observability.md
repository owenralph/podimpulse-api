# Operations Observability

## Request-Level Metrics

`function_app.py` now emits a request metric log for every route:

- `route`
- `method`
- `status`
- `duration_ms`
- `request_id`

Log pattern:

`[metric] request route=<route> method=<method> status=<status> duration_ms=<ms> request_id=<id>`

## Retry and External Timeout Monitoring

`utils/retry.py` now emits:

- `[metric] retry.success` with operation name, attempts, and elapsed time.
- `[metric] retry.attempt_failed` per retry attempt.
- `[metric] retry.exhausted` when retries are fully exhausted.

External HTTP calls include per-call metrics and hard timeouts:

- CSV fetch in ingest: operation `ingest.csv_fetch`, timeout `10s`.
- Facebook token/pages/analytics: operations
  `facebook.exchange_user_token`,
  `facebook.get_page_token`,
  `facebook.get_user_pages`,
  `facebook.query_page_analytics`, timeout `10s`.

## Recommended Alerts (Application Insights / Log Analytics)

Use these KQL queries and set alert rules:

1. Error rate > 5% over 5 minutes

```kusto
traces
| where message startswith "[metric] request "
| where timestamp > ago(5m)
| extend status = toint(extract(@"status=(\d+)", 1, message))
| summarize total=count(), errors=countif(status >= 500)
| extend error_rate = todouble(errors) / iif(total == 0, 1, total)
| where error_rate > 0.05
```

2. External call retries spiking

```kusto
traces
| where message startswith "[metric] retry.attempt_failed "
| where timestamp > ago(10m)
| summarize retries=count() by operation=tostring(extract(@"operation=([^ ]+)", 1, message))
| where retries >= 10
```

3. External call retry exhaustion

```kusto
traces
| where message startswith "[metric] retry.exhausted "
| where timestamp > ago(10m)
```
