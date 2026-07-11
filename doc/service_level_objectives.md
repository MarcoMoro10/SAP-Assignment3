# Service Level Objectives (SLO)

The following objectives are defined for the **delivery-service** of the
"Shipping on the Air" system, measured over a rolling 30-day window.

- The service should have an **availability** greater than **99%** over 30 days.
- The service should sustain a **throughput** greater than **40 requests/second** over 30 days.

# Service Level Indicators (SLI)

- **Availability**: percentage of successful requests over total requests.
  A request is considered *successful* when its HTTP status is `< 400`
  (health/liveness probes are excluded from the count).
- **Throughput**: number of requests processed per second.

# Measurement


| Metric (exposed name)                 | Type    | Meaning                                                    |
|---------------------------------------|---------|------------------------------------------------------------|
| `rest_requests_total`                 | counter | Total domain REST requests received (probes excluded)      |
| `successful_rest_requests_total`      | counter | Requests answered with status `< 400`                      |
| `request_response_time_seconds_total` | counter | Accumulated request/response time, in seconds              |

## PromQL

**Availability** (successful over total, 30-day window):

```promql
sum(increase(successful_rest_requests_total[30d]))
/
sum(increase(rest_requests_total[30d]))
```

**Throughput** (requests per second, 30-day average):

```promql
sum(rate(rest_requests_total[30d]))
```
