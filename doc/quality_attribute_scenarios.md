# Quality Attribute Scenarios

Both service-level scenarios are measured by the api-gateway through Prometheus counters exposed on
port 9401. Health probes (`/api/v1/health`, `/api/v1/health/live`) are excluded from both SLIs via an
early return in `observeRequest`, so the indicators count domain requests only (domain-driven
definition). A request is counted as successful when its final status code is `< 400`.


**Quality attribute scenario**: Throughput (SLO-2 / SLI-2)

**Feature**: Sustained request rate under load

_when_ a batch of 2000 domain requests is issued to the gateway \
_caused by_ concurrent clients \
_occur in_ the system \
_operating in_ normal operation \
_then_ the gateway processes all of them and exposes the delta of `rest_requests` over the load window \
_so that_ the measured throughput (SLI-2 = requests processed per second) stays above 40 req/s (SLO-2)


**Quality attribute scenario**: Availability (SLO-1 / SLI-1)

**Feature**: High rate of successful requests over time

_when_ a batch of domain requests is issued to the gateway \
_caused by_ clients performing valid operations \
_occur in_ the system \
_operating in_ normal operation \
_then_ the gateway records `successful_rest_requests` (final status `< 400`) over `rest_requests` \
_so that_ the measured availability (SLI-1 = successful / total requests) stays above 99% over 30 days (SLO-1)


**Quality attribute scenario**: Handling partial failures

**Feature**: Avoid pointless requests in case of unavailability of account service

_when_ more than 50% of total requests fail \
_caused by_ unavailability of account service \
_occur in_ the system \
_operating in_ normal operation \
_then_ the system makes the requests fail immediately without propagating to the account service \
_so that_ it lightens the workload avoiding pointless requests