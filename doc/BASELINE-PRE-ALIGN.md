# Baseline pre-allineamento (STEP 1)

**Scopo.** Fotografia oggettiva dello stato dei test di A3 **prima** di qualunque
modifica dell'allineamento ad A2, così da poter distinguere le rotture *nuove*
da quelle *preesistenti*. Questo step **non modifica codice di produzione**.

- **Repo:** `MarcoMoro10/SAP-Assignment3`
- **Branch:** `align-a2` (creato da HEAD di `main` = `1d692e4` "Add SLI and SLO documentation, ...")
- **Data esecuzione:** 2026-07-19
- **Ambiente:** Windows 11, JDK 25 (Temurin 25+37), Apache Maven 3.9.12,
  Docker 29.5.3 / Docker Compose v5.1.4 (Desktop, backend WSL2).

---

## 1. Conteggio file di test per modulo (conferma della misura fornita)

Misurato con `Glob` su `src/test/**`. **Confermato: coincide esattamente** con
la misura indicata nel piano.

| Modulo | file `.java` di test | file `.feature` |
|---|---|---|
| account-service | 10 | 1 |
| api-gateway | 14 | 0 |
| delivery-service | 23 | 0 |
| system-end-to-end | 10 | 3 |

> Nota: il conteggio `.java` include le classi di supporto (fake/stub, `KafkaTestSupport`,
> step-definition Cucumber, ecc.), non solo le classi con metodi `@Test`. È il numero
> di *file*, coerente con la misura del piano.

---

## 2. `mvn -q test` per modulo — esecuzione **locale** (senza broker Kafka)

Eseguito in locale, dove **non** c'è un broker Kafka raggiungibile su `localhost:29092`.
`KafkaTestSupport.assumeBrokerReachable()` usa `Assumptions.abort()` di JUnit: gli
integration test che richiedono un broker vengono quindi **saltati/non eseguiti**
(comportamento *by-design*, non un fallimento e non flakiness). Conteggi ricavati
dai report Surefire (`target/surefire-reports/*.txt`).

| Modulo | Tests run | Failures | Errors | Skipped | Esito `mvn` |
|---|---|---|---|---|---|
| account-service | 25 | 0 | 0 | 0 | BUILD SUCCESS (exit 0) |
| api-gateway | 29 | 0 | 0 | 0 | BUILD SUCCESS (exit 0) |
| delivery-service | 55 | 0 | 0 | 1 | BUILD SUCCESS (exit 0) |

**Classi gated dal broker (eseguono 0 test in locale, per assunzione abortita):**

- api-gateway → `DeliveryServiceProxyIntegrationTest` (0 test eseguiti).
- delivery-service → `ArrivalTerminalFrameIntegrationTest`, `DeliveryServiceHealthTest`,
  `TrackingSocketCloseIntegrationTest` (0 test eseguiti ciascuno).
- delivery-service → `ScheduledDeliveryTrackingIntegrationTest` registra 1 test **Skipped**
  (anch'esso gated dal broker; viene eseguito quando il broker è presente — vedi §3).

Queste omissioni **non** sono rosse né flaky: sono lo stesso codice che gira verde
sotto i compose di test dedicati (§3), dove il broker è disponibile.

### Dettaglio per classe (locale)

**account-service** (25):
`ArchitectureTests` 2 · `AccountServiceImplTest` 7 · `RunAccountComponentTest` 6 (Cucumber, 6 scenari) ·
`AccountDomainTest` 8 · `AccountServiceHealthTest` 1 · `FileBasedAccountRepositoryTest` 1.

**api-gateway** (29):
`ArchitectureTests` 2 · `SessionServiceImplTest` 8 · `AccountServiceCircuitBreakerTest` 2 ·
`AdminRoleAuthorizationIntegrationTest` 1 · `CircuitBreakerTest` 6 · `EnvTest` 3 ·
`GatewayHealthIntegrationTest` 3 · `GatewayRegistrationIntegrationTest` 3 ·
`PrometheusGatewayMetricsTest` 1 · `DeliveryServiceProxyIntegrationTest` 0 *(broker-gated)*.

**delivery-service** (55, +1 skip):
`ArchitectureTests` 2 · `DeliveryServiceImplTest` 10 · `DeliveryValueObjectsTest` 6 ·
`DeliveryAggregateTest` 8 · `DeliveryReplayTest` 3 · `DroneAgentTest` 2 · `DroneTest` 9 ·
`CancelDuringSchedulingIntegrationTest` 1 · `EventSourcedDeliveryRepositoryTest` 2 ·
`LiveEtrReadModelTest` 1 · `PrometheusDeliveryMetricsTest` 1 ·
`ScheduledDeliveryTrackingIntegrationTest` 1 (1 skip) · `SchedulerVerticleIntegrationTest` 1 ·
`FleetModuleTest` 3 · `ReservationLifecycleTest` 5 ·
`ArrivalTerminalFrameIntegrationTest` 0 · `DeliveryServiceHealthTest` 0 ·
`TrackingSocketCloseIntegrationTest` 0 *(le ultime tre broker-gated)*.

---

## 3. Compose di test dedicati (broker Kafka reale)

Comando (per ciascun file):
`docker compose -f <file> up --build --abort-on-container-exit --exit-code-from <servizio-test>`.
Con il broker raggiungibile, **tutti** gli integration test gated in §2 vengono eseguiti.

| Compose | Servizio test | Tests run | Failures | Errors | Skipped | Build | Exit |
|---|---|---|---|---|---|---|---|
| `docker-compose-test-delivery.yml` | `delivery-service-test` | 58 | 0 | 0 | 0 | SUCCESS | 0 |
| `docker-compose-test-api-gateway.yml` | `api-gateway-test` | 35 | 0 | 0 | 0 | SUCCESS | 0 |

- delivery: **58** vs 55 locali (i test broker-gated e lo skip ora vengono eseguiti; 0 skip).
- api-gateway: **35** vs 29 locali (`DeliveryServiceProxyIntegrationTest` ora esegue i suoi test).

Entrambi i container Maven terminano con `BUILD SUCCESS` ed exit code 0.

---

## 4. Sistema completo + `system-end-to-end`

Il modulo e2e auto-orchestra lo stack: `Setup.ensureSystemUp()` esegue
`docker compose build` + `docker compose up --detach` dalla root, attende la
`/api/v1/health/live` del gateway, poi esegue i test e a fine JVM fa
`docker compose down`. Eseguito con `mvn -q test` in `system-end-to-end`.

| Test | Tests run | Failures | Errors | Skipped | Tempo |
|---|---|---|---|---|---|
| `AvailabilityTest` | 1 | 0 | 0 | 0 | ~82 s |
| `CircuitBreakerTest` | 1 | 0 | 0 | 0 | ~59 s |
| `PerformanceTest` | 1 | 0 | 0 | 0 | ~3 s |
| Cucumber (`RunCucumberTest` + engine) | 6 | 0 | 0 | 0 | ~31 s |
| **Totale** | **9** | **0** | **0** | **0** | — |

- I 3 user-journey (creazione, tracking, cancellazione) come feature Cucumber: **tutti verdi**.
- Lo stack è salito *healthy* entro il timeout ed è stato smontato correttamente (`docker compose down`).
- Esito complessivo `mvn`: exit 0.

> Le 6 esecuzioni Cucumber corrispondono agli scenari dei 3 file `.feature`
> (creazione consegna, tracking+stop e tracking di schedulata, cancellazione).

---

## 5. Test già rossi o flaky PRIMA delle modifiche

**Nessun test rosso. Nessuna flakiness osservata.**

- Tutti i moduli: `BUILD SUCCESS`, 0 failures / 0 errors ovunque.
- L'unico `Skipped` locale (delivery `ScheduledDeliveryTrackingIntegrationTest`) e le
  classi "0 test" in §2 sono **gated dal broker Kafka** via `Assumptions.abort()`:
  comportamento deliberato per l'esecuzione locale, non rosso e non flaky. Le stesse
  classi girano verdi nei compose di test dedicati (§3).
- Nessuna ri-esecuzione ha prodotto esiti diversi entro questa sessione.

### Sintesi baseline (numeri di riferimento da non peggiorare)

| Contesto | Run | Fail | Err | Skip |
|---|---|---|---|---|
| account-service (locale) | 25 | 0 | 0 | 0 |
| api-gateway (locale) | 29 | 0 | 0 | 0 |
| delivery-service (locale) | 55 | 0 | 0 | 1 |
| delivery-service (compose broker) | 58 | 0 | 0 | 0 |
| api-gateway (compose broker) | 35 | 0 | 0 | 0 |
| system-end-to-end | 9 | 0 | 0 | 0 |

**Criterio di uscita STEP 1 soddisfatto:** esiste questo documento con i numeri per
modulo, i conteggi file confermati, e l'annotazione esplicita che non ci sono test
preesistenti rossi o flaky.
