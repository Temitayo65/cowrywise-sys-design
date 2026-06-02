# cowrywise-sys-design

# Tech Spec: Notification System (Push / SMS / Email) for 1M+ Users

**Author:** Temitayo Ilori
**Date:** May 2026
**Status:** Draft for review

---

## 1. Goals and non-goals

**Goals**
- Deliver push, SMS, and email notifications to 1M+ users.
- Guarantee no duplicate sends and no missed sends for transactional traffic (OTPs, withdrawal confirmations, KYC status).
- Degrade gracefully when any single provider (FCM, Termii, SendGrid, etc.) fails.
- Support both immediate (event-driven) and scheduled (campaign) sends.
- Observable end-to-end: every notification has a traceable lifecycle.

**Non-goals**
- Real-time chat (use a separate service).
- Marketing segmentation/A-B testing tooling (a downstream concern that consumes this service).
- Rich templating UI for non-engineers (v2).

---

## 2. Traffic assumptions

- 1M active users.
- Peak: ~50 transactional notifications/sec (OTPs at 09:00 + portfolio statements + trade confirmations).
- Burst: a single market-event push to all 1M users in <5 minutes = ~3,300/sec sustained.
- Read-to-write ratio is irrelevant; this is write-heavy.

---

## 3. High-level architecture

```
+----------------+        +-------------------+        +------------------+
|  App services  |--HTTP->|  Notification API |--SQS-->|  Channel workers |
| (auth, trades, |        |   (FastAPI/Django)|        | (push/sms/email) |
|  portfolios)   |        +-------------------+        +------------------+
+----------------+                 |                            |
                                   v                            v
                            +-------------+              +-----------+
                            | Postgres    |              | Providers |
                            | notif state |              | FCM/APNs  |
                            |             |              | Termii/   |
                            |             |              |   Twilio  |
                            |             |              | SES/      |
                            |             |              |   SendGrid|
                            +-------------+              +-----------+
                                   ^                            |
                                   |        webhooks            |
                                   +----------------------------+
```

**Three logical layers:**
1. **Ingress (Notification API)** — accepts notification requests, validates, deduplicates, persists, enqueues.
2. **Dispatch (Channel workers)** — Celery workers (or AWS Lambda triggered from SQS) per channel, talk to providers.
3. **Feedback (Webhook receiver)** — ingests delivery receipts and bounces, updates state.

---

## 4. Data model

```sql
CREATE TABLE notifications (
    id              UUID PRIMARY KEY,
    user_id         UUID NOT NULL,
    idempotency_key TEXT NOT NULL,         -- supplied by caller
    channel         TEXT NOT NULL,         -- 'push' | 'sms' | 'email'
    template_id     TEXT NOT NULL,
    payload         JSONB NOT NULL,        -- template variables
    priority        SMALLINT NOT NULL,     -- 0 = transactional, 1 = important, 2 = bulk
    status          TEXT NOT NULL,         -- 'queued','sending','sent','delivered','failed','suppressed'
    provider        TEXT,                  -- chosen at dispatch time
    provider_msg_id TEXT,                  -- returned by provider
    attempts        SMALLINT DEFAULT 0,
    last_error      TEXT,
    scheduled_for   TIMESTAMPTZ,           -- NULL = immediate
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, idempotency_key, channel)
);

CREATE INDEX idx_notif_status_scheduled ON notifications (status, scheduled_for)
    WHERE status IN ('queued','sending');
CREATE INDEX idx_notif_user_created ON notifications (user_id, created_at DESC);

CREATE TABLE user_channel_preferences (
    user_id     UUID NOT NULL,
    channel     TEXT NOT NULL,
    enabled     BOOLEAN DEFAULT true,
    quiet_hours JSONB,                     -- {start: "22:00", end: "07:00", tz: "Africa/Lagos"}
    PRIMARY KEY (user_id, channel)
);

CREATE TABLE provider_health (
    provider     TEXT PRIMARY KEY,
    channel      TEXT NOT NULL,
    state        TEXT NOT NULL,            -- 'healthy','degraded','open' (circuit breaker)
    failure_rate NUMERIC,
    last_checked TIMESTAMPTZ
);
```

The `UNIQUE (user_id, idempotency_key, channel)` constraint is the **no-duplicates guarantee** at the persistence layer. Two app-service retries with the same idempotency key collide on insert; the second returns the first's notification ID.

---

## 5. End-to-end flow (single notification)

1. App service calls `POST /v1/notifications` with `{user_id, channel, template_id, payload, idempotency_key, priority}`.
2. API validates input, checks user preferences and suppression list, runs `INSERT ... ON CONFLICT DO NOTHING RETURNING *`. If conflict, return the existing notification.
3. API enqueues a job onto the appropriate SQS queue (`notifications-push`, `notifications-sms`, `notifications-email`). Queue choice by priority: `transactional` and `bulk` queues per channel so a campaign doesn't starve OTPs.
4. Channel worker pulls the message, sets status to `sending`, picks a healthy provider, calls it.
5. Provider returns `provider_msg_id` → worker writes `sent`, `provider`, `provider_msg_id`.
6. Provider webhook arrives → webhook handler maps `provider_msg_id` to our row, sets `delivered` or `failed`.
7. On worker failure: SQS visibility timeout expires, message is redelivered. Worker checks `attempts` to skip work it already completed (status-based idempotency).
8. After max attempts (e.g. 5 with exponential backoff: 30s, 2m, 10m, 1h, 6h), message goes to a dead-letter queue and an alert fires.

---

## 6. The "no duplicates" guarantee

Three layers of defence:

1. **Ingress dedup** — unique constraint on `(user_id, idempotency_key, channel)`. App services must supply idempotency keys; for event-driven sends, key = `f"{event_type}:{event_id}:{channel}"`.
2. **Worker dedup** — worker reads current row state before calling the provider. If status is already `sent` or `delivered`, it ACKs the SQS message without sending. This handles the case where the worker crashed *after* the provider call but *before* the DB write — on redelivery, we still re-send (because we don't know if the original landed). To prevent that, we use a short-lived Redis lock `notif:{id}` around the provider call and write the `sent` status before releasing.
3. **Provider-side dedup** — for providers that support it (FCM, SES), we send our notification ID as the client message ID. Providers reject duplicates.

The honest reality: with at-least-once delivery semantics underneath, "no duplicates" is best-effort. The combination above pushes the duplicate rate to near zero for the cost of a Redis call per send.

---

## 7. The "no missed sends" guarantee

1. **Durable persistence first, queue second.** A notification is only acknowledged to the calling app service *after* the DB row is committed. SQS enqueue happens after commit. If enqueue fails, a background reaper job scans `status='queued' AND scheduled_for < now() - INTERVAL '1 minute'` every minute and re-enqueues.
2. **SQS guarantees** at-least-once delivery; with a DLQ and alerts on DLQ depth > 0, we surface anything stuck.
3. **Reconciliation job** every 6h: for `status='sent'` rows older than 24h with no delivery webhook, query the provider's status API and resolve. Catches lost webhooks.
4. **Outbox pattern** for the most critical sends (OTPs, withdrawal confirmations): the app service writes the notification row in the *same DB transaction* as the business event (e.g. the withdrawal transaction). A separate process tails the outbox and enqueues. Eliminates the "we committed the business event but failed to enqueue the notification" failure mode.

---

## 8. Graceful degradation when providers fail

**Per-channel provider pool with a circuit breaker.**

```
push:  [FCM (primary), APNs (iOS only, primary)]
sms:   [Termii (primary), Twilio (failover), AWS SNS (failover)]
email: [SendGrid (primary), AWS SES (failover)]
```

**Mechanism:**
- Each worker reads `provider_health` (cached in Redis, 30s TTL) before dispatching.
- A rolling-window failure-rate calculator (last 100 calls per provider, kept in Redis) updates after each call.
- If failure rate > 20% over 100 calls → state goes to `degraded`, traffic shifts to failover at 50%.
- If failure rate > 50% over 100 calls → circuit `open`, all traffic shifts to failover. A health-check goroutine probes the open provider every 60s; two consecutive successes → `healthy`.
- If *all* providers in a channel are open: messages sit in `queued`. Don't drop. An on-call alert fires.

**Provider-specific quirks worth noting:**
- SMS to Nigerian numbers via Termii is usually cheaper and faster than Twilio for local routes; failover order matters for cost.
- FCM and APNs are not interchangeable — iOS users must go through APNs (directly or via FCM relay). Routing is per-device-token.

---

## 9. Rate limiting and abuse

- Per-user, per-channel rate limit (e.g. 10 SMS/hour) enforced at ingress via Redis token bucket. Transactional priority bypasses with auditing.
- Global per-provider rate limit aligned to provider TOS, also Redis token bucket.
- Campaign sends declare a `dispatch_rate` (e.g. 500/sec) so they don't saturate transactional capacity.

---

## 10. Observability

- **Metrics** (CloudWatch / Prometheus): sends per channel per minute, p50/p95/p99 dispatch latency, failure rate per provider, DLQ depth, queue age.
- **Logs**: structured JSON, every log line carries `notification_id`, `user_id`, `channel`, `provider`. Sentry for exceptions.
- **Tracing**: OpenTelemetry from API → SQS → worker → provider call. Single trace per notification.
- **Dashboards**: one per channel showing the funnel (queued → sent → delivered → failed) with provider breakdown.
- **Alerts**: DLQ depth > 0, p95 latency > SLO, any provider in `open` state for > 5 min, reconciliation job finding > 100 missed sends in a run.

---

## 11. API surface

```
POST /v1/notifications
  body: { user_id, channel, template_id, payload, idempotency_key, priority?, scheduled_for? }
  returns: 201 { notification_id, status } on create
           200 { notification_id, status } on idempotent replay

GET /v1/notifications/{id}            -> current state
GET /v1/users/{user_id}/notifications -> paginated history

POST /v1/notifications/bulk           -> campaign endpoint
  body: { template_id, payload, audience_query, priority, dispatch_rate }

POST /v1/webhooks/{provider}          -> internal, signed
```

---

## 12. Deployment and capacity

- API: containerised FastAPI behind ALB, autoscaled on CPU + request count. 2–10 replicas.
- Workers: ECS/EKS, autoscaled on SQS queue depth. Separate worker groups per channel to isolate blast radius.
- Postgres: RDS Multi-AZ, read replica for analytics queries. Partition `notifications` by `created_at` (monthly) at >50M rows.
- Redis: ElastiCache cluster mode, used for rate limits, dedup locks, provider health cache.
- SQS: 6 queues (3 channels × 2 priorities) + 6 DLQs.

---

## 13. Open questions and v2

- WhatsApp Business as a fourth channel (Nigerian context — high reach).
- In-app inbox (durable, user-visible feed of all past notifications).
- ML-driven send-time optimisation per user (out of scope for v1, but the schema supports it via `scheduled_for`).
- Cost optimisation: route low-priority SMS via cheaper local aggregators after-hours.

---

## 14. Rollout plan

1. Week 1–2: API + Postgres schema + push channel only, behind feature flag, 1% of traffic.
2. Week 3: SMS channel, dual-write old + new system for OTPs, compare delivery rates.
3. Week 4: Email channel, decommission old paths gradually.
4. Week 5: Campaign endpoint, internal-only.
5. Week 6: 100% traffic, old system off.
