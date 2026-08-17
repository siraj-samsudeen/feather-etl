# Multi-client load monitoring — one central control plane, per-client collection

**Version:** 1.0
**Date:** 2026-08-17
**Status:** Accepted — binding on the source register, the control-plane schema, and every externally-namespaced asset name
**Issue:** [siraj-samsudeen/feather-flow#77](https://github.com/siraj-samsudeen/feather-flow/issues/77) — carries the full walkthrough, the n=3 evidence, and the options weighed

| Version | Changes |
|---|---|
| 1.0 | Initial version. Settles central-vs-isolated for load monitoring across clients, ahead of designing the source register. Adds **enrollment** as a distinct layer after the first four-layer cut missed it. |

---

## Purpose

Feather sells a **managed data layer**: we ingest a client's source systems, maintain and evolve the pipelines, and layer reporting and an MCP/LLM answering surface on top. Clients pay us to keep the pipelines running.

At 3 clients heading for ~50–100, with a monitoring team who **will not have the founder's tribal knowledge of which asset belongs to which client**, one axis has to be settled before anything else can be designed: is load-monitoring **one central plane across all clients**, or **one isolated setup per client**?

Everything downstream depends on the answer — where the source register lives, whether tenancy is a column, and how externally-namespaced assets must be named. Read this before designing any of those.

---

## The decision

**Hub-and-spoke.** One central control plane owning enrollment, register, and findings; per-client isolated collection; **tenancy carried as explicit data everywhere**.

Monitoring is not one binary — it is five independently-assignable layers:

| # | Layer | Verdict | Why |
|---|---|---|---|
| 1 | **Enrollment** — how a source becomes known | **CENTRAL + automatic** | A hand-maintained register drifts. The first control-plane event auto-enrols; the register reconciles both directions. |
| 2 | **Register** — what exists, who owns it, what "healthy" means | **CENTRAL**, outside every client estate | A client with no warehouse has no per-client register to federate. |
| 3 | **Collection** — the process reading control tables and logs | **PER-CLIENT isolated processes** | Control planes differ per client; one process holding N credential sets is the leak. |
| 4 | **Findings** — the anomaly list a human reads | **CENTRAL, tenant-tagged** | Findings are already ambiguous at n=2 (see "collisions" in #77). |
| 5 | **Remediation** — who fixes it | **CENTRAL team**, per-client handover supported | Handover only works if findings slice cleanly by tenant. |

**Fully-isolated-per-client is the option the evidence rules out**: it makes tribal knowledge *the interface* — precisely what the monitoring hires exist to eliminate.

---

## The three principles

### 1. Tenancy is data, never location

Today "which client is this?" is answered by *where the thing lives* — a cloud workspace, a warehouse account, a repo. That works only while the mapping stays 1:1, and the business model breaks the 1:1 on purpose:

- Warehouse accounts have a per-account price floor, so **small clients must share one account**, separated by database and grants.
- Some clients have **no warehouse account at all** (Excel-fed, pre-landing).
- Some sources run **on-premise behind a VPN**, with no client-identifiable cloud namespace.

> Once the physical isolation boundary varies by client, location stops being a reliable answer to *"whose is this?"* — so **tenancy must be an explicit attribute carried as data**.

This is structural, not merely economic: even if warehouse pricing changes, the Excel-fed client still has no account and the on-prem box still has no namespace. Grafana Mimir reaches the same conclusion — a tenant ID is a mandatory header on every read and write, never inferred from which cluster data sits in.

**Consequence:** a tenant column on every register, heartbeat, and finding row.

### 2. Isolate at the account level where the vendor supports delegated operator access

Fall back to in-account grants **only** where it does not — and treat that fallback as **temporary and vendor-driven, with a named exit**.

| Vendor capability | Boundary |
|---|---|
| Workspaces/orgs with **invited operator membership** (e.g. Railway, GitHub) | per-client account. Offboarding is revoking one invitation; a team member is added to one workspace only. This is tenant-as-container (cf. Datadog child orgs, Azure Lighthouse delegation) — a model to **preserve**. |
| **No** delegated operator access (e.g. MotherDuck today) | shared account + `RESTRICTED` per-database grants for small clients. **Named exit:** the vendor's multi-workspace support landing. |

**Corollary — namespaces that cannot hold a column must carry the tenant in the name.** Cloud project names, warehouse database names, secret names, and cross-repo issue/decision references all need a canonical short client slug, with the register as the authority for slug → everything. Two real failures make this concrete: a monitoring session misattributed one client's cloud project to another because both were called "sap", and the warehouse credential env var is byte-identical across two client repos, so any process holding both silently keeps one.

### 3. The hub must not share a failure domain with what it watches

The central plane lives on infrastructure **separate from the collectors it monitors**, and **outside every client estate**.

- **Outside client estates** because one client's source inventory sitting in another's warehouse is a *commercial* leak — which client runs which system is competitive information, distinct from data leakage — and it would depart with that client on handover.
- **Separate from the collectors** because if the hub shares their platform, one platform incident removes the pipelines *and* the ability to see they are gone. Hosted elsewhere, that same incident produces exactly the finding you want: *"every collector for tenant X went silent at 14:02."*

**Corollary:** the hub needs **its own dead man's switch** — an external always-firing check — or "the morning sweep silently stopped" reproduces the failure this document exists to prevent, one level up.

**Chosen home:** a dedicated Postgres on **Supabase Pro**. Managed daily backups with 7-day retention and optional PITR, on a platform separate from the one running the collectors. The free tier is disqualified — no backups on the one database that is the memory of everything. Supabase's built-in auth is explicitly *not* adopted; an auth provider is already chosen elsewhere.

---

## Enrollment — the layer that fixes the drift

Treating the register as *"a list someone maintains"* is the bug. A hand-maintained central register drifts exactly as scattered documentation does. The RMM industry collapsed two acts into one: **installing the agent IS registering the asset** — there is no separate "update the docs" step.

So a pipeline's first control-plane event **auto-enrols** it, and the register **reconciles in both directions**. That yields two alarm states, and both correspond to failures already observed in the field:

| Alarm state | Real instance |
|---|---|
| **emits but never registered** | A source ran live in production for months while documentation said "not yet ingested". Nothing noticed it cross probe→production. |
| **registered but never emits** | A deployed service with zero events and no documentation — indistinguishable from mid-development or abandoned. |

This is not a documentation problem. It is a **missing enrollment mechanism**; documentation was the symptom.

---

## Collection — per-tenant processes, centrally scheduled, heart-beating the hub

The apparent fork — federated collectors (dead ones invisible) versus one central collector (crown-jewel credential target) — is a false dilemma. It conflates three independent axes: *where the process runs*, *who holds the credential*, *how liveness is detected*.

**Per-tenant collector processes, each injected with exactly one tenant's read-only credential, all heart-beating one central ledger — running centrally unless network topology forbids it.**

- **Topology decides placement**, not policy. A VPN-only on-prem source *must* push; a plain SaaS warehouse needs no resident agent; an Excel-fed client with no telemetry gets a healthchecks.io-style ping from its hand-run script.
- **A dead collector is not invisible.** In the industry pattern the agent *polls the centre*, so the centre always knows last-seen. Prefect states it plainly: *"Prefect Cloud never makes an inbound request into your network. It only receives metadata and state updates from your workers."* **Silence is a first-class finding**, in the same table as everything else.
- **Credential cross-wiring dies at the naming layer** — one distinctly-named secret per tenant, never a shared key name.
- **Modes migrate per tenant.** When a technically-capable client takes over their own monitoring, that tenant's collector job converts to a resident agent pushing the same heartbeat. Register and findings do not change.

---

## Two lifecycles, two grains

**Tenant lifecycle** (a client: prospect → live → handover → offboarded) and **source lifecycle** (one feed within a client: probed → … → retired) are **separate**, at different grains, and both are worth encoding. Stage values are deliberately not fixed here.

Offboarding is the underweighted one: done right it is **a query against the register** — enumerate this tenant's credentials to revoke, agents to uninstall, checks to disable, data to hand over. That is mechanical with a reconciled register and impossible without one.

---

## Consequences for implementation

1. **Tenant is a mandatory column** on every register, heartbeat, and finding row.
2. **A tenant slug prefixes every columnless namespace** — cloud projects, warehouse databases, secret names, cross-repo references.
3. **Findings need alerting semantics from day one** — `tenant, source, check_id, first_seen, last_seen, ack_state`. At N=50 a bare morning list becomes wallpaper; the real surface is routing, dedup, and acknowledgement.
4. **The register is the access-control seed** — the monitoring team must see every tenant's *health* and no tenant's *data*.
5. **Grace periods are load-bearing, not polish.** Warehouse catalog lag and multi-hour nightly loads make naive freshness checks page falsely, which trains a team to ignore alerts. Expected cadence per source belongs in the register.
6. **Error strings must be scrubbed at collection** — they can embed client data values, which would make the findings table personal data under a DPA.
7. **Where a client runs two control planes during a migration**, the register records which is authoritative per source, or the monitoring team reads the stale one.

---

## What would change this decision

1. **Telemetry becomes contractually residency-bound.** A client demanding that run-level metadata stay inside their estate with right-to-audit takes central tenant-tagged findings off the table *for that tenant*, shifting to a per-tenant store with federated reads. Government and regulated clients are the live risk.
2. **Handover becomes the norm rather than the exception.** If most clients end up operating their own monitoring, build resident-agent-first — handover is then "keep the agent, repoint its heartbeat" rather than a rebuild.
3. **A warehouse vendor ships delegated multi-workspace access.** The shared-account fallback retires; principle 2 already names this exit.

---

## Sources

Verified directly:

- [Prefect hybrid work pools](https://www.prefect.io/learn/diagram-hybrid-pool) — outbound-only worker model, no inbound requests into the customer network.
- [Grafana Mimir authentication & authorization](https://grafana.com/docs/mimir/latest/manage/secure/authentication-and-authorization/) — tenant ID as a mandatory `X-Scope-OrgID` header; multi-tenancy on by default.
- [Railway backups](https://docs.railway.com/reference/backups) · [Supabase backups](https://supabase.com/docs/guides/platform/backups) — backup posture behind the hub-hosting choice recorded in #77.

Cited but not individually fetched: [Dagster+ Hybrid architecture](https://docs.dagster.io/deployment/dagster-plus/hybrid/architecture), [Fivetran Hybrid Deployment](https://fivetran.com/docs/deployment-models/hybrid-deployment), [Azure Sentinel multi-tenant MSSP guidance](https://learn.microsoft.com/en-us/azure/sentinel/multiple-tenants-service-providers), [Datadog multi-organization accounts](https://docs.datadoghq.com/account_management/multi_organization/), [healthchecks.io cron monitoring](https://healthchecks.io/docs/monitoring_cron_jobs/), [Grafana Labs metamonitoring](https://grafana.com/blog/2021/04/08/how-we-use-metamonitoring-prometheus-servers-to-monitor-all-other-prometheus-servers-at-grafana-labs/), [IT Glue for MSPs](https://www.itglue.com/msp/), [MSP360 client offboarding checklist](https://www.msp360.com/resources/blog/msp-client-offboarding-checklist/msp-client-offboarding-checklist/).
