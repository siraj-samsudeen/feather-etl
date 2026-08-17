# feather-flow

Config-driven ETL that extracts from heterogeneous ERP sources into a local DuckDB warehouse. This is the project's glossary — the canonical word for each domain concept, and the words we deliberately avoid.

The four warehouse layers (Bronze, Silver, Gold, Departmental Data Marts) are defined authoritatively in [`docs/architecture/warehouse-layers.md`](docs/architecture/warehouse-layers.md). Use that document's definitions; this file does not restate them.

## Language

**Run**:
One recorded execution of a feather verb against a deployment, captured as a row in `_runs`. Its `trigger` names the verb that wrote it (`run`, `extract`, `transform`, `schedule`) — so a Run is not necessarily a `feather run`.
_Avoid_: job, execution, invocation

**Customer** (never *client*):
The party whose estate we monitor. One word for one thing — *client* is consulting vocabulary, and using both makes a reader stop to ask whether they mean different things.
_Avoid_: client, tenant (reserve *tenant* for the multi-tenancy sense below).

**Customer estate**:
One customer's entire set of systems, servers and data, treated as a single unit we are responsible for. *Estate* carries "everything one party owns, held together".
_Avoid_: environment (means dev/staging/prod), account (means billing or cloud account), site (in RMM tools a site is one location *inside* a customer), footprint.

**Multi-tenant**:
One monitoring system serving many customers at once. Kept over the plainer *multi-client* because **tenants share a building and have walls between them** — that isolation is the whole architectural claim, and *client* carries no wall. Gloss on first use: "one system, many customers, walled off from each other".
_Avoid_: multi-client, many-client, shared-across-clients.

**Managed data platform**:
The data infrastructure we operate on customers' behalf.
_Avoid_: managed data layer (*layer* carries no meaning here and is crowded — OSI, Docker, neural nets), managed data environment.

**Central monitoring service** (the control plane):
The central place that knows what exists, its status, its problems and what alerts are needed — system registry, heartbeat history ledger, detected issues, alerting. Named for position **and** job: the customer-side collectors also monitor, so "monitoring" alone would not separate centre from edge.
_Avoid_: control plane (*plane* gives a newcomer no route to "deciding" — it needs re-explaining every time), monitoring layer (names the domain, not the position), central monitor (reads as a screen).

**Central control + customer-side collectors** (the topology):
A central system coordinates monitoring while the parts that touch customer systems stay inside each customer estate.
_Avoid_: hybrid hub-and-spoke — entirely metaphor; describe the two parts instead.

**Customer-side collector**:
The software running inside or beside a customer estate that gathers load and health data and sends it to the central monitoring service.
_Avoid_: **agent** — in this repo *agent* means an LLM agent, so "deploy an agent into each estate" reads to an agent as being about itself. If the word is ever unavoidable it must be qualified (*monitoring agent*) and never bare: no `agents` table, no `agent_id`.

**Heartbeat**:
A small, regular, automatic signal each collector sends to prove it is still running. Regular and automatic — unlike a *check-in*, which someone chooses to make.
_Avoid_: liveness signal (a second name for one thing, and *liveness* has a separate distributed-systems meaning), keep-alive, still-alive signal.

**Heartbeat history ledger**:
The central record of every heartbeat received, kept over time. Every word earns its place — *heartbeat* names the contents, *history* is the plain word, *ledger* carries the guarantee that it is **append-only and permanent**: entries are added, never edited or deleted.
_Avoid_: ledger alone (pulls toward blockchain and accounting).

**System registry**:
The central list of every customer system and component being monitored.
_Avoid_: register (reads as the CPU or cash-drawer sense), inventory, asset list.

**Detected issues**:
Problems the monitoring finds and records — a failed load, a job running long.
_Avoid_: findings (audit vocabulary, vague about what was found), issues alone (collides with GitHub issues), exceptions (means a thrown error in code).

**Metadata**:
Names, schedules and run status of a customer's jobs and systems — explicitly **not** the business data held inside them. *Meta* = "about": data **about** their systems, not data **from** them.
_Avoid_: leaving the boundary implicit — the isolation claim rests entirely on where this line falls, so state it wherever the claim is made.

**Walled off** (isolated):
Each customer's collection runs separately, sharing no credentials or data paths with any other customer. Reuses the multi-tenant walls image, so one picture explains both terms.
_Avoid_: `isolation` as an identifier — in a data platform it means transaction isolation levels. Use `per_customer_separation`.

**Data pipeline scheduler** (orchestrator):
The tool that schedules and runs the data jobs — Dagster, Prefect, Airflow. A **scheduler decides when; an orchestrator also handles dependencies, retries and ordering** — the contrast is the explanation.
_Avoid_: job runner.

**Unwritten knowledge**:
What the team knows from experience and has never written down. Keeps the implication that matters: unrecorded means fragile, and it leaves when people do.
_Avoid_: tribal knowledge.

**MSP / RMM / MSSP / SIEM**:
Managed Service Provider (a company that runs IT for other companies) · Remote Monitoring and Management (the tool category MSPs use to watch many customers' machines from one place) · Managed Security Service Provider (an MSP whose product is security) · Security Information and Event Management (the central system that collects security logs and raises alerts on them). Expand on first use, short form after.
_Avoid_: MSP and MSSP unexpanded in the same passage — they differ by one letter and mean different things; write *security MSP* if in doubt.
