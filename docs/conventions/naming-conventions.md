# Naming conventions — self-evident names, no house codes

**Shared convention — kept byte-identical across repos.** Edit it in one repo and copy it verbatim to the others. Do not fork it with repo-specific detail: no local links, no issue numbers, no ADR numbers. Anything that would only make sense in one repo belongs in that repo's `CONTEXT.md` instead.

## The rule

Any name we coin for a value, column, config key, store format, category, status, or flag — anything a human or an AI agent will read — must be **self-evident**:

1. **No acronyms or house codes.** A name must not need a lookup table to decode. `SC-M-LWGF-N`, `BB`, `FAS`, `OTB`, `LFL` fail this; `Super Centre`, `Fashion Store`, `remaining buy budget`, `comparable store sales` pass.
2. **Readable by an industry veteran.** Use the word the industry already uses, in its plain form. A ten-year hand should be willing to say it out loud in a design review without feeling talked down to.
3. **A newcomer understands it on first explanation and remembers it easily.** One sentence should be enough, and it should still be enough next week. A name sticks when the reader can **rebuild** the meaning from the words, and fails when the mapping is arbitrary and must be **memorised**. *Estate* → everything one party owns → everything one customer runs: rebuildable, so one sentence holds. *Plane* offers no route to "deciding": memorised, and asked about again a week later.
4. **Terminology an AI agent is well-trained on.** Our repos are worked on heavily by agents, so a word that misleads a model costs as much as one that misleads a person. Prefer the globally-standard word over a locally-invented label, and avoid words whose training-data meaning outweighs ours: `environment` (dev/staging/prod, env vars), `setup` (`setup.py`, test setup), `site` (`site-packages`), `layer` (OSI, Docker, neural nets), `range` (`range()`, date ranges), `markdown` (the text format), `account` (billing, cloud), and above all **`agent`** — which now means an LLM agent.
5. **No clever special-cases that become footguns.** Avoid names that smuggle in a second dimension: "Big Box" encodes *size*, "City Center" encodes *location*, and neither says what the store *sells*, which is what a format is. Avoid magic catch-alls (`SPECIAL`) that mean two unrelated things at once. A name that answers a different question than its column asks reads fine and quietly classifies on the wrong axis — the error surfaces only when someone filters on it.

**The grep test for rule 4:** search the candidate in a normal repo. Unrelated hits mean an agent carries the same competing readings. This splits by where the word lives — **prose takes the plain phrase; identifiers take the distinctive one**, because filenames, functions, tables and columns are what agents grep.

## When to use it

Apply it whenever you **introduce or rename** anything a reader will meet:

- dimension and column names, and **enumerated values** (formats, categories, statuses, modes, flags),
- config keys and CLI flags,
- glossary terms in [`CONTEXT.md`](../../CONTEXT.md),
- seed values and `accepted_values` sets,
- anything surfaced on a dashboard, in a report, in a log line or an error message.

It does **not** apply to:

- **Source-faithful ingestion names** — the raw/Bronze layer mirrors the source system verbatim (SAP, GoFrugal, Zakya) so rows can be traced back. Fidelity beats friendliness there; see this repo's warehouse-layer doc.
- **Established external standards** — SAP `MATKL`, GST HSN chapters, ISO codes.
- **Product names** — *Dagster*, *DuckDB*, *Azure Lighthouse*. Explain them; never translate them.

## Three verdicts, not one

Renaming is one outcome of three, and reaching for it every time is the usual mistake:

- **Replace it** — the original earns nothing. *hybrid hub-and-spoke* → *central control + customer-side collectors*.
- **Add a word to it** — keep the original, add the word that kills the wrong reading. *estate* → **customer estate**. The veteran's word survives and the newcomer stops guessing.
- **Keep it** — the jargon is the best name available and any replacement loses information. *multi-tenant* beat the plainer *multi-client* because tenants share a building **and have walls between them**, and that isolation was the whole architectural claim; *client* carries no wall.

Where the original is strong industry jargon that practitioners say out loud, lead with the plain phrase and keep the original in brackets **permanently** — *shelf layout (planogram)*, *product selection (assortment)*, *data pipeline scheduler (orchestrator)*. Two names is the right answer there, not a compromise.

A dropped implication can also be carried by the **explanation** rather than the name, and the sharpest explanations contrast with the term next door: *"a scheduler decides when; an orchestrator also handles dependencies, retries and ordering."*

## The local-vs-global tension — flag it

Sometimes the local term is *less* clear than the global one, and the writer cannot see it because it is their own usage. In India "department store" reads as a glorified kirana, so we use the precise format word instead. An internal name-prefix like `CC` / `HM` should be replaced by the plain term it stands for, not preserved. **Watch especially for vocabulary carried in from a previous industry or employer** — *client* is standard in consulting and reads as jargon in a product context, which is why **customer** is the word in these repos.

## One thing, one word

Sweep every document for **one concept appearing under two names**. This is a defect on its own, independent of whether either name is good, because a reader cannot tell whether two words mean two things and often guesses wrong. Found twice in a single report: *heartbeat* / *liveness signal* for the same message, and *client* / *customer* for the same entity.

Check first whether it is genuinely **two concepts, both badly named**. Otherwise pick one, sweep the whole document, and report which earlier decisions the sweep changes.

## How to settle a name

Run the `/convert-jargon-to-beginner-friendly-terms` skill. It proposes lettered candidates, scores each against the three readers in rules 2–4, names what the losing options cost, and writes the decision back into `CONTEXT.md` in house format (`**Term**:` / definition / `_Avoid_:`). Grill the result if the name is load-bearing: propose the industry-standard candidate, state what it must be distinguished *from*, and confirm it reads correctly to both audiences before committing.

Prefer **deriving** a classification from objective inputs over hand-keying a label that will drift.

## Worked example — the store dimension

Redesigned 2026-06-30 with the CEO under this rule:

- **Rejected:** `SC-M` / `BB` / `FS` / `SS` (size codes), `LWGF` / `FLD` / `GH` (line-of-business codes), `N` / `G` / `M` (lifecycle codes), `Big Box` (a size word), `City Center` (a location word), `Department Store` (ambiguous in India).
- **Adopted:** `store_format` ∈ {Supermarket, Fashion Store, Family Store, Super Centre, Hypermarket, Gold House}; `lifecycle` ∈ {Pre-opening, New, Maturing, Mature, …} via the Walmart 13-month like-for-like rule — all **derived** from objective line-of-business sq-ft, not hand-typed.

## Worked example — the monitoring vocabulary

Settled 2026-08-17 under this rule:

- **Rejected:** `control plane` (*plane* gives no route to "deciding" — re-explained every time), `agent` for a collector (means an LLM agent), `ledger` alone (pulls toward blockchain and accounting), `findings` (audit vocabulary, vague about what was found), `register` (reads as the CPU sense), `isolated` as an identifier (means transaction isolation levels), `client` (consulting vocabulary), `liveness signal` (a second name for *heartbeat*), `markdown` as an identifier (means the text format).
- **Adopted:** `customer estate`, `central monitoring service`, `customer-side collector`, `heartbeat`, `heartbeat history ledger`, `system registry`, `detected issues`, `walled off`, `data pipeline scheduler (orchestrator)`, and `multi-tenant` kept deliberately.
