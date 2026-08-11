# `docs/agents/` — hub

Per-repo configuration consumed by the Matt Pocock engineering skills (`/triage`, `/to-tickets`, `/to-spec`, `/qa`, `/wayfinder`, `/domain-modeling`, and friends). These files are *configuration for agents*, not project documentation — they tell a skill where issues live and what vocabulary to use, so the same skill behaves correctly in this repo.

- [`issue-tracker.md`](issue-tracker.md) — issues live as GitHub issues in `siraj-samsudeen/feather-flow`, driven by the `gh` CLI. Also carries the "PRs as a request surface" flag (currently **no**) and the `/wayfinder` map/child-ticket conventions.
- [`triage-labels.md`](triage-labels.md) — maps the five canonical triage roles to this repo's actual label strings. Currently the defaults: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`.
- [`domain.md`](domain.md) — domain-doc layout (single-context: root `CONTEXT.md` + `docs/adr/`) and the rules for reading it before exploring the codebase.

## When to add a doc

Only when a new engineering skill needs its own per-repo configuration. Everything else about how we work belongs in [`../CONTRIBUTING.md`](../CONTRIBUTING.md) or [`../conventions/`](../conventions/README.md).

To change existing configuration, edit these files directly — re-running `/setup-matt-pocock-skills` is only needed to switch issue trackers or start over.
