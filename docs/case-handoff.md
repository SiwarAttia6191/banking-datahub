# The alert -> case handoff (appian-style case management)

An alert is not a case. The detection rules produce alerts every night; most are
noise, and the ones that need a human should live in a **case-management
workflow** - in a bank that is a bpm/low-code tool (appian is the classic choice),
not the data platform. The platform's job is the **handoff**: hand over exactly
the alerts that deserve a human, with the context to work them, and keep an
audit trail of what happened.

`scripts/alert_cases.py` is that handoff, implemented against the local postgres.

## What it does

- `open` reads `serving.aml_alerts` and creates one case per **critical/high**
  alert in `cases.alert_cases`. Idempotent: `alert_id` is unique, so re-running
  a day never duplicates the queue (the same rule as the serving publication).
- Each case carries the alert context a triage analyst needs: rule, date,
  account, severity, score.
- The lifecycle is enforced in sql-adjacent code: `NEW -> IN_REVIEW -> CLOSED`.
  No shortcuts - a closed case without a review would be an audit finding.
- Every move lands in `cases.case_events` (event, actor, note, timestamp): the
  audit trail the compliance officer asks for.

## Run it

```bash
python scripts/alert_cases.py open                       # critical+high -> NEW cases
python scripts/alert_cases.py open --severities critical # stricter queue
python scripts/alert_cases.py list --status NEW          # the triage queue
python scripts/alert_cases.py transition <case_id> IN_REVIEW --assignee j.doe
python scripts/alert_cases.py transition <case_id> CLOSED --note "false positive, confirmed"
python scripts/alert_cases.py audit <case_id>            # the trail
```

## Where it sits in the flow

```
detection rules -> serving.aml_alerts -> [alert_cases.py open] -> cases.alert_cases
                        |                                        |
                        | kibana (ops dashboard)                 | case-management tool
                        v                                        v
                 what fired overnight                    who is working it, what was decided
```

The dashboard shows **what fired**; the case table shows **who owns it and what
was decided**. Keeping them separate is the point: the lake publishes facts, the
bpm workflow owns the human decisions, and the handoff table is the contract
between the two.

## Honest boundary

This is the handoff pattern and the lifecycle, small enough to run on a laptop.
A real appian deployment adds queues per team, sla timers, and the sar
generation behind `CLOSED` - the data-engineering contract (`alert_id` in,
`case_id` out, audit on every move) would not change.
