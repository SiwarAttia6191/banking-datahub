#!/usr/bin/env python3
"""The alert -> case handoff: an alert is not a case until a human owns it.

In a bank, high-severity alerts leave the data platform and become cases in a
case-management workflow (appian-style: the human process lives in a bpm tool,
not in the lake). This script is that handoff, implemented locally against the
postgres serving instance:

  - `open`        critical/high alerts from serving.aml_alerts that have no case
                  yet become NEW cases (idempotent: one case per alert_id)
  - `list`        the triage queue, optionally filtered by status
  - `transition`  move a case through the lifecycle NEW -> IN_REVIEW -> CLOSED,
                  with assignee and note; every move is written to case_events

usage:
  python scripts/alert_cases.py open
  python scripts/alert_cases.py open --severities critical,high
  python scripts/alert_cases.py list [--status NEW]
  python scripts/alert_cases.py transition <case_id> IN_REVIEW --assignee j.doe
  python scripts/alert_cases.py transition <case_id> CLOSED --note "false positive, confirmed with the relationship manager"
"""

import argparse
import os
import sys

import psycopg2
from psycopg2.extras import RealDictCursor

PG = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5436")),
    dbname=os.getenv("PGDATABASE", "datahub"),
    user=os.getenv("PGUSER", "datahub"),
    password=os.getenv("PGPASSWORD", "datahub"),
)

# the lifecycle: a case is worked, then it is closed - no shortcuts
TRANSITIONS = {"NEW": {"IN_REVIEW"}, "IN_REVIEW": {"CLOSED"}, "CLOSED": set()}

SCHEMA = """
create schema if not exists cases;
create table if not exists cases.alert_cases (
  case_id       uuid primary key default gen_random_uuid(),
  alert_id      text unique not null,
  rule_id       text not null,
  business_date date not null,
  account_id    text not null,
  severity      text not null,
  score         numeric,
  status        text not null default 'NEW',
  assignee      text,
  note          text,
  opened_at     timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);
create table if not exists cases.case_events (
  event_id  bigserial primary key,
  case_id   uuid not null references cases.alert_cases(case_id),
  event     text not null,
  actor     text,
  note      text,
  at        timestamptz not null default now()
);
"""


def ensure_schema(cur) -> None:
    cur.execute(SCHEMA)


def open_cases(conn, severities: list[str]) -> int:
    cur = conn.cursor()
    ensure_schema(cur)
    cur.execute(
        "select alert_id, rule_id, business_date, account_id, severity, score "
        "from serving.aml_alerts where severity = any(%s) "
        "order by business_date, score desc", (severities,))
    alerts = cur.fetchall()
    opened = 0
    for alert_id, rule_id, date, account, severity, score in alerts:
        # one case per alert, ever - re-running a day must not duplicate the queue
        cur.execute(
            "insert into cases.alert_cases (alert_id, rule_id, business_date, "
            "account_id, severity, score) values (%s, %s, %s, %s, %s, %s) "
            "on conflict (alert_id) do nothing returning case_id",
            (alert_id, rule_id, date, account, severity, score))
        row = cur.fetchone()
        if row:
            cur.execute(
                "insert into cases.case_events (case_id, event) values (%s, 'OPENED')",
                (row[0],))
            opened += 1
    conn.commit()
    cur.close()
    return opened


def list_cases(conn, status: str | None) -> None:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    ensure_schema(cur)
    sql = ("select case_id, business_date, rule_id, account_id, severity, score, "
           "status, coalesce(assignee, '-') as assignee, coalesce(note, '-') as note "
           "from cases.alert_cases")
    if status:
        sql += " where status = %s"
        cur.execute(sql + " order by opened_at", (status,))
    else:
        cur.execute(sql + " order by opened_at")
    rows = cur.fetchall()
    cur.close()
    if not rows:
        print("no cases")
        return
    print(f"{'case_id':<38}{'date':<12}{'rule':<26}{'account':<12}"
          f"{'sev':<9}{'score':<7}{'status':<11}{'assignee':<10}note")
    for r in rows:
        print(f"{str(r['case_id']):<38}{str(r['business_date']):<12}{r['rule_id']:<26}"
              f"{r['account_id']:<12}{r['severity']:<9}{str(r['score']):<7}{r['status']:<11}"
              f"{r['assignee']:<10}{r['note']}")


def transition(conn, case_id: str, status: str, assignee: str | None,
               note: str | None) -> None:
    if status not in TRANSITIONS:
        sys.exit(f"unknown status '{status}' - use NEW / IN_REVIEW / CLOSED")
    cur = conn.cursor()
    ensure_schema(cur)
    cur.execute("select status from cases.alert_cases where case_id = %s", (case_id,))
    row = cur.fetchone()
    if not row:
        sys.exit(f"no case {case_id}")
    current = row[0]
    if status not in TRANSITIONS.get(current, set()):
        sys.exit(f"illegal transition {current} -> {status} "
                 f"(lifecycle: NEW -> IN_REVIEW -> CLOSED)")
    cur.execute(
        "update cases.alert_cases set status = %s, assignee = coalesce(%s, assignee), "
        "note = coalesce(%s, note), updated_at = now() where case_id = %s",
        (status, assignee, note, case_id))
    cur.execute(
        "insert into cases.case_events (case_id, event, actor, note) values (%s, %s, %s, %s)",
        (case_id, status, assignee, note))
    conn.commit()
    cur.close()
    print(f"case {case_id}: {current} -> {status}")


def audit(conn, case_id: str) -> None:
    cur = conn.cursor()
    ensure_schema(cur)
    cur.execute(
        "select event, coalesce(actor, '-'), coalesce(note, '-'), at "
        "from cases.case_events where case_id = %s order by at", (case_id,))
    print(f"audit trail for {case_id}:")
    for event, actor, note, at in cur.fetchall():
        print(f"  {at}  {event:<12} {actor:<12} {note}")
    cur.close()


def main() -> None:
    p = argparse.ArgumentParser(description="alert -> case handoff")
    sub = p.add_subparsers(dest="cmd", required=True)

    o = sub.add_parser("open")
    o.add_argument("--severities", default="critical,high",
                   help="comma-separated severities that open a case")

    l = sub.add_parser("list")
    l.add_argument("--status")

    t = sub.add_parser("transition")
    t.add_argument("case_id")
    t.add_argument("status")
    t.add_argument("--assignee")
    t.add_argument("--note")

    a = sub.add_parser("audit")
    a.add_argument("case_id")

    args = p.parse_args()
    conn = psycopg2.connect(**PG)
    try:
        if args.cmd == "open":
            sev = [s.strip() for s in args.severities.split(",") if s.strip()]
            n = open_cases(conn, sev)
            print(f"opened {n} new case(s) for severity in {sev}")
        elif args.cmd == "list":
            list_cases(conn, args.status)
        elif args.cmd == "transition":
            transition(conn, args.case_id, args.status.upper(), args.assignee, args.note)
        elif args.cmd == "audit":
            audit(conn, args.case_id)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
