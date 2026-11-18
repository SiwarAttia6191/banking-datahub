#!/usr/bin/env python3
"""The data assistant: answers questions about the numbers on the dashboards,
grounded in the serving layer - no ingestion of the question, no copy of the data.

How it stays honest:
  - grounded: it introspects the serving schema (tables, columns, date range) at
    startup; every query it runs targets only those tables
  - read-only: one select per question, against an allowlist of serving tables,
    an enforced LIMIT, and a blocklist of non-select keywords
  - transparent: every answer prints the sql it ran - the answer without the sql
    is not an answer
  - two brains: rules mode (default, deterministic intents, works offline) and
    llm mode (opt-in via env, schema goes in the prompt, sql comes back through
    the same guard)

usage:
  python scripts/data_assistant.py "how many high alerts on 2026-11-17"
  python scripts/data_assistant.py "top 5 accounts by volume"
  python scripts/data_assistant.py            # repl mode

llm mode (optional):
  DATA_ASSISTANT_LLM_URL=https://api.openai.com/v1
  DATA_ASSISTANT_LLM_KEY=sk-...
  DATA_ASSISTANT_LLM_MODEL=gpt-4o-mini
"""

import os
import re
import sys
import json
import urllib.request

import psycopg2
from psycopg2.extras import RealDictCursor

PG = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5436")),
    dbname=os.getenv("PGDATABASE", "datahub"),
    user=os.getenv("PGUSER", "datahub"),
    password=os.getenv("PGPASSWORD", "datahub"),
)

DEFAULT_LIMIT = 50
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
NON_SELECT_RE = re.compile(
    r"(?is)\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|vacuum)\b")

HELP = """supported questions, e.g.:
  what data do you have
  how many alerts on 2026-11-17            (date optional - defaults to the latest)
  alerts by severity on 2026-11-17
  alerts by rule
  top 5 accounts by volume on 2026-11-17
  volume trend for ACCT151716
  high severity accounts"""


def introspect(conn) -> dict:
    """The grounding: real schema, real columns, real date range - not assumed."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "select table_name, column_name, data_type from information_schema.columns "
        "where table_schema = 'serving' order by table_name, ordinal_position")
    tables: dict = {}
    for r in cur.fetchall():
        tables.setdefault(r["table_name"], []).append(r["column_name"])
    context = {}
    stats = conn.cursor()  # plain cursor: tuple rows unpack positionally
    for t in tables:
        stats.execute(f"select min(business_date), max(business_date), count(*) "
                      f"from serving.{t}")
        lo, hi, n = stats.fetchone()
        context[t] = {"columns": tables[t], "date_range": (str(lo), str(hi)), "rows": n}
    stats.close()
    return context


def guard(sql: str, allowed_tables: list[str]) -> str:
    """Read-only gate: one statement, select only, allowlisted tables, forced limit."""
    s = sql.strip().rstrip(";")
    if ";" in s:
        raise ValueError("guard: a single statement only")
    if not re.match(r"(?is)^\s*(with|select)\b", s):
        raise ValueError("guard: select statements only")
    if NON_SELECT_RE.search(s):
        raise ValueError("guard: read-only")
    refs = re.findall(r"(?i)(?:from|join)\s+([a-z_][\w\.]*)", s)
    bad = [r for r in refs if r.lower() not in allowed_tables]
    if bad:
        raise ValueError(f"guard: table(s) outside the serving allowlist: {bad}")
    if not re.search(r"(?is)\blimit\b", s):
        s += f" limit {DEFAULT_LIMIT}"
    return s


def date_or_latest(question: str, table: str) -> tuple[str | None, str]:
    """A literal date if the question carries one, else the latest date in the table."""
    m = DATE_RE.search(question)
    if m:
        return m.group(0), "param"
    return None, f"(select max(business_date) from serving.{table})"


def run(conn, sql: str, params: tuple = ()) -> list[dict]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def rules_answer(conn, ctx: dict, question: str) -> tuple[str, str, list[dict]]:
    """Deterministic intents. Returns (title, sql, rows) or raises LookupError."""
    q = question.lower()
    allowed = [f"serving.{t}" for t in ctx]

    if re.search(r"what data|inventory|how much data|row counts", q):
        sqls = [f"select '{t}' as source, rows, min_date, max_date from ("
                f"select count(*) as rows, min(business_date) as min_date, "
                f"max(business_date) as max_date from serving.{t}) s" for t in ctx]
        sql = " union all ".join(sqls)
        return "serving layer inventory", guard(sql, allowed), run(conn, sql)

    if re.search(r"alerts?\s+by\s+rule|by rule", q):
        sql = ("select rule_id, count(*) as alerts, round(max(score)::numeric, 1) as max_score "
               "from serving.aml_alerts group by rule_id order by alerts desc")
        return "alerts by rule (all days)", guard(sql, allowed), run(conn, sql)

    if "alert" in q:
        m = DATE_RE.search(q)
        sev = [s for s in ("critical", "high", "medium", "low") if s in q]
        conds, params = [], []
        title = "alerts"
        if sev:
            conds.append("severity = %s")
            params.append(sev[0])
            title = f"{sev[0]} alerts"
        if m:
            conds.append("business_date = %s")
            params.append(m.group(0))
            title += f" on {m.group(0)}"
        if conds:
            sql = ("select business_date, account_id, rule_id, score, severity "
                   f"from serving.aml_alerts where {' and '.join(conds)} "
                   "order by score desc")
            return title, guard(sql, allowed), run(conn, sql, tuple(params))
        if "by severity" in q:
            sql = ("select severity, count(*) as alerts, round(max(score)::numeric, 1) as max_score "
                   "from serving.aml_alerts group by severity")
            return "alerts by severity (all days)", guard(sql, allowed), run(conn, sql)
        # no date in the question: pin to the latest day the table knows about
        _, frag = date_or_latest(q, "aml_alerts")
        sql = ("select business_date, account_id, rule_id, score, severity "
               f"from serving.aml_alerts where business_date = {frag} order by score desc")
        return "alerts (latest day)", guard(sql, allowed), run(conn, sql)

    m = re.search(r"top\s+(\d+)\s+accounts", q)
    if m:
        date, mode = date_or_latest(q, "daily_volumes")
        sql = ("select account_id, direction, sum(day_sum_eur) as volume_eur, "
               "sum(txn_count) as txns from serving.daily_volumes")
        params = ()
        if date and mode == "param":
            sql += " where business_date = %s"
            params = (date,)
        elif date:
            sql += f" where business_date = {date}"
        sql += (" group by account_id, direction order by volume_eur desc "
                f"limit {int(m.group(1))}")
        title = f"top {m.group(1)} accounts by volume"
        if date:
            title += f" on {date if mode == 'param' else 'latest day'}"
        return title, guard(sql, allowed), run(conn, sql, params)

    m = re.search(r"(trend|volume).*(for|of)\s+(\w+)", q)
    if m:
        account = m.group(3).upper()
        sql = ("select business_date, direction, txn_count, day_sum_eur, sum_7d_eur, "
               "round(zscore_day_log::numeric, 2) as zscore from serving.daily_volumes "
               "where account_id = %s order by business_date desc")
        return (f"volume trend for {account}",
                guard(sql, allowed), run(conn, sql, (account,)))

    raise LookupError(HELP)


def print_answer(title: str, sql: str, rows: list[dict]) -> None:
    print(f"\n{title}")
    print(f"sql: {sql}")
    if not rows:
        print("(no rows)")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in cols}
    print("  " + "  ".join(c.ljust(widths[c]) for c in cols))
    print("  " + "  ".join("-" * widths[c] for c in cols))
    for r in rows:
        print("  " + "  ".join(str(r[c]).ljust(widths[c]) for c in cols))


def llm_sql(ctx: dict, question: str) -> str:
    """Optional: schema in the prompt, one read-only select out - through the same guard."""
    base = os.getenv("DATA_ASSISTANT_LLM_URL", "").rstrip("/")
    key = os.getenv("DATA_ASSISTANT_LLM_KEY", "")
    model = os.getenv("DATA_ASSISTANT_LLM_MODEL", "gpt-4o-mini")
    if not base or not key:
        return ""
    schema = "\n".join(
        f"serving.{t} ({', '.join(c['columns'])}) rows={c['rows']} "
        f"dates={c['date_range'][0]}..{c['date_range'][1]}"
        for t, c in ctx.items())
    prompt = (
        "You answer questions about a bank data hub. Write ONE read-only postgres "
        "SELECT against ONLY these tables:\n" + schema +
        "\nRules: select only, single statement, no ddl/dml, add a LIMIT if heavy. "
        "Answer with the sql only, no markdown.\nQuestion: " + question)
    body = json.dumps({"model": model, "temperature": 0,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    req = urllib.request.Request(
        f"{base}/chat/completions", data=body, method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        res = json.loads(r.read().decode())
    return res["choices"][0]["message"]["content"].strip()


def answer(conn, ctx: dict, question: str) -> None:
    allowed = [f"serving.{t}" for t in ctx]
    if os.getenv("DATA_ASSISTANT_LLM_URL") and os.getenv("DATA_ASSISTANT_LLM_KEY"):
        try:
            sql = guard(llm_sql(ctx, question), allowed)
            return print_answer("llm mode", sql, run(conn, sql))
        except Exception as e:  # noqa: BLE001 - llm mode degrades to rules, never blocks
            print(f"(llm mode unavailable: {e} - falling back to rules mode)")
    title, sql, rows = rules_answer(conn, ctx, question)
    print_answer(title, sql, rows)


def main() -> None:
    conn = psycopg2.connect(**PG)
    ctx = introspect(conn)
    inv = ", ".join(f"serving.{t} ({c['rows']} rows)" for t, c in ctx.items())
    print(f"grounded on: {inv}")
    if os.getenv("DATA_ASSISTANT_LLM_URL") and os.getenv("DATA_ASSISTANT_LLM_KEY"):
        print("llm mode: enabled")
    questions = [a for a in sys.argv[1:] if not a.startswith("-")]
    if questions:
        try:
            answer(conn, ctx, " ".join(questions))
        except LookupError:
            print(HELP)
        except ValueError as e:
            print(f"rejected: {e}")
        conn.close()
        return
    # repl
    print("data assistant - ask about the serving layer, 'help', ctrl-c to exit")
    while True:
        try:
            q = input("\n> ").strip()
        except (KeyboardInterrupt, EOFError):
            break
        if not q:
            continue
        if q.lower() in ("help", "?"):
            print(HELP)
            continue
        try:
            answer(conn, ctx, q)
        except LookupError:
            print(HELP)
        except ValueError as e:
            print(f"rejected: {e}")
    conn.close()


if __name__ == "__main__":
    main()
