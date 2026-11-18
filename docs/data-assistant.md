# The data assistant: answers on the dashboards, grounded in the serving layer

The dashboards answer the questions you thought of when you built them. The
data assistant answers the ones you didn't: ask in plain english, get a number
- and the sql that produced it.

It is deliberately **grounded**: at startup it introspects the serving schema
(tables, columns, row counts, the real date range) and answers only from there.
It is not a chatbot over the lake - it is a sql-generating layer over the
published serving tables, with the guardrails a bank would demand first.

## The guardrails (the interesting part)

- **read-only, by construction**: one statement per question; the sql passes a
  gate that rejects multi-statement input, any non-select keyword
  (insert/update/delete/drop/...), and any table outside the serving allowlist
  - the allowlist itself is derived from the live schema, not hardcoded
- **forced LIMIT**: a query without one gets `limit 50`
- **transparent**: every answer prints the sql it ran. An answer without its
  sql is not an answer
- **no data leaves the deployment** in rules mode

## Two brains

- **rules mode** (default): deterministic intents - inventory, alerts by
  rule/severity/date, top accounts by volume, volume trend per account.
  Works offline, no keys, and its sql is parameterized.
- **llm mode** (opt-in): set `DATA_ASSISTANT_LLM_URL` + `DATA_ASSISTANT_LLM_KEY`
  (any openai-compatible endpoint) and the schema goes into the prompt, the sql
  comes back - through the **same guard** as rules mode. If the endpoint is
  unreachable or returns something illegal, it degrades to rules mode with a
  printed note; the bot never blocks on the llm.

## Run it

```bash
python scripts/data_assistant.py "what data do you have"
python scripts/data_assistant.py "alerts by rule"
python scripts/data_assistant.py "how many high alerts on 2026-11-17"
python scripts/data_assistant.py "top 5 accounts by volume"
python scripts/data_assistant.py "volume trend for ACCT151716"
python scripts/data_assistant.py          # repl mode
```

example:

```
> alerts by rule
alerts by rule (all days)
sql: select rule_id, count(*) as alerts, round(max(score)::numeric, 1) ...
  rule_id                  alerts  max_score
  RULE_RESTRICTED_COUNTRY  12      80.0
  RULE_VOLUME_SPIKE        1       52.0
```

## Why the serving layer and not the lake

The serving tables are the small, published, contract-shaped copy (postgres
schema `serving` - see `ServingJob`). Grounding the assistant there means: the
numbers it quotes are the same numbers on the dashboards, the surface is small
enough to guard, and a wrong answer is traceable to one sql statement. Asking
free-form questions against raw lake partitions would need full-text indexes
and a much bigger trust boundary - not worth it for a laptop demo, and the
pattern (schema-grounded, guarded, transparent) is what carries over to a real
deployment.
