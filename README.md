# PagerKV

PagerKV gives an LLM agent a small, hard-bounded context window without losing
anything it has observed. **Node A** is the agent's RAM: it holds the blocks
currently in the prompt and keeps their total (including headers) at or below
`MAX_WINDOW_TOKENS` (2000 by default). **Node B** is the disk: a lossless
SQLite archive holding every version of every block plus a short stub for each.
Node A may replace a full block with its stub only after Node B has acknowledged
that *exact* version. If Node B is down, slow, or errors, the block is dirty and
stays in Node A in full, even if that pushes the prompt over budget. Hydration
(`GET /context/{key}?hydrate=true`) pages the full text back from Node B, so data
is never lost and a stub is never mistaken for the real thing.

## Architecture

```
                          Agent
                            |
                            |  GET /context/prompt   (prompt <= 2000 tokens)
                            v
                 +-------------------------+
                 |  Node A (L1, RAM) :8001 |
                 |                         |
                 |  full blocks + stubs    |       PUT /replicate (versioned, 2s timeout)
                 |  LRU compaction         |  <-------------------------------------------+
                 |  dirty-bit guard        |       GET /archive/{key}   (page fault)    |
                 +------------+------------+  ------------------------------------------->+
                              |                                                             |
                              |  Node A -> Node B                                          |
                              v                                                             |
                 +-------------------------+                                                |
                 |  Node B (L2, disk) :8002 |  <---------------------------------------------+
                 |                         |
                 |  SQLite archive         |
                 |  full text per version  |
                 |  deterministic stub     |
                 +-------------------------+

     Agent prompt <-- Node A (L1) <--> Node B (L2) --> durable full text
```

Node A is bounded and volatile-ish (it persists its state as JSON for restarts).
Node B is the source of truth for full text. "RAM" and "disk" are roles, not
implementation claims: both nodes are FastAPI services, and Node A also writes a
small JSON checkpoint.

## State machine

Consistency (per key):

```
                 PUT (version v)
                       |
                       v
             PENDING_REPLICATION
                /           \
   ack v matches          timeout / 503 / bad JSON
          |                     |
          v                     v
   SYNCED_ARCHIVED  <------  DIRTY_UNREPLICATED
          ^                     |
          |                     | background retry (every RETRY_INTERVAL_S)
          +---------------------+
                ack for the same current version

An ack whose version != entry.version is ignored. It can never mark a newer
version as synced.
```

Residence (per key):

```
   RESIDENT_FULL  -- compact(): eviction_allowed only -->  PAGED_TO_NODE_B
        ^                                                     |
        |                                                     |
        +------------- hydrate(): GET Node B full text --------+
                     (only if archive version == entry.version)
```

## The six invariants

1. A key is paged out **only** if `acked_version == version` and its status is
   `SYNCED_ARCHIVED`.
2. A `DIRTY_UNREPLICATED` or `PENDING_REPLICATION` key is **never** paged out,
   even if the window is over budget.
3. Data is never deleted from Node A unless Node B holds the same version.
4. If the window cannot get under budget because the remaining full keys are
   dirty, `over_budget` is `true`. Nothing is deleted or truncated.
5. An ack for an old version is ignored. The key stays pending/dirty until its
   current version is acked.
6. Every key in Node A is discoverable: it appears in the rendered prompt as full
   text or stub, or it is named in `archive_index`, or
   `GET /context/search` with its key name returns it.

## Prompt budget and headers

All token numbers come from `common/tokens.py` (`tiktoken` `cl100k_base`, loaded
once, with a `max(1, len(text)//4)` fallback if the encoder is unavailable).

The budget is enforced on the **rendered prompt**, including the `### key
(vN)` headers and every stub. The `active_window_tokens` field reports that
rendered footprint, so it can be smaller than the sum of the per-key
`active_tokens_in_node_a` values when cold stubs are omitted from the view.

There is a second-order effect at large page counts: the mandated stub format
alone costs roughly 20-30 tokens per paged key, plus a header. With 100 live
keys, the stubs and headers cost about 3800 tokens, which cannot fit in 2000 no
matter how many full blocks are paged out. To honor the hard cap without
deleting anything, `GET /context/prompt` omits the **oldest already-paged**
blocks from the rendered view when necessary, and renders a single
`### archive_index` block **first** that names them (newest omitted first,
compressed into ranges like `obs_1–obs_57`, up to `INDEX_BUDGET_TOKENS` = 150
tokens, with `+M more` for the rest). Those blocks remain in Node A's state
(with their stubs) and in Node B (full text), are still listed by
`/context/status` (`omitted_from_prompt`, `index_tokens`), and can still be
hydrated by key or found via `GET /context/search`. Resident full blocks are
never trimmed or omitted. During an outage, dirty residents are never trimmed
or paged, so the prompt can exceed 2000 and `over_budget` is `true`; that is
correct.

The index block is itself never omitted. Stubs are omitted only after the index
block's budget is reserved, so the hard cap always holds when it can. This is
the one place where the rendered prompt is smaller than the full block list,
and it is a view-only budget, not eviction: no data is lost, and every key
stays discoverable through the prompt, the index, or search (invariant 6).

Node B also exposes `GET /search?q=<text>&limit=5` (SQLite FTS5, with a LIKE
fallback) and Node A proxies it as `GET /context/search?q=<text>&limit=5`,
merging local key-name matches so dirty keys are searchable before Node B has
them.

## Configuration

Node A reads these environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `NODE_B_URL` | `http://127.0.0.1:8002` | where Node B lives |
| `MAX_WINDOW_TOKENS` | `2000` | hard cap for the active prompt |
| `INDEX_BUDGET_TOKENS` | `150` | tokens reserved inside the cap for `archive_index` |
| `REPLICATION_TIMEOUT_S` | `2.0` | timeout for every Node A -> Node B call |
| `RETRY_INTERVAL_S` | `3.0` | dirty-key retry period |
| `NODE_A_STATE_FILE` | `node_a_state.json` | atomic JSON state checkpoint |

Node B reads `NODE_B_DB` (default `node_b_archive.db`). All settings are read at
process startup.

## Run it

```bash
pip install -r requirements.txt

# Terminal 1 (or use the launcher, which starts both)
bash scripts/run_nodes.sh

# Tests (they launch their own isolated subprocesses on 8001/8002)
pytest -q

# Benchmark and chart (self-contained; uses ports 18001/18002)
python bench/run_benchmark.py
python bench/plot.py
```

`run_nodes.sh` starts Node B on 8002 first and Node A on 8001, and kills both on
Ctrl-C/TERM. `python` must be the interpreter with the requirements installed
(activate a virtualenv first, or set `PYTHON=/path/to/python`).

## API examples

### Node B

```bash
# Archive one version (returns the stub, token count, sha256, stale_write flag)
curl -s -X POST http://127.0.0.1:8002/replicate \
  -H 'content-type: application/json' \
  -d '{"key":"db_schema","version":3,"full_text":"Postgres schema with users, orders, payments."}'

# Read the archived full text
curl -s http://127.0.0.1:8002/archive/db_schema

# Inject a fault: none | down | slow
curl -s -X POST http://127.0.0.1:8002/admin/fault \
  -H 'content-type: application/json' -d '{"mode":"down","delay_s":0}'

curl -s -X POST http://127.0.0.1:8002/admin/fault \
  -H 'content-type: application/json' -d '{"mode":"slow","delay_s":5}'

curl -s -X POST http://127.0.0.1:8002/admin/fault \
  -H 'content-type: application/json' -d '{"mode":"none","delay_s":0}'

# Search the archive (FTS5, with a LIKE fallback)
curl -s 'http://127.0.0.1:8002/search?q=deploy+token&limit=5'
```

With `mode=down`, `/replicate` and `/archive/{key}` return 503; `/health` and
`/admin/fault` stay available. With `mode=slow`, data endpoints sleep
`delay_s` seconds before running.

### Node A

```bash
# Save or update a block (version increments on every PUT)
curl -s -X PUT http://127.0.0.1:8001/context/db_schema \
  -H 'content-type: application/json' \
  -d '{"text":"Postgres schema with users, orders, payments."}'

# Current view: full text if resident, stub if paged
curl -s http://127.0.0.1:8001/context/db_schema

# Page fault: fetch the exact full text from Node B
curl -s 'http://127.0.0.1:8001/context/db_schema?hydrate=true'

# Whole-window status
curl -s http://127.0.0.1:8001/context/status

# What the agent would put in its prompt, in created_at order
curl -s http://127.0.0.1:8001/context/prompt

# Search Node A (merges Node B results with local key-name matches)
curl -s 'http://127.0.0.1:8001/context/search?q=obs_17&limit=5'

curl -s http://127.0.0.1:8001/health
```

`GET /context/status` and `GET /context/prompt` are registered before
`GET /context/{key}`, so the literal words `status` and `prompt` are never
treated as keys.

Hydration failures are explicit:

- Node B unreachable/slow: `503` with `{"error": "...", "stub": "..."}`.
- Archive version differs from Node A's version: `409`, never wrong text.
- Missing key: `404`.

## Tests

`pytest -q` currently reports **19 passed**. The suite starts real Node B and
Node A subprocesses on 8001/8002 with a temporary database and state file,
resets fault injection before each test, uses unique keys, and runs entirely
offline.

Covered:

- Node B: round-trip and sha256, stale-write rejection, short stubs, 503 down.
- Node A: under-budget residency, LRU paging, exact hydration, prompt budget,
  literal `status` route, version bump, key-range compression, discoverability
  of all 120 keys.
- Faults: dirty keys never evicted, recovery after Node B returns, slow Node B
  returns within the timeout, stale/out-of-order acks ignored, hydrate 503 with
  stub while down, Node A restart preserves keys/versions/statuses.

## Benchmark

`bench/run_benchmark.py` runs 100 turns with `random.seed(42)`. Each observation
is 150-900 tokens. The baseline keeps every observation and joins them into one
prompt; it crashes when that prompt exceeds 2000 tokens. The PagerKV agent does
`PUT /context/obs_{turn}`, reads `/context/prompt`, records dirty/paged/saved
counts, hydrates a paged older key every 10th turn, and runs a whole-outage
window: Node B is set `down` at turn 40 and `none` at turn 50. At the end it
hydrates **every** key and compares sha256 with the original. Every observation
also embeds a unique `Fact obs_N: deploy token is XXXX.` line, and every 10th
turn (plus 20 random keys at the end) the simulated agent tries to recover that
fact using only the prompt, `/context/search`, and hydration. Recall succeeds
when the hydrated full text contains the stored code.

Latest run:

```
Baseline crashed at turn 5 | PagerKV ran 100 turns | max prompt tokens 5567 | lossless 100/100 | page faults 137
recall 29/30 (outside-fault 28/28)
Max prompt tokens outside the fault window: 1995
```

- Baseline crash: turn 5 (requirement: before turn 15).
- PagerKV completed all 100 turns.
- Outside turns 40-50: maximum prompt 1995 tokens (cap 2000).
- Lossless: 100/100 full texts recovered by hydration.
- Recall: 29/30 overall; the single miss is turn 40, inside the outage window,
  where `/context/search` correctly returns 503 because Node B is down. Outside
  the fault window recall is 28/28 (100%).
- The 5567-token maximum occurs inside the outage window, where 10 dirty blocks
  cannot be paged out. This is intentional safety over budget, not a bug.

![PagerKV benchmark](bench/benchmark.png)

The orange band is the Node B outage. The prompt spikes there because dirty
keys must stay resident and full; after recovery the retry loop syncs them and
compaction returns the prompt under 2000. The baseline line ends at its crash.

`bench/results.csv` has the raw per-turn data:
`turn,baseline_tokens,pagerkv_prompt_tokens,dirty_keys,paged_out_keys,tokens_saved`.

## Project layout

```
pagerkv/
  requirements.txt
  README.md
  common/
    tokens.py          # the only token counter
    schemas.py         # shared pydantic models
  node_a/
    config.py          # env settings
    state.py           # KeyEntry + NodeAState, invariants, compaction, hydration
    replicator.py      # shared httpx client + dirty-key retry loop
    app.py             # FastAPI routes for L1
  node_b/
    archive.py         # SQLite archive, stale-write guard
    summarizer.py      # deterministic extractive stubs
    app.py             # FastAPI routes + fault injection for L2
  bench/
    run_benchmark.py
    plot.py
    results.csv
    benchmark.png
  scripts/
    run_nodes.sh
  tests/
    conftest.py
    test_node_b.py
    test_node_a.py
    test_fault_tolerance.py
```

## Why versioned acks matter

Each PUT increments the key's version and sends that version to Node B. When
the ack comes back, Node A compares the acked version with the current version
before changing anything. If a slower v1 request finishes after a v2 PUT has
started, the v1 success is ignored. The deterministic
`test_stale_ack_out_of_order_is_ignored` test forces the v2 ack to arrive before
the v1 ack and verifies that v2 stays synced with a v2 stub. Without this check,
a late ack could mark newer text as safe to evict while Node B only holds the
older text.

## Limitations

- **Single Node B.** There is no replication of the archive itself; losing the
  Node B SQLite file loses the full texts.
- **No authentication or TLS.** Both services bind to localhost by default and
  are intended for a trusted machine.
- **LRU only.** Eviction is least-recently-used; there is no frequency, size, or
  pinning policy beyond the one-shot hydration pin. With many keys, the stub
  metadata can consume the entire budget, so cold stubs are omitted from the
  rendered prompt view as described above.
- **Extractive stubs are lossy.** The stub is a whitespace-collapsed first
  sentence (cut to 25 tokens), not a semantic summary. It is useful as a page
  table, not as a substitute for the text. Hydration is what makes the system
  lossless.
- **No LLM summarizer.** `STUB_LLM` is intentionally not wired to a network
  provider: tests and benchmarks must run offline with no API key.
- **In-process state lock.** Node A assumes a single process (one uvicorn
  worker). Multiple workers would need shared state or sticky routing.
- **Full-text JSON in status.** `/context/status` includes each key's
  `prompt_view`, which is large for resident blocks. It is an observability
  endpoint, not the hot path.
- **Version counters live with Node A.** `DELETE` removes a key from Node A but
  Node B keeps its archive. Re-putting a previously deleted key works when its
  archived version is not higher than the new local version. If Node A's state
  file is lost while Node B still holds a higher version, new PUTs for that key
  are rejected as stale and stay dirty; the JSON state checkpoint is part of the
  durability story.
