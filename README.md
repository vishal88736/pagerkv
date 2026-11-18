# PagerKV

PagerKV gives an LLM agent a small, hard-bounded context window without losing
anything it has observed. **Node A** is the agent's RAM: it holds the blocks
currently in the prompt and keeps their total (including headers) at or below
`MAX_WINDOW_TOKENS` (2000 by default). **Node B** is the disk: a lossless
SQLite archive holding the current version of every block plus a short stub for
each, and a tombstone watermark for every deleted key.
Node A may drop a block's full text only after Node B has acknowledged that
*exact* version. If Node B is down, slow, or errors, the block is dirty: Node A
keeps its full text in its own fsync'd state file and, if the window is full,
*spills* it out of the prompt as a `[SPILLED ...]` stub until Node B is back.
Hydration (`GET /context/{key}?hydrate=true`) brings the full text back from
Node B or from the local spill, so data is never lost and a stub is never
mistaken for the real thing.

A live dashboard is served at **http://127.0.0.1:8001/ui** (see [Dashboard](#dashboard)).

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
                 |  priority/LRU compaction|  <-------------------------------------------+
                 |  dirty-bit guard        |       GET /archive/{key}   (page fault)    |
                 |  local spill (dirty)    |       DELETE /archive/{key}?version=N      |
                 +------------+------------+  ------------------------------------------->+
                              |                                                             |
                              |  Node A -> Node B                                          |
                              v                                                             |
                 +-------------------------+                                                |
                 |  Node B (L2, disk) :8002 |  <---------------------------------------------+
                 |                         |
                 |  SQLite archive         |
                 |  current full text      |
                 |  delete tombstones      |
                 |  deterministic stub     |
                 +-------------------------+

     Agent prompt <-- Node A (L1) <--> Node B (L2) --> durable full text
```

Node A is bounded and persists its state as JSON (atomic write + fsync) for
restarts; that file is also where spilled dirty blocks keep their full text.
Node B is the source of truth for full text once a version is acked. "RAM" and "disk" are roles, not
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

If Node B answers "stale write" because it already holds a newer version
(Node A lost its state file, or a delete is still pending there), Node A
reconciles: it moves the key to Node B's version + 1 and retries, so the newest
write wins instead of staying dirty forever. Two different payloads claiming
the *same* version are a conflict, not an overwrite: Node B keeps the version it
already acked and answers stale, and Node A retries as v+1. An identical
same-version retry is idempotent and succeeds without rewriting the row.

Deletes are versioned too. Node B keeps a tombstone (`deleted_through`) even
after the row is gone, so a delayed replication for that version or older is
rejected instead of resurrecting the key. A genuinely newer write is accepted.
```

Residence (per key):

```
   RESIDENT_FULL  -- compact(): eviction_allowed only -->  PAGED_TO_NODE_B
     ^      |  ^                                              |   ^
     |      |  +------- hydrate(): GET Node B full text ------+   |
     |      |          (only if archive version == entry.version) |
     |      |                                                     |
     |      +-- compact(): dirty, still over budget --> SPILLED_LOCAL
     |                    (SPILL_DIRTY=1, full text kept)    |
     +-------- hydrate(): served from the local spill -------+
                                                             |
                Node B acks this exact version --------------+--> PAGED_TO_NODE_B
```

Compaction runs two passes, each choosing the lowest `priority` first and then
the least recently used key, and never touching pinned keys: first it pages out
blocks Node B has acked; then, only if still over budget, it spills dirty blocks.
Both passes compare the **rendered prompt** against the budget and only move a
block when its stub is smaller than its full text, so a tiny block is never
"evicted" into something longer. Cold stubs that already fit inside
`archive_index` do not force more eviction.

## The seven invariants

1. A key is paged out to Node B (its full text dropped from Node A) **only** if
   `acked_version == version` and its status is `SYNCED_ARCHIVED`.
2. A `DIRTY_UNREPLICATED` or `PENDING_REPLICATION` key is **never** paged out.
   A dirty key may be *spilled*: it leaves the prompt, but its full text stays
   in Node A's fsync'd state file until Node B acks that version.
3. Data is never deleted from Node A unless Node B holds the same version (or
   the caller deletes the key).
4. If the window cannot get under budget (pending or pinned keys, or
   `SPILL_DIRTY=0`), `over_budget` is `true`. Nothing is deleted or truncated.
5. An ack for an old version is ignored. The key stays pending/dirty until its
   current version is acked.
6. Every key in Node A is discoverable: it appears in the rendered prompt as full
   text or stub, or it is named in `archive_index`, or
   `GET /context/search` with its key name returns it.
7. A settled delete is never undone by an in-flight or delayed replication:
   Node B keeps a tombstone watermark and rejects writes at or below it.

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
never trimmed or omitted. During an outage, dirty blocks are spilled rather than
paged, so the prompt stays within budget; with `SPILL_DIRTY=0` they stay
resident and the prompt can exceed 2000 with `over_budget: true`.

The index block is itself never omitted. Stubs are omitted only after the index
block's budget is reserved, so the hard cap always holds when it can. This is
the one place where the rendered prompt is smaller than the full block list,
and it is a view-only budget, not eviction: no data is lost, and every key
stays discoverable through the prompt, the index, or search (invariant 6).

Node B also exposes `GET /search?q=<text>&limit=5` (SQLite FTS5, with a LIKE
fallback) and Node A proxies it as `GET /context/search?q=<text>&limit=5`,
merging local key-name matches so dirty keys are searchable before Node B has
them. While Node B is unreachable, Node A also matches the query against its own
full texts (resident and spilled blocks), so search degrades instead of failing.

### Stubs

Stubs are extractive and deterministic (`common/summarizer.py`): the first
sentence, plus `key terms:` — the distinctive identifiers in the rest of the
text (words with an underscore, letters mixed with digits, or ALL-CAPS codes),
within a 25-token summary budget:

```
[STUB v1 | GET /context/obs_12?hydrate=true for full 200 tokens]: Observation 12: billing telemetry update. | key terms: obs_12, K7QX, total_cents
```

Identifiers are usually what an agent needs to decide whether to hydrate a
block, and they are exactly what a first-sentence summary drops.

## Configuration

Node A reads these environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `NODE_B_URL` | `http://127.0.0.1:8002` | where Node B lives |
| `MAX_WINDOW_TOKENS` | `2000` | hard cap for the active prompt |
| `INDEX_BUDGET_TOKENS` | `150` | tokens reserved inside the cap for `archive_index` |
| `REPLICATION_TIMEOUT_S` | `2.0` | timeout for every Node A -> Node B call |
| `RETRY_INTERVAL_S` | `3.0` | dirty-key retry period |
| `NODE_A_STATE_FILE` | `node_a_state.json` | atomic JSON state checkpoint (also holds spilled full text) |
| `SPILL_DIRTY` | `1` | spill dirty blocks out of the prompt during an outage; `0` keeps them resident (over budget) |
| `PAGERKV_ADMIN_TOKEN` | unset | see [Admin endpoints](#admin-endpoints); both nodes |

Node B reads `NODE_B_DB` (default `node_b_archive.db`) and `PAGERKV_ADMIN_TOKEN`.
All settings are read at process startup.

### Admin endpoints

Fault injection (`/admin/fault` on Node B, `/admin/node_b/fault` on Node A)
can take Node B offline, so it is guarded (`common/admin.py`):

- With `PAGERKV_ADMIN_TOKEN` unset, only loopback clients (127.0.0.1, ::1) may
  call it. A node bound to `0.0.0.0` cannot be switched off from the network.
- With it set, every call needs an `X-Admin-Token` header with that value
  (constant-time compare), from any address. Node A forwards the header to
  Node B. The dashboard asks for the token the first time it gets a 403.

## Run it

```bash
pip install -r requirements-dev.txt     # runtime deps are in requirements.txt

# Start both nodes (Node B on 8002, Node A on 8001), then open http://127.0.0.1:8001/ui
bash scripts/run_nodes.sh

# Tests (each integration test launches an isolated pair on ephemeral ports)
pytest -q

# Lint and type check (what CI runs)
ruff check .
mypy

# Benchmark and chart (self-contained; uses ports 18001/18002)
python bench/run_benchmark.py           # add --check to fail on regressions
python bench/plot.py
```

With Docker instead:

```bash
docker compose up --build               # dashboard at http://127.0.0.1:8001/ui
```

The compose file runs each node in its own container with a named volume for its
state, publishes ports on 127.0.0.1 only, and passes `MAX_WINDOW_TOKENS`,
`SPILL_DIRTY` and `PAGERKV_ADMIN_TOKEN` through. Browser requests into a
container are not loopback, so set `PAGERKV_ADMIN_TOKEN` if you want the
dashboard's fault switch to work there.

Dependencies are pinned. CI (`.github/workflows/ci.yml`) runs ruff, mypy, the
test suite, the benchmark with `--check` (fails if the prompt ever exceeds the
budget, any block is not recovered byte-for-byte, or recall outside the outage
drops below 100%), and a Docker build.

`run_nodes.sh` starts Node B on 8002 first and Node A on 8001, and kills both on
Ctrl-C/TERM. `python` must be the interpreter with the requirements installed
(activate a virtualenv first, or set `PYTHON=/path/to/python`).

## Dashboard

With the nodes running, open **http://127.0.0.1:8001/ui** (`/` redirects there).
It is one static file (`ui/index.html`, no build step) served by Node A, so it
needs no CORS. It polls `/health` and `/context/status?include_text=false` every
1.5 s (the slim status leaves out every key's text) and `/context/prompt` every
4.5 s. Reading a key in the dashboard uses `?peek=true`, which does not count as
an access, so watching the system does not change its eviction order.

- **Top bar:** Node A health, Node B fault mode (a dropdown to switch it between
  online, slow and down), window tokens against the budget, and resident /
  spilled / paged key counts.
- **Stat cards:** active window tokens, page faults, tokens saved by replication,
  and dirty keys (with how many are spilled).
- **Replication activity:** a packet animates from Node A to Node B during a
  dashboard write. Green means an acknowledgment was observed; red means a
  failed or unconfirmed copy. Polling also animates newly observed acknowledgments
  after background recovery, and displays cumulative successful/failed attempts,
  pending keys and pending deletes. These are illustrations of observed requests
  and status changes, not a packet capture. Reduced-motion preferences disable
  packet movement. Connection errors retain the snapshot and label it stale.
- **Isometric view:** the L1 floor shows keys on Node A (resident blocks, plus
  spilled blocks drawn short, dashed and violet); the L2 floor shows all keys
  with acknowledged archive versions, including copies still resident on Node A.
  Each archive block has a native tooltip naming its acknowledged version, so an
  older copy awaiting a newer update can be distinguished. Block height scales with tokens, the grid grows with the key count, dirty
  keys glow amber, and the L2 floor turns red while Node B is down. Hover a block
  for its status (including pin and priority), or click it to inspect it.
- **Window composition:** synced, dirty/pending, spilled-stub, paged-stub and
  archive-index tokens as shares of the window, plus a bar for budget usage.
- **Timeline:** *Live* plots window tokens against the budget, with page-fault
  markers and dirty/spilled counts. *Benchmark replay* loads `bench/results.csv`
  (served at `/ui/bench.csv`) and animates the 100 turns: baseline vs. PagerKV,
  the shaded outage, and spilled keys. Open `/ui#replay` to start on that tab.
- **Panels:** a key table with policy column; inspect (view, hydrate, delete, pin
  or unpin, set priority); a form to PUT new context; search; an **event log**
  (PUTs, replications, failures, page-outs, spills, hydrations, deletes, Node B
  mode changes); and the rendered prompt.

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

# Delete only if the archived version is <= 3 (safe to retry; never removes v4)
# A tombstone is kept, so a late replication at v3 or older cannot resurrect it.
curl -s -X DELETE 'http://127.0.0.1:8002/archive/db_schema?version=3'

# Readiness: 503 while fault mode is "down"; /health stays 200 for the switch
curl -s http://127.0.0.1:8002/ready

# Prometheus metrics: archived keys/tokens, tombstones, fault mode
curl -s http://127.0.0.1:8002/metrics
```

With `mode=down`, `/replicate`, `/search`, `/ready` and `/archive/{key}` return
503; `/health`, `/metrics` and `/admin/fault` stay available so the fault can be
switched back off. With `mode=slow`, data endpoints sleep `delay_s` seconds before
running. `/health` reports `fault_mode` and `ready`, and Node A exposes the same
data-plane signal as `node_b_ready`.

### Node A

```bash
# Save or update a block (version increments on every PUT).
# Optional policy: "pinned": true keeps it resident; "priority" (-100..100):
# lower is evicted first.
curl -s -X PUT http://127.0.0.1:8001/context/db_schema \
  -H 'content-type: application/json' \
  -d '{"text":"Postgres schema with users, orders, payments.", "pinned": true}'

# Change policy later
curl -s -X PATCH http://127.0.0.1:8001/context/db_schema \
  -H 'content-type: application/json' -d '{"pinned": false, "priority": -10}'

# Current view: full text if resident, stub if paged or spilled
# (?peek=true reads without counting as an access for LRU)
curl -s http://127.0.0.1:8001/context/db_schema

# Page fault: exact full text from Node B, or from the local spill
curl -s 'http://127.0.0.1:8001/context/db_schema?hydrate=true'

# Delete locally and on Node B (retried in the background if Node B is down)
curl -s -X DELETE http://127.0.0.1:8001/context/db_schema

# Whole-window status (include_text=false omits each key's text)
curl -s 'http://127.0.0.1:8001/context/status?include_text=false'

# One aggregate answer to "is the key-value store consistent?"
curl -s http://127.0.0.1:8001/replication/status

# Plain value API: GET hydrates when the key is paged or spilled
curl -s -X PUT http://127.0.0.1:8001/kv/db_schema \
  -H 'content-type: application/json' \
  -d '{"value":"Postgres schema with users, orders, payments."}'
curl -s http://127.0.0.1:8001/kv/db_schema
curl -s -X DELETE http://127.0.0.1:8001/kv/db_schema

# Prometheus metrics: window tokens, budget, page faults, resident/paged/
# spilled/dirty key counts, pending deletes, replication attempts
curl -s http://127.0.0.1:8001/metrics

# What the agent would put in its prompt, in created_at order
curl -s http://127.0.0.1:8001/context/prompt

# Search Node A (merges Node B results with local key-name matches)
curl -s 'http://127.0.0.1:8001/context/search?q=obs_17&limit=5'

# Node A health includes node_b_reachable and node_b_ready
curl -s http://127.0.0.1:8001/health
```

Keys may contain slashes (`obs/turn-041`); the key routes use `{key:path}`, as
Node B does. `GET /context/status`, `/context/prompt` and `/context/search` are
registered first, so those three exact names are reserved: `PUT` with one of them
returns 422 instead of storing a key that could never be read back. Names that
only start with them (`status-report`, `search/history`) are ordinary keys.

Inputs are bounded on both nodes: keys are 1-512 characters with no control
characters (so a key cannot inject prompt structure), and text/value is at most
1,000,000 characters. Search `limit` is 1-50. Violations are a 422.

`/kv/{key}` is a plain value facade for callers that do not care about prompt
stubs: `PUT` accepts `{"value": ...}`, `GET` returns the exact value (hydrating
from the spill or Node B when needed, with `source` naming the path), and
`DELETE` returns 204. It reports the version and whether it was replicated.
`GET /replication/status` aggregates keys by consistency and residence,
replication attempt/success/failure counts, the last error, pending deletes and
Node B's reachability/readiness into one `consistent` or `degraded` answer.

`DELETE` removes the key from Node A at once and records a tombstone (persisted)
for the deleted version. Node A then calls Node B's versioned delete; if Node B is
unreachable, the retry loop keeps trying until Node B confirms. A key re-created
before that happens continues numbering after the deleted version, so the
pending delete can never remove the new copy. Node B also keeps the tombstone
after the row is gone, so a replication that was already in flight when the
delete settled is rejected rather than resurrecting the key.

Hydration answers with `source`: `L1` (already resident), `L1_SPILL` (restored
from the local spill) or `L2_PAGE_FAULT` (fetched from Node B). Failures are
explicit:

- Paged key and Node B unreachable/slow: `503` with `{"error": "...", "stub": "..."}`.
- Archive version differs from Node A's version: `409`, never wrong text.
- Missing key: `404`.

## Tests

`pytest -q` currently reports **49 passed**. Integration tests start a fresh
Node B and Node A subprocess pair on ephemeral ports with their own temporary
database and state file, so keys, faults and token budgets cannot leak between
tests. Unit tests that drive `NodeAState` directly use fake replicators and
start no servers at all. Everything runs offline.

Covered:

- Node B: round-trip and sha256, stale-write rejection, same-version idempotency,
  same-version conflict rejection, short stubs, delete tombstones rejecting late
  replication, `/ready` during fault injection, 503 down, versioned delete.
- Node A: under-budget residency, LRU paging, exact hydration, prompt budget,
  literal `status` route, reserved names, keys with slashes, version bump,
  key-range compression, discoverability of all 120 keys, peek reads.
- State/compaction: rendered-prompt budget keeps hot keys resident instead of
  paging everything, `full_tokens_on_node_b` reports the acked version (not a
  dirty newer one), stubs longer than their text are never used, consistency and
  replication counters.
- Faults: dirty keys spill but are never paged and hydrate byte-for-byte while
  Node B is down, spill disabled keeps the old over-budget rule, recovery after
  Node B returns, slow Node B returns within the timeout, stale/out-of-order acks
  ignored, stale-write reconciliation, delete tombstones retried, pin/priority
  eviction, hydrate 503 with stub while down, Node A restart preserves state.
- Ops: admin guard (loopback and token), `/metrics` on both nodes, slim status,
  delete reaching Node B, identifier-keeping stubs, local search while Node B is
  down, `/kv` PUT/GET/DELETE round trip including hydration, aggregate
  `/replication/status` in healthy and degraded modes, input limits, bounded
  search limits, dashboard route and fault proxy.

## Benchmark

`bench/run_benchmark.py` runs 100 turns with `random.seed(42)`. Each observation
is 150-900 tokens. The baseline keeps every observation and joins them into one
prompt; it crashes when that prompt exceeds 2000 tokens. The PagerKV agent does
`PUT /context/obs_{turn}`, reads `/context/prompt`, records dirty/paged/saved
counts, hydrates a paged older key every 10th turn, and runs a whole-outage
window: Node B is set `down` at turn 40 and `none` at turn 50. At the end it
hydrates **every** key and compares sha256 with the original. Every observation
also embeds a unique `Fact obs_N: deploy token is xxxxxx.` line, and every 10th
turn (plus 20 random keys at the end) the simulated agent tries to recover that
fact using only the prompt, `/context/search`, and hydration. Recall succeeds
when the hydrated full text contains the stored code. The code is lowercase and
non-identifier-shaped on purpose: uppercase codes, digits and underscores are
copied into stubs as `key terms`, which would make recall pass without hydrating
anything. Page faults are captured after the 100-turn workload but before the
final 100 correctness hydrations, so the number measures agent and recall
behaviour rather than the benchmark's own verification.

Latest run:

```
Baseline crashed at turn 3 | PagerKV ran 100 turns | max prompt tokens 1999 | lossless 100/100 | page faults 17
recall 29/30 (outside-fault 28/28)
Max prompt tokens outside the fault window: 1999
Benchmark check passed.
```

- Baseline crash: turn 3 (requirement: before turn 15).
- PagerKV completed all 100 turns, and the prompt never exceeded the 2000-token
  cap, **including the outage** (previously 5567 there).
- Lossless: 100/100 full texts recovered by hydration.
- Recall: 28/28 outside the outage, which is the CI gate. The single in-fault
  miss is expected: Node B is down, so a fact that is neither resident nor
  spilled cannot be hydrated until recovery.
- During turns 40-49 up to 10 dirty blocks are spilled to Node A's disk instead
  of staying in the prompt; at turn 50 the retry loop syncs them and they become
  ordinary paged blocks.
- The page-fault count can vary by a few between runs: it depends on how quickly
  the background retry loop syncs keys after the outage and on which recall keys
  were already resident. It counts only hydrations performed by the workload and
  recall checks, not the final verification pass.

![PagerKV benchmark](bench/benchmark.png)

The orange band is the Node B outage. The prompt stays at the cap through it
because dirty blocks are spilled rather than kept resident. The baseline line
ends at its crash. The dashboard's *Benchmark replay* tab animates the same data.

`bench/results.csv` has the raw per-turn data:
`turn,baseline_tokens,pagerkv_prompt_tokens,dirty_keys,paged_out_keys,spilled_keys,tokens_saved,recall_ok`.

## Project layout

```
pagerkv/
  requirements.txt     # pinned runtime deps
  requirements-dev.txt # + pytest, matplotlib, ruff, mypy
  pyproject.toml       # pytest / ruff / mypy config
  Dockerfile
  docker-compose.yml
  .github/workflows/ci.yml
  README.md
  common/
    tokens.py          # the only token counter
    schemas.py         # shared pydantic models
    validation.py      # key/text limits shared by both nodes
    summarizer.py      # deterministic stubs (first sentence + key terms)
    admin.py           # admin-endpoint guard (loopback or token)
    metrics.py         # Prometheus text rendering
  node_a/
    config.py          # env settings
    state.py           # KeyEntry + NodeAState: invariants, compaction, spill, hydration, tombstones
    replicator.py      # shared httpx client + dirty-key retry loop
    app.py             # FastAPI routes for L1
  node_b/
    archive.py         # SQLite archive, stale-write/conflict guards, tombstones
    summarizer.py      # re-exports common.summarizer
    app.py             # FastAPI routes + fault injection for L2
  ui/
    index.html         # dashboard, served by Node A at /ui
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
    test_state.py
    test_fault_tolerance.py
    test_ops.py
    test_ui.py
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
- **No TLS, and only the admin endpoints are authenticated.** The data API is
  open to anyone who can reach the port; both services bind to localhost by
  default and are intended for a trusted machine or network.
- **Spill durability is Node A's disk.** A spilled block's only full copy is in
  Node A's state file until Node B acks it. Losing that file during an outage
  loses those writes, exactly as it would without spilling.
- **Simple eviction policy.** Priority, then least-recently-used, with pinning.
  There is no frequency- or size-aware scoring. With many keys, the stub metadata
  can consume the entire budget, so cold stubs are omitted from the rendered
  prompt view as described above. Pinned keys can push the window over budget.
- **Extractive stubs are lossy.** The stub is the first sentence plus key
  identifiers, not a semantic summary. It is useful as a page table, not as a
  substitute for the text. Hydration is what makes the system lossless.
- **No LLM summarizer.** `STUB_LLM` is intentionally not wired to a network
  provider: tests and benchmarks must run offline with no API key.
- **In-process state lock.** Node A assumes a single process (one uvicorn
  worker). Multiple workers would need shared state or sticky routing.
- **Full-text JSON in status by default.** `/context/status` includes each key's
  `prompt_view` unless called with `include_text=false` (the dashboard and
  `/metrics` use the slim form). It is an observability endpoint, not the hot path.
- **Reconciliation is last-writer-wins.** If Node A loses its state file, its
  next write to a key that Node B already holds supersedes Node B's copy
  (versions jump past it). Two writes claiming the same version are detected as
  a conflict and retried as v+1 rather than overwriting silently. Pending
  deletes are tracked in the same state file, so losing it can leave deleted
  keys on Node B. Node B's tombstones are retained (they are tiny) and are not
  garbage-collected.
- **Polling dashboard.** The UI polls; it does not stream updates. That is fine
  for hundreds of keys, but a large deployment would want server-sent events.
