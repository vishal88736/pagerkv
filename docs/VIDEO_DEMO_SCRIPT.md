# PagerKV video demonstration script

The italic paragraphs describe what to show. Read the quoted paragraphs as narration.
Use fresh keys for manual examples and read observed versions/statuses from the screen.
The script has no timestamps.

## 1. Explain the problem statement

*Show the README's problem-statement alignment table.*

> The assignment asks us to build a REST-based mini distributed key-value store. Node A must store key-value pairs, a second Node B must receive a copy on every PUT, and the system must track replication success, failure, and consistency. PagerKV implements these requirements with two real HTTP services. It then applies the store to agent context management, making replication and its consequences visible.

## 2. Explain the architecture

*Show the architecture diagram and point to A's backing store, A's active prompt, and B's archive.*

> An observation's name is the key, and its complete text is the value. A keeps a durable full value and a separate active-context view. B stores another full copy in SQLite. Replication sends the original text from A to B. Paging replaces eligible text only in the active prompt with a short stub. Hydration retrieves the exact original from B. The context is smaller, but the stored originals remain complete.

## 3. Start both services

*From the pagerkv directory, run the following in a terminal, then open the dashboard.*

```bash
source .venv/bin/activate
SPILL_DIRTY=0 bash scripts/run_nodes.sh
```

Open `http://127.0.0.1:8001/ui`.

> Node A runs on port 8001 and Node B on 8002. These are separate services communicating over HTTP. A checkpoints its state to JSON, and B uses a separate SQLite database. The launcher starts and stops both. The project also includes Docker Compose configuration with separate data volumes; this recording uses local Python services.

## 4. Tour the dashboard

*Show the node status and metric cards in Overview. Open Diagnostics to show prompt composition and the node diagram.*

> Overview shows node availability and whether the copies match. The cards show prompt usage, context restores, prompt tokens saved, and pending copies. The composition panel and node diagram occupy separate areas. Tall blocks represent full active context; short dashed blocks represent stubs. B shows acknowledged full copies, including copies of blocks still resident on A.

## 5. Demonstrate PUT and full replication

*Open Replication and click Start guided demo. Expand HTTP request and response details, then open Compare copies under Selected key in Keys.*

> The first step sends a real PUT /key with a complete sample observation. A saves it locally and then attempts POST /replicate to B. The initial attempt is awaited before A responds. Here the copy is acknowledged, and both nodes show matching values and versions. The result panel records B's actual HTTP response and duration. The acknowledgment checks the key, version, and SHA-256 content digest; that checks integrity, not authentication.

## 6. Demonstrate replica failure

*Advance to Simulate outage. Show the new version, HTTP 503 result, dirty state, and unknown consistency with pending work.*

> This step makes B's data endpoints return HTTP 503 and updates the sample on A. A still accepts the value locally, but B has not acknowledged the new version. The copy is pending and the block is dirty. Consistency is unknown because B cannot be inspected. This demonstrates behavior during a simulated replica failure; it is not a physical network partition or a formal CAP classification.

## 7. Demonstrate a local read during the outage

*Advance to Read locally. Show the GET route and complete returned text.*

> A returns the complete updated value from LOCAL_STORE while B remains unavailable. Its stored value is independent of the active prompt and the replica. A successful local write or read does not imply that both nodes are currently synchronized.

## 8. Demonstrate background recovery

*Advance to Recover replica. Wait for the actual comparison to match and show the background-retry trigger.*

> B is now online. The demo does not issue another PUT. The background retry loop sends the pending version, validates the acknowledgment, and restores the matching copy. The pending work clears for this key. Success and failure counters remain as a history of write-replication attempts, including retries.

## 9. Demonstrate paging separately from copying

*Advance to Page and restore, but pause before clicking Hydrate sample. Show the stub, paired complete values, and independent badges.*

> The demo fills the active context until an acknowledged block becomes a stub. Replication is still in sync, while context residence is now paged. A's complete local value and B's original remain unchanged. This reduces prompt tokens, not storage size. The stub is a deterministic extractive summary and retrieval reference, not ZIP-style compression. Its summary portion has a 25-token budget; the complete stub also includes metadata. No LLM produces it.

## 10. Demonstrate hydration

*Click Hydrate sample B → A. Show the reverse animation, full text, and the Context restores count increase.*

> Hydration reads the archived text through GET /context/{key}?hydrate=true. The version must match before the text is restored. This step verifies an actual L2_PAGE_FAULT and exact original-text equality. Other eligible blocks can page out to make room. A resident or locally spilled block can be served without contacting B, so a hydration request is not always a network page fault.

## 11. Compare the two kinds of reads

*Select a paged filler key in Stored keys. Click View prompt block, then Read full value.*

> View prompt block shows how the key appears in the current prompt. Read full value returns the complete text from Node A. Restore to prompt loads that text into the prompt. The table shows the current version, what appears in the prompt, and replication status. The Selected key panel shows the last confirmed version and policy. A last acknowledgment is historical; paired comparison checks the current replica.

## 12. Demonstrate pinning and priority

*Select the small resident sample, open Pinning and priority, toggle Pin, set priority to 20, inspect the policy, then unpin it.*

> Pinning keeps a block from being selected for active-context eviction. Among eligible unpinned blocks, lower priority is considered first, followed by least recently used access order. A replica-safety acknowledgment and a pinning policy are separate conditions. Pinned or dirty text can keep the prompt above budget; PagerKV reports this rather than silently truncating full text.

## 13. Demonstrate search and the rendered prompt

*Search for billing, select a result, and show Current prompt in Diagnostics.*

> Archive search uses SQLite FTS5 when available, with a fallback search implementation. A merges archive results with local key matches and can use local text when B is unavailable. The rendered prompt shows the actual context an agent could consume: headers, full observations, and stubs. With larger datasets, an archive index keeps cold keys discoverable when individual stubs would fill the budget. This is context infrastructure, not a running LLM agent.

## 14. Demonstrate manual create, update, and delete

*Enter a fresh key in Save a key. Save text, update the same key, select it, then click Delete and confirm. Inspect the missing key through API docs.*

> The ordinary workflow uses the same REST API as the guided tour. Creating a key saves and replicates a full value. Updating it advances the version. DELETE removes it locally and replicates a versioned deletion; local completion returns HTTP 204, and a later GET returns 404. If B is unavailable, the deletion remains pending for retry. The dashboard clears deleted copy details only after a successful local deletion.

## 15. Demonstrate version guards and tombstones

*Use only a fresh disposable key. In B's API docs, submit an older version and then inspect the unchanged copy. Delete the key through A, retry the deleted version on B, and inspect the missing archive entry.*

> B rejects stale writes rather than overwriting newer data. Identical same-version retries are idempotent, while a different payload claiming the same version is rejected. After deletion, B retains a version watermark, so a delayed write at or below it cannot resurrect the key. Rejection is indicated by stale_write in the replication response; it need not be an HTTP error.

*For a separate disposable key, write a higher version directly to B, inspect divergence, then update the key normally through A.*

> Live comparison can detect an out-of-band mismatch even if A previously received an acknowledgment. A subsequent normal write can advance above B's version and reconcile that key. Direct writes to B are only for this controlled demonstration; normal clients write through A. This is not an automatic rebuild of every possible divergent dataset.

## 16. Demonstrate slow responses and timeouts

*Open Manual outage simulation and choose Slow (3 seconds). Write another fresh sample, inspect the failure, then select Normal and wait for recovery.*

> The simulated delay exceeds the default two-second A-to-B timeout. A keeps its accepted value, while the result shows no HTTP response within the deadline. That does not prove B never stored it; the delayed request may finish later. Version checks make subsequent retries safe. Restoring normal operation lets the pending version be acknowledged.

## 17. Show API docs, health, readiness, and metrics

*Open /docs on both nodes, then show /health, B's /ready, and /metrics. Restore B to Online after any failure demonstration.*

> The interactive API docs expose the key-value, context, comparison, and fault-control endpoints. B's health endpoint remains available during simulation, while readiness reports data-service availability. Both nodes expose metrics for inspection. Data endpoints are for trusted local use; an optional shared admin token protects failure controls, not the entire API.

## 18. Show live trends and events

*Show Token Window Dynamics on Live Trend and the Live Event Stream while performing a sample update or hydration.*

> The live chart connects token usage with the configured budget, page faults, and dirty or spilled work. The event stream records observed updates, replication transitions, paging, hydration, and failures. These are polling-based observations, and the animations illustrate them rather than capture network packets. If A becomes unreachable, the interface marks its last status as stale.

## 19. Demonstrate restart persistence

*Keep a known sample key, stop the launcher with Ctrl+C, restart with the same command, and inspect the same key and version.*

> A's checkpoint and B's SQLite archive survive a normal service restart. The complete values and versions remain available. This is restart persistence, not automatic failover or protection against losing both nodes' storage. Each node runs as a single process.

## 20. Demonstrate the corrected terminal workflow

*With both services running and B Online, run the command below in another activated terminal.*

```bash
python demo.py --auto --delay 0.5
```

> The terminal demo uses Python's standard library and verifies the same five core phases. It compares full text, versions, and SHA-256; checks failure controls and local reads; waits for background recovery; and requires a real archive page fault with exact text equality. Every run has unique keys. Success appears only after all checks and scoped cleanup pass. Failed checks return a nonzero exit code. Interactive mode is available with python demo.py.

## 21. Show the benchmark and replay

*Run the benchmark, show its measured output and chart, then open Benchmark Replay and use Play or the slider.*

```bash
SPILL_DIRTY=0 python bench/run_benchmark.py --check
python bench/plot.py
```

> The benchmark runs 100 deterministic observations against isolated services and simulates an outage during turns 40 through 49. It checks exact recovery and retrieval-based recall. The baseline stops when its prompt exceeds the budget. PagerKV's dirty context can exceed the limit during an outage because unacknowledged text is retained. After recovery, safe compaction resumes. These are simulated-agent results, not live LLM performance measurements. The replay visualizes the recorded CSV.

*Show the current verification commands and results if including engineering validation.*

```bash
python -m pytest -q
node --test --test-isolation=none tests/dashboard.test.cjs
ruff check .
mypy
```

> Backend and CLI tests exercise real isolated services. Dashboard logic tests use a simulated DOM, while screenshots come from the running website. These checks provide different kinds of evidence.

## 22. Show narrow-screen and reduced-motion support

*Use a narrow browser viewport and scroll through the tour, metrics, transfer strip, and comparison panels. Enable the browser's reduced-motion preference and trigger an action.*

> The interface adapts to smaller screens, and both transfer nodes remain visible. With reduced motion enabled, movement is suppressed while text, request results, and status feedback remain available. The presentation remains usable without relying on animation alone.

## 23. Clean up and close

*Restore B to Online. Reset the guided demo and delete only your additional manual sample keys. Leave the normal dashboard visible.*

> PagerKV satisfies the original assignment through REST storage on A, full replication to B, tracked outcomes, and live consistency. Context paging is the application built on that foundation. The key distinction is that replication copies complete data, paging changes the active prompt, and hydration retrieves the exact original. The project demonstrates these mechanisms with one primary and one replica; it does not claim consensus, automatic failover, or reversible summary compression.
