# PagerKV

### REST-based Mini Distributed Key-Value Store with Replication Simulation

PagerKV demonstrates how two HTTP services store and replicate key-value pairs.
**Node A** accepts client writes and keeps the full value. Every PUT triggers an
HTTP replication request to **Node B**, which stores a second full copy. A web
dashboard shows both stores, replication results, and their consistency status.

**Core concepts:** REST · HTTP verbs · primary-to-replica replication · consistency · failure recovery

![PagerKV dashboard showing three matching key-value pairs on Node A and Node B](docs/images/dashboard-online.png)

*The running dashboard with both nodes online. The screenshots in this README
use sample data written through the REST API to isolated local services.*

## Contents

- [Problem statement and implementation](#problem-statement-and-implementation)
- [How it works](#how-it-works)
- [Run locally](#run-locally)
- [Use the REST API](#use-the-rest-api)
- [Failure and recovery demo](#failure-and-recovery-demo)
- [Consistency states](#consistency-states)
- [Configuration and storage](#configuration-and-storage)
- [Run with Docker](#run-with-docker)
- [Project structure](#project-structure)
- [Tests and checks](#tests-and-checks)
- [Scope and limitations](#scope-and-limitations)

## Problem statement and implementation

| Requirement | Implementation |
| --- | --- |
| Build an HTTP service, Node A, to store key-value pairs | FastAPI service with a local SQLite database |
| Add a second Node B service or module | Separate FastAPI service with its own SQLite database |
| Every `PUT /key` triggers a replication call | A saves locally, then calls B over HTTP with the full value and version |
| Track replication success and failure | Persistent attempt counters, per-key errors, and acknowledged versions |
| Show a basic consistency status | Live comparison API and dashboard with clear status labels |

The project also includes GET and DELETE operations, automatic retries,
version checks, restart persistence, and animated copying between nodes.

## How it works

```text
Client / web dashboard
         |
         | PUT /key  {"key": "city", "value": "Bhilai"}
         v
+-----------------------------+
| Node A · primary · :8001     |
| Stores the complete value   |
| SQLite: kv_node_a.db         |
+-----------------------------+
         |
         | PUT /replicate/city
         | Full value + version + deletion flag
         v
+-----------------------------+
| Node B · replica · :8002     |
| Stores the complete copy    |
| SQLite: kv_node_b.db         |
+-----------------------------+
         |
         +---- acknowledgment returned to Node A
```

1. **Save locally.** Node A writes the value and its new version to SQLite.
2. **Attempt replication.** A sends an HTTP request to B and waits for a response.
3. **Validate the acknowledgment.** The returned key, value, version, and deletion
   flag must match the request before A marks the update as replicated.
4. **Record the result.** Success or failure increments the corresponding counter.
5. **Retry failures.** Unacknowledged updates remain stored on A and retry in the
   background, every three seconds by default.

GET requests to A always use its own full local value. B being unavailable does
not prevent A from serving previously stored values or accepting new writes.

Versions protect against delayed or conflicting requests. B rejects older
versions and different payloads claiming the same version; an identical retry is
safe. If B already has a conflicting version, A advances its current update above
that version and retries. DELETE uses a versioned deletion record, or
*tombstone*, to prevent an older request from restoring a deleted key.

## Run locally

### Requirements

- Python 3.12 or newer is recommended and used by CI.
- Bash for the launcher below; commands are shown for Linux/macOS.
- Ports **8001** and **8002** available.
- Node.js 22+ only if you want to run the dashboard behavior checks.

Run these commands from the `pagerkv` directory:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
bash scripts/run_nodes.sh
```

For an application-only installation, use `requirements.txt` instead of
`requirements-dev.txt`.

| Surface | Local URL |
| --- | --- |
| Web dashboard | [http://127.0.0.1:8001/ui](http://127.0.0.1:8001/ui) |
| Node A interactive API docs | [http://127.0.0.1:8001/docs](http://127.0.0.1:8001/docs) |
| Node B interactive API docs | [http://127.0.0.1:8002/docs](http://127.0.0.1:8002/docs) |

The launcher starts both services on localhost. Press **Ctrl+C** to stop them;
the databases remain available for the next run.

<details>
<summary>Use the existing virtual environment in the parent directory</summary>

If your workspace already has `DS/.venv`, run this from `DS/pagerkv`:

```bash
PYTHON="../.venv/bin/python" bash scripts/run_nodes.sh
```

</details>

<details>
<summary>Start the services in two separate terminals</summary>

Activate your virtual environment and run from `pagerkv` in each terminal.

**Terminal 1 — Node B:**

```bash
python -m uvicorn node_b.app:app --host 127.0.0.1 --port 8002
```

**Terminal 2 — Node A:**

```bash
python -m uvicorn node_a.app:app --host 127.0.0.1 --port 8001
```

Stop each service with Ctrl+C. Run one process per node; do not add multiple workers.

</details>

## Use the REST API

Keys and values are strings. Keys contain **1–512 characters** without control
characters; values contain at most **1,000,000 characters**. Empty values are
allowed. Keys containing slashes are supported.

### Endpoints

| Method | Node A endpoint | Purpose |
| --- | --- | --- |
| `PUT` | `/key` | Create or update a key using `{"key":"...","value":"..."}` |
| `PUT` | `/key/{key}` | Create or update the named key using `{"value":"..."}` |
| `GET` | `/key/{key}` | Read its complete value and replication metadata |
| `DELETE` | `/key/{key}` | Delete locally and replicate the deletion |
| `GET` | `/keys` | List live keys and their replication metadata |
| `GET` | `/replication/status` | Compare both nodes and inspect replication counters |
| `GET` | `/health` | Check whether Node A is running |
| `GET` | `/admin/node_b/fault` | Read the current failure simulation mode |
| `POST` | `/admin/node_b/fault` | Set B to online, slow, or down |

Node B provides `GET /key/{key}`, `GET /keys`, `GET /health`, the internal
`PUT /replicate/{key}` endpoint, and its own `/admin/fault` controls.
`/kv/{key}` remains a compatibility alias for Node A's PUT, GET, and DELETE routes.

### 1. Create a key and replicate it

```bash
curl -sS -X PUT http://127.0.0.1:8001/key \
  -H 'Content-Type: application/json' \
  -d '{"key":"city","value":"Bhilai"}'
```

A successful first write to a new key returns:

```json
{
  "key": "city",
  "value": "Bhilai",
  "version": 1,
  "deleted": 0,
  "acked_version": 1,
  "error": null,
  "replicated": true,
  "consistency_status": "synced",
  "node": "A"
}
```

`replicated: true` means B acknowledged that version. If B is unavailable, the
PUT still returns HTTP 200 after saving locally, with `replicated: false`,
`consistency_status: "pending"`, and an error explaining the replication failure.

### 2. Read the value from each node

```bash
curl -sS http://127.0.0.1:8001/key/city
curl -sS http://127.0.0.1:8002/key/city
```

Both return `Bhilai` as the value after replication succeeds.

### 3. Update the value

```bash
curl -sS -X PUT http://127.0.0.1:8001/key/city \
  -H 'Content-Type: application/json' \
  -d '{"value":"Raipur"}'
```

The version increases, and A attempts replication again.

### 4. Check consistency

```bash
curl -sS http://127.0.0.1:8001/replication/status
```

The response includes `consistency`, `pending`, replication attempt counters,
and snapshots of `node_a` and `node_b`. If B cannot be checked, `node_b` is `null`.

### 5. Delete the key

```bash
curl -sS -X DELETE http://127.0.0.1:8001/key/city
```

DELETE returns HTTP **200** when B acknowledges it, or **202** if replication is
pending. GET then returns **404** on A. B returns 404 after the deletion reaches it.
Deletion is safe to repeat; recreating a key uses a newer version.

Invalid request bodies or validated key names return **422**. Requests for a
missing or deleted key return **404**.

## Failure and recovery demo

### Dashboard walkthrough

1. Open the dashboard and save `city` with value `Bhilai`. Both tables show the copy.
2. Choose **Down** under Node B mode and click **Apply mode**.
3. Update `city` to `Raipur`. A retains the new value; replication fails and the
   pending count increases. **GET · read from A** still returns `Raipur`.
4. Consistency becomes **Unknown · B unavailable**, because B's current contents
   cannot be inspected.
5. Choose **Online** and click **Apply mode**. Background retry copies the latest
   value to B. Both tables show `Raipur`, and consistency returns to **In sync**.
6. Try **Slow** to exercise timeouts, or delete a key while B is down to observe
   deletion recovery.

**During the outage:** Node A serves `Raipur` at version 2, the pending count is
one, and the dashboard explicitly marks B's contents as unavailable.

![Dashboard during a Node B outage, with one pending update retained on Node A](docs/images/dashboard-node-b-down.png)

**After recovery:** both nodes hold `Raipur` at version 2 and pending work returns
to zero. The failed-attempt counter remains as a record of the earlier failure.

![Dashboard after recovery, with matching version 2 values on both nodes](docs/images/dashboard-recovered.png)

The animation illustrates requests and acknowledgments observed by the dashboard.
Reduced-motion preferences disable packet movement. The dashboard checks status
every two seconds and marks its last snapshot as stale if A becomes unavailable.

### Reproduce the outage through HTTP

With both services running:

```bash
# Simulate Node B being down.
curl -sS -X POST http://127.0.0.1:8001/admin/node_b/fault \
  -H 'Content-Type: application/json' -d '{"mode":"down"}'

# Save on A while replication cannot succeed.
curl -sS -X PUT http://127.0.0.1:8001/key/city \
  -H 'Content-Type: application/json' -d '{"value":"Raipur"}'

# Confirm A still serves the complete value.
curl -sS http://127.0.0.1:8001/key/city

# Restore B; automatic retries resume copying.
curl -sS -X POST http://127.0.0.1:8001/admin/node_b/fault \
  -H 'Content-Type: application/json' -d '{"mode":"none"}'

# Check again after a retry has completed.
curl -sS http://127.0.0.1:8001/replication/status
```

Use `{"mode":"slow","delay_s":3}` to simulate slow data requests. A timeout
means the write was not acknowledged within the deadline; B may still finish
processing it later. Version checks make the subsequent retry safe.

If an admin token is configured, add `-H 'X-Admin-Token: YOUR_TOKEN'` to the fault
control requests. Simulated failure affects B's data endpoints; its health and
admin endpoints remain available so you can restore it.

## Consistency states

`GET /replication/status` compares full values, versions, and deletion records
while holding A's writer lock.

| API state | Dashboard label | Meaning |
| --- | --- | --- |
| `in_sync` | In sync | The comparison matches and A has no unacknowledged updates |
| `pending` | Pending replication | B responded, but A has writes or deletes awaiting acknowledgment |
| `diverged` | Diverged | B responded and there is no pending work, but the datasets differ |
| `unknown` | Unknown · B unavailable | B could not be checked, including during an outage or timeout |

Per-key `synced` / `pending` metadata describes acknowledgment history. The global
status also checks B's current contents, so an earlier acknowledgment alone does
not prove the stores still match.

Counters count **replication attempts**, including background retries. Several
writes during an outage retain the newest unacknowledged state for each key;
this is not a queue containing every historical value.

## Configuration and storage

Set environment variables before starting the services.

| Variable | Default | Used by | Purpose |
| --- | --- | --- | --- |
| `NODE_B_URL` | `http://127.0.0.1:8002` | A | Replica HTTP address |
| `NODE_A_DB` | `kv_node_a.db` | A | Primary SQLite file |
| `NODE_B_DB` | `kv_node_b.db` | B | Replica SQLite file |
| `REPLICATION_TIMEOUT_S` | `2` | A | Timeout for each HTTP request to B |
| `RETRY_INTERVAL_S` | `3` | A | Delay between background retry passes |
| `PAGERKV_ADMIN_TOKEN` | unset | Both | Token protecting failure controls; otherwise loopback clients only |
| `PYTHON` | `python` | Launcher | Python executable used for both services |

Each node has a separate SQLite database with write-ahead logging. Complete
values, versions, deletion records, pending work, and A's counters survive restart.
Local database files are ignored by Git.

Older `node_a_state.json` and `node_b_archive.db` files, if present, are preserved
and are not imported into these databases.

## Run with Docker

From `pagerkv`, with Docker Engine and Docker Compose available:

```bash
PAGERKV_ADMIN_TOKEN=choose-your-own-demo-token docker compose up --build
```

Open the same dashboard URL: **http://127.0.0.1:8001/ui**. When using failure
controls, enter that token when the dashboard asks for it. It stays in page memory.
The same token is passed to both nodes, and A forwards it to B for admin requests.

Each container uses its own named volume. Ports are published on localhost only.
To stop the containers while retaining their data:

```bash
docker compose down
```

The Compose file defines the node addresses and database paths for Docker.
The additional timing settings above describe direct local launches; they are
not forwarded from the host by the supplied Compose file.

## Project structure

```text
pagerkv/
├── common/
│   ├── admin.py              # Access control for failure simulation
│   ├── schemas.py            # REST request validation
│   ├── store.py              # SQLite values, versions, deletion records, counters
│   └── validation.py         # Shared key and value limits
├── node_a/
│   ├── app.py                # Primary REST API, consistency comparison, dashboard
│   ├── config.py             # Environment configuration
│   ├── replicator.py         # HTTP calls to Node B and acknowledgment checks
│   └── state.py              # Ordered writes and background retries
├── node_b/
│   └── app.py                # Replica API and failure simulation
├── ui/
│   └── index.html            # Dashboard; no frontend build step
├── docs/images/              # Screenshots embedded in this README
├── scripts/run_nodes.sh      # Start and stop the two local services
├── tests/                    # Backend integration and dashboard behavior checks
├── .github/workflows/ci.yml  # Lint, typing, tests, and Docker build jobs
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── requirements-dev.txt
```

## Tests and checks

Install `requirements-dev.txt` and activate the virtual environment, then run:

```bash
python -m pytest -q
ruff check .
mypy
```

With Node.js 22+ installed:

```bash
node --test tests/dashboard.test.cjs
```

Backend tests start isolated services with temporary databases. Coverage includes
HTTP CRUD, full-copy retention, simulated and real process outages, slow responses,
recovery, restart persistence, version conflicts, stale writes, deletion retry,
input validation, and admin-token forwarding.

Dashboard checks exercise value display, HTML escaping, animation states,
reduced motion, stale-status handling, and PUT behavior with a simulated DOM.
The screenshots above were captured from the running website with Chromium.

CI is configured to run lint, type checks, backend tests, dashboard checks, and
a Docker image build. A local check passing does not imply a remote CI run or
container deployment has completed.

## Scope and limitations

- **One primary and one replica.** There is no leader election, automatic failover,
  quorum, or distributed consensus.
- **Single-process nodes.** A uses one in-process writer lock. Run one worker per node.
- **Eventual recovery.** A waits for the initial replication attempt; a locally
  accepted write can remain pending while B is unavailable.
- **Trusted local use.** Data endpoints have no authentication or TLS. The token
  protects failure controls. B's replication endpoint is an internal trusted API.
- **Divergence is reported.** Pending updates retry automatically, but a replica
  containing extra data or losing already acknowledged data is not automatically
  rebuilt. The live comparison reports the difference.
- **Small datasets.** Status checks compare complete snapshots, and the dashboard
  polls them. This is intended for learning and demonstration.
- **Local durability.** Persistence survives service restarts, but deleting a
  node's database is not equivalent to restarting the service.
