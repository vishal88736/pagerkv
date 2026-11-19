# PagerKV

**Group 28** · Vishal Agrawal, C. H. Tarun, Shreyas Rai, Vijay, and Vinay

PagerKV is a distributed key-value store. It saves text on a primary node, copies it to a replica, and shows what happens when the replica goes offline and recovers.

## Start with Docker

Install Docker and the Docker Compose plugin. From the project folder, run:

```bash
PAGERKV_ADMIN_TOKEN=pagerkv-local-demo docker compose up --build
```

Open [http://127.0.0.1:8001/ui](http://127.0.0.1:8001/ui). If the guided demo asks for a token, enter `pagerkv-local-demo`. Both ports are published only on localhost. The first run downloads and builds the image. Both nodes share that image.

Press **Ctrl+C** to stop, or run `docker compose down` from another terminal. Docker keeps the saved data in volumes.

## Run on Ubuntu without Docker

Extract the ZIP and open the `pagerkv` folder in a terminal. On **Ubuntu 24.04 LTS**, run:

```bash
sudo apt update
sudo apt install -y python3 python3-venv ca-certificates
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
bash scripts/run_nodes.sh
```

On another Linux distribution, install Bash and Python 3.12 with virtual environment support, then run the last four commands. Ports **8001** and **8002** must be available. Press **Ctrl+C** to stop both nodes. On later runs, activate `.venv` and run the launcher again.

| Open | Address |
| --- | --- |
| Dashboard | [http://127.0.0.1:8001/ui](http://127.0.0.1:8001/ui) |
| Node A API docs | [http://127.0.0.1:8001/docs](http://127.0.0.1:8001/docs) |
| Node B API docs | [http://127.0.0.1:8002/docs](http://127.0.0.1:8002/docs) |

## Dependencies

- **FastAPI and Pydantic** define and validate requests. **Uvicorn** runs both services.
- **HTTPX** sends requests between nodes. **SQLite**, included with Python, stores the replica.
- **tiktoken** counts prompt tokens. Approximate counting is available if the tokenizer data cannot load.
- The dashboard uses HTML, CSS, and JavaScript. Node.js is only needed to run dashboard tests. Python versions and package pins are listed in [pyproject.toml](pyproject.toml) and [requirements.txt](requirements.txt).

## Architecture

- The browser dashboard sends REST requests to **Node A** on port **8001**.
- Node A saves the full value and its version in a local JSON checkpoint, then sends a copy to **Node B**.
- Node B stores the copy in SQLite on port **8002**. If B is unavailable, A can still serve local reads and retries pending copies in the background.
- Node A can summarize eligible blocks in the active prompt and fetch the original text from Node B when needed.

[View the architecture diagram](docs/images/pagerkv-architecture.png)

## Try the demo

1. Open **Replication → Start guided demo** to save and copy a sample key.
2. Click **Simulate outage**, then **Read locally** to see that Node A still returns the updated value.
3. Click **Recover replica** and wait for the background retry to confirm the copy.
4. Click **Page and restore**, then **Hydrate sample B → A** to see the full text return to the prompt.
5. Click **Reset demo** to remove the sample keys from this demo session.

![The current PagerKV dashboard with both nodes online and matching copies](docs/images/dashboard-online.png)

See the [outage, recovery, and mobile screenshots](docs/TECHNICAL_GUIDE.md#dashboard-screenshots) or the [video recording guide](docs/VIDEO_DEMO_SCRIPT.md).

## Project layout

```text
pagerkv/
├── .github/workflows/    GitHub Actions checks
├── bench/                Benchmark scripts and dashboard replay data
├── common/               Shared schemas, validation, and token counting
├── docs/                 Technical guide, screenshots, and architecture diagram
├── node_a/               Primary REST API, local store, and replication retry
├── node_b/               Replica REST API and SQLite archive
├── scripts/              Local launcher
├── tests/                Python and dashboard tests
├── ui/                   Web dashboard
├── demo.py               Command-line demo
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

## Run the tests

With the virtual environment active:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

With Node.js 22 or later installed, run `node --test tests/dashboard.test.cjs` for the dashboard checks. For APIs, settings, persistence, and benchmarking, see the [technical guide](docs/TECHNICAL_GUIDE.md).
