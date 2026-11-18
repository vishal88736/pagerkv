"""Configuration for the primary key-value node."""

import os

NODE_B_URL = os.environ.get("NODE_B_URL", "http://127.0.0.1:8002").rstrip("/")
NODE_A_DB = os.environ.get("NODE_A_DB", "kv_node_a.db")
REPLICATION_TIMEOUT_S = max(0.1, float(os.environ.get("REPLICATION_TIMEOUT_S", "2")))
RETRY_INTERVAL_S = max(0.1, float(os.environ.get("RETRY_INTERVAL_S", "3")))
