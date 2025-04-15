import os
import json
from airflow.operators.python import get_current_context

CONFIG_FILE = "/opt/airflow/config.json"

def load_config():
    try:
        context = get_current_context()
        dag_run = context.get("dag_run")
        if dag_run and dag_run.conf:
            print("[INFO] Using dynamic config from dag_run.conf")
            return dag_run.conf
    except Exception as e:
        # Not inside a task context (e.g., running from CLI or outside DAG execution)
        print(f"[INFO] No DAG context available: {e}")

    # Fallback to static config.json
    if not os.path.exists(CONFIG_FILE):
        print(f"[INFO] Config file not found at: {CONFIG_FILE}")
        return None

    try:
        with open(CONFIG_FILE, "r") as file:
            config = json.load(file)
            print(f"[INFO] Loaded config from: {CONFIG_FILE}")
            return config
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON config: {e}")
        return None
