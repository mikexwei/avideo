import argparse
import json
import re
import sys
from typing import Optional

import requests

DEFAULT_HOST = "http://10.0.0.40:11434"


def normalize_host(raw: str) -> str:
    """Normalize common host typos like `10.,0.0.40` and ensure scheme."""
    host = (raw or "").strip()
    host = host.replace(",", "")
    host = re.sub(r"\s+", "", host)
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"http://{host}"
    return host.rstrip("/")


def request_json(method: str, url: str, timeout: float, payload: Optional[dict] = None) -> dict:
    resp = requests.request(method, url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    parser = argparse.ArgumentParser(description="Test Ollama server connectivity and generation.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Ollama host (default: http://10.0.0.40:11434)")
    parser.add_argument("--model", default="", help="Model name to run a generation test")
    parser.add_argument("--timeout", type=float, default=10.0, help="Request timeout in seconds")
    args = parser.parse_args()

    host = normalize_host(args.host)
    print(f"[1/3] Testing Ollama host: {host}")

    try:
        tags = request_json("GET", f"{host}/api/tags", timeout=args.timeout)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return 1

    models = [m.get("name", "") for m in tags.get("models", [])]
    print("✅ Server reachable")
    print(f"[2/3] Models found: {len(models)}")
    if models:
        for name in models[:15]:
            print(f"  - {name}")
        if len(models) > 15:
            print(f"  ... and {len(models) - 15} more")

    model = args.model.strip() or (models[0] if models else "")
    if not model:
        print("[3/3] Skipping generation test (no model available).")
        return 0

    print(f"[3/3] Running generation test with model: {model}")
    payload = {
        "model": model,
        "prompt": "Reply with only: OK",
        "stream": False,
        "options": {"temperature": 0},
    }

    try:
        result = request_json("POST", f"{host}/api/generate", timeout=max(args.timeout, 30.0), payload=payload)
        response_text = (result.get("response") or "").strip()
        print("✅ Generation succeeded")
        print("Response:")
        print(response_text or "<empty>")
        return 0
    except Exception as e:
        print(f"❌ Generation test failed: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
