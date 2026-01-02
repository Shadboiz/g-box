import time
from typing import Any, Callable, Dict, Tuple
from urllib.parse import parse_qs, urlsplit

from botasaurus.browser import cdp


def _is_target_response(url: str) -> bool:
    """Return True when the response looks like a Maps business page (ech=1/2/3)."""
    if not url:
        return False
    parsed = urlsplit(url)
    if "google.com/search" not in parsed.netloc + parsed.path:
        return False
    ech_vals = parse_qs(parsed.query).get("ech", [])
    return any(val in {"1", "2", "3"} for val in ech_vals)


def build_capture_tracker() -> Tuple[Dict[str, Any], Callable]:
    """
    Provide a captured state dict and a response handler for driver.after_response_received.
    """
    captured: Dict[str, Any] = {"urls": [], "request_ids": [], "last_seen": None, "ech_map": {}}

    def handler(request_id, response: cdp.network.Response, event: cdp.network.ResponseReceived):
        url = response.url or ""
        if _is_target_response(url):
            captured["urls"].append(url)
            captured["request_ids"].append(request_id)
            captured["last_seen"] = time.time()
            ech_vals = parse_qs(urlsplit(url).query).get("ech", [])
            captured["ech_map"][request_id] = ech_vals[0] if ech_vals else None
            print(f"[network] saw business page endpoint (ech): {url}")

    return captured, handler
