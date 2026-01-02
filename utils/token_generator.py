import json
from typing import Optional


def extract_token(payload: dict) -> Optional[str]:
    """
    Extract pagination token from an ech response payload.
    First try payload[29][0][1][0] (and inside it if it's a list); if not found,
    scan for the first string starting with '0ahU' (token format).
    """
    try:
        t = payload[29][0][1][0]
        if isinstance(t, str):
            return t
        if isinstance(t, list):
            # Some payloads store the token at index 8 of this list
            if len(t) > 8 and isinstance(t[8], str) and t[8].startswith("0ahU"):
                return t[8]
            for item in t:
                if isinstance(item, str) and item.startswith("0ahU"):
                    return item
    except Exception:
        pass

    # Fallback: scan breadth-first for a token-like string
    from collections import deque

    queue = deque([payload])
    while queue:
        obj = queue.popleft()
        if isinstance(obj, str) and obj.startswith("0ahU"):
            return obj
        if isinstance(obj, list):
            queue.extend(obj)
        elif isinstance(obj, dict):
            queue.extend(obj.values())
    return None


def update_url_with_token(base_url: str, new_token: str) -> Optional[str]:
    """
    Replace the token segment in the maps search URL with the provided token.
    Looks for !5e1!9s <token> !10m2 marker pattern and bumps ech to 3 for paginated calls.
    """
    marker = "!5e1!9s"
    start = base_url.find(marker)
    if start == -1:
        return None
    start += len(marker)
    end = base_url.find("!10m2", start)
    if end == -1:
        return None
    new_url = base_url[:start] + new_token + base_url[end:]
    # After the first page (ech=2), ensure subsequent requests use ech=3
    new_url = new_url.replace("&ech=2", "&ech=3")
    return new_url
