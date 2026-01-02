import json
import os

from typing import Any, Dict, List, Tuple


def strip_wrappers(text: str) -> str:
    """Remove XSSI prefix and trailing comment markers from Maps responses."""
    text = text.strip()
    if text.startswith(")]}'"):
        text = text[4:]
    if text.endswith("*/"):
        start = text.rfind("/*")
        if start != -1:
            text = text[:start]
    return text.strip()


def parse_payload(raw_text: str) -> Any:
    """Parse nested Maps payload that may be wrapped twice."""
    outer = json.loads(strip_wrappers(raw_text))
    if isinstance(outer, dict) and isinstance(outer.get("d"), str):
        return json.loads(strip_wrappers(outer["d"]))
    return outer


def load_payload_from_file(path: str = "f.txt") -> Any:
    """Read a payload file and return the parsed JSON object."""
    with open(path, "r", encoding="utf-8") as f:
        raw_text = f.read()
    if not raw_text.strip():
        raise ValueError(f"{path} is empty; add a payload first.")
    return parse_payload(raw_text)


def _summarize(node: Any, indent: int = 0, max_children: int = 5) -> None:
    """Print a quick shape summary of the parsed payload."""
    prefix = "  " * indent
    if isinstance(node, list):
        print(f"{prefix}list (len={len(node)})")
        for child in node[:max_children]:
            _summarize(child, indent + 1, max_children)
        if len(node) > max_children:
            print(f"{prefix}  ... ({len(node) - max_children} more)")
    elif isinstance(node, dict):
        print(f"{prefix}dict (keys={list(node.keys())})")
        for i, (k, v) in enumerate(node.items()):
            if i >= max_children:
                print(f"{prefix}  ... ({len(node) - max_children} more)")
                break
            print(f"{prefix}  {k}:")
            _summarize(v, indent + 2, max_children)
    else:
        print(f"{prefix}{type(node).__name__}: {repr(node)[:80]}")


def summarize_to_lines(node: Any, indent: int = 0, max_children: int = 5) -> List[str]:
    """Return summary lines to optionally save to disk."""
    lines: List[str] = []
    prefix = "  " * indent
    if isinstance(node, list):
        lines.append(f"{prefix}list (len={len(node)})")
        for child in node[:max_children]:
            lines.extend(summarize_to_lines(child, indent + 1, max_children))
        if len(node) > max_children:
            lines.append(f"{prefix}  ... ({len(node) - max_children} more)")
    elif isinstance(node, dict):
        lines.append(f"{prefix}dict (keys={list(node.keys())})")
        for i, (k, v) in enumerate(node.items()):
            if i >= max_children:
                lines.append(f"{prefix}  ... ({len(node) - max_children} more)")
                break
            lines.append(f"{prefix}  {k}:")
            lines.extend(summarize_to_lines(v, indent + 2, max_children))
    else:
        lines.append(f"{prefix}{type(node).__name__}: {repr(node)[:80]}")
    return lines


def _prompt(message: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    response = input(f"{message}{suffix}: ").strip()
    return response or (default or "")


def _maybe_save_json(payload: Any, default_path: str) -> None:
    """Optionally write a normalized JSON file."""
    choice = _prompt("Save normalized JSON to disk? y/N", "N").lower()
    if choice != "y":
        return
    out_path = _prompt("Output file path", default_path)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[structure] wrote normalized JSON to {out_path}")


def _maybe_save_summary(lines: List[str], default_path: str) -> None:
    """Optionally write the summary shape to disk."""
    choice = _prompt("Save shape summary to a text file? y/N", "N").lower()
    if choice != "y":
        return
    out_path = _prompt("Summary file path", default_path)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[structure] wrote summary to {out_path}")


if __name__ == "__main__":
    try:
        path = _prompt("Enter payload file path", "f.txt")
        payload = load_payload_from_file(path)
        print("[structure] Parsed payload shape:")
        _summarize(payload)
        summary_lines = summarize_to_lines(payload)
        print("\n[structure] Quick stats:")
        print(f"  Top-level type: {type(payload).__name__}")
        if isinstance(payload, list):
            print(f"  Top-level list length: {len(payload)}")
        if isinstance(payload, dict):
            print(f"  Top-level keys: {list(payload.keys())}")
        base = os.path.splitext(path)[0]
        _maybe_save_json(payload, f"{base}_normalized.json")
        _maybe_save_summary(summary_lines, f"{base}_summary.txt")
    except Exception as exc:
        print(f"[structure] Failed to load/parse payloadexample.txt: {exc}")
