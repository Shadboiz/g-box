import json
import os
import tempfile
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

from utils.extractor2 import extract_companies_advanced
from utils.token_generator import extract_token, update_url_with_token


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


def _merge_by_name(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    def pick(existing, new):
        """Prefer the new value when the existing one is missing or 'N/A'."""
        if existing in (None, "N/A", ""):
            return new
        return existing

    for rec in records:
        name = rec.get("company_name")
        if not name:
            continue
        current = merged.get(name, {})
        merged[name] = {
            "Name": name,
            "Profile": pick(current.get("Profile"), rec.get("profile_url")),
            "Website": pick(current.get("Website"), rec.get("company_website")),
            "Phone": pick(current.get("Phone"), rec.get("company_phone")),
            "Rating": pick(current.get("Rating"), rec.get("rating_of_reviews")),
            "Reviews": pick(current.get("Reviews"), rec.get("number_of_reviews")),
        }
    return list(merged.values())


def _normalize_phone(phone: str):
    if not phone:
        return None
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    return digits if len(digits) >= 6 else None


def _normalize_site(site: str):
    if not site:
        return None
    site = str(site).lower()
    for prefix in ("http://", "https://"):
        if site.startswith(prefix):
            site = site[len(prefix) :]
    if site.startswith("www."):
        site = site[4:]
    # strip Google redirect
    if site.startswith("/url?q="):
        site = site[len("/url?q=") :]
        if "&" in site:
            site = site.split("&", 1)[0]
    return site.rstrip("/")


def _is_lgbtq(rec: Dict[str, Any]) -> bool:
    text = " ".join(
        str(rec.get(field, "")).lower()
        for field in ("Name", "Profile", "Website", "company_name")
    )
    keywords = ["lgbt", "lgbtq", "pride", "queer", "gay", "lesbian", "trans"]
    return any(k in text for k in keywords)


def _dedupe(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    key_map: Dict[tuple, int] = {}

    def pick(existing, new):
        if existing in (None, "N/A", ""):
            return new
        return existing

    def to_int(val):
        try:
            if isinstance(val, str):
                val = val.replace(",", "")
            return int(float(val))
        except Exception:
            return None

    for rec in records:
        if _is_lgbtq(rec):
            continue

        name = rec.get("Name") or rec.get("company_name")
        profile = rec.get("Profile") or rec.get("profile_url")
        website = rec.get("Website") or rec.get("company_website")
        phone = rec.get("Phone") or rec.get("company_phone")
        rating = rec.get("Rating") or rec.get("rating_of_reviews")
        reviews = rec.get("Reviews") or rec.get("number_of_reviews")
        city = rec.get("City") or rec.get("city")
        niche = rec.get("Niche") or rec.get("niche")

        keys = []
        n_phone = _normalize_phone(phone)
        n_site = _normalize_site(website)
        n_name = name.lower().strip() if isinstance(name, str) else None
        if n_phone:
            keys.append(("phone", n_phone))
        if n_site:
            keys.append(("site", n_site))
        if n_name:
            keys.append(("name", n_name))

        existing_idx = None
        for k in keys:
            if k in key_map:
                existing_idx = key_map[k]
                break

        if existing_idx is None:
            existing_idx = len(merged)
            merged.append(
                {
                    "Name": name,
                    "Profile": profile,
                    "Website": website,
                    "Phone": phone,
                    "Rating": rating,
                    "Reviews": reviews,
                    "City": city,
                    "Niche": niche,
                }
            )
        else:
            cur = merged[existing_idx]
            cur["Name"] = pick(cur.get("Name"), name)
            cur["Profile"] = pick(cur.get("Profile"), profile)
            cur["Website"] = pick(cur.get("Website"), website)
            cur["Phone"] = pick(cur.get("Phone"), phone)
            # Prefer the entry with the higher review count when both are numeric
            cur_reviews_int = to_int(cur.get("Reviews"))
            new_reviews_int = to_int(reviews)
            if cur_reviews_int is None:
                cur_reviews_int = 0
            if new_reviews_int is None:
                new_reviews_int = 0
            if new_reviews_int > cur_reviews_int:
                cur["Reviews"] = new_reviews_int
                # If new rating exists, take it alongside better reviews
                cur["Rating"] = pick(cur.get("Rating"), rating)
            else:
                # Otherwise keep existing rating, but fill if missing
                cur["Rating"] = pick(cur.get("Rating"), rating)
                if cur_reviews_int == 0:
                    cur["Reviews"] = pick(cur.get("Reviews"), reviews)
                else:
                    cur["Reviews"] = cur_reviews_int
            cur["City"] = pick(cur.get("City"), city)
            cur["Niche"] = pick(cur.get("Niche"), niche)

        for k in keys:
            if k not in key_map:
                key_map[k] = existing_idx

    return merged


def _paginate_requests(
    start_url: str,
    first_token: str,
    max_pages: int,
    *,
    headers: Dict[str, str],
    cookies: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], str]:
    """Follow pagination tokens with requests to pull additional records."""
    page_counter = 0
    seen_tokens = set()
    next_token = first_token
    next_url = start_url
    paged_records: List[Dict[str, Any]] = []

    while next_token:
        if next_token in seen_tokens:
            print("[requests] token repeated; stopping pagination.")
            break
        if page_counter >= max_pages:
            print(f"[requests] reached max pagination limit ({max_pages}); stopping.")
            break
        seen_tokens.add(next_token)
        page_counter += 1
        next_url = update_url_with_token(next_url, next_token)
        if not next_url:
            break
        try:
            print(f"[requests] fetching page {page_counter} with token {next_token}")
            resp = requests.get(next_url, headers=headers, cookies=cookies)
            resp.raise_for_status()
        except Exception as e:
            print(f"Failed pagination request ({page_counter}): {e}")
            break
        print(f"[requests] fetched URL: {next_url}")

        paged_raw = resp.text.strip()
        try:
            paged_json = parse_payload(paged_raw)
        except Exception as e:
            print(f"Failed to parse paged response ({page_counter}): {e}")
            break
        # Persist each paginated payload as JSON (no TXT)
        ech_label = 2 + page_counter  # first paginated page after ech=2 -> ech=3
        page_json_path = os.path.join(
            "output", f"ech{ech_label}_payload_page{ech_label}.json"
        )
        try:
            with open(page_json_path, "w", encoding="utf-8") as f:
                json.dump(paged_json, f, ensure_ascii=False, indent=2)
            print(f"[requests] saved paginated payload to {page_json_path}")
        except OSError as e:
            print(f"[requests] failed to save paginated payload: {e}")

        before = len(paged_records)
        paged_records.extend(extract_companies_advanced(paged_json))
        print(
            f"[requests] page {page_counter} added {len(paged_records) - before} records "
            f"(total so far {len(paged_records)})"
        )
        next_token = extract_token(paged_json)

    if not next_token:
        print("[requests] no further tokens; pagination complete.")
    return paged_records, next_url


def process_captured_payloads(
    captured: Dict[str, Any],
    driver,
    max_pages: int,
    *,
    meta: Dict[str, Any] | None = None,
) -> Tuple[str, int, List[Dict[str, Any]]]:
    """Parse collected responses, follow pagination, dedupe, and persist output."""
    os.makedirs("output", exist_ok=True)
    ech1_records: List[Dict[str, Any]] = []
    ech2plus_records: List[Dict[str, Any]] = []
    meta = meta or {}
    meta_city = meta.get("city")
    meta_niche = meta.get("niche")
    try:
        browser_cookies = {c.get("name"): c.get("value") for c in driver.get_cookies()}
    except Exception:
        browser_cookies = {}
    chrome_headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": "https://www.google.com/",
        "priority": "u=1, i",
        "x-browser-channel": "stable",
        "x-browser-copyright": "Copyright 2025 Google LLC. All Rights reserved.",
        "x-browser-validation": "UujAs0GAwdnCJ9nvrswZ+O+oco0=",
        "x-browser-year": "2025",
        "x-client-data": "CJP+ygE=",
        "x-maps-diversion-context-bin": "CAE=",
    }

    ech_counts: Dict[str, int] = {}

    for req_id, url in zip(captured["request_ids"], captured["urls"]):
        response_body = driver.collect_response(req_id)
        raw_text = (response_body.get_decoded_content() or "").strip()

        try:
            payload_json = parse_payload(raw_text)
        except Exception as e:
            print(f"Captured {url} but failed to parse JSON ({e}); skipping.")
            continue

        ech_val = captured["ech_map"].get(req_id)

        ech_label = ech_val or "unknown"
        ech_counts[ech_label] = ech_counts.get(ech_label, 0) + 1
        json_payload_path = os.path.join(
            "output", f"ech{ech_label}_payload_page{ech_counts[ech_label]}.json"
        )
        try:
            with open(json_payload_path, "w", encoding="utf-8") as f:
                json.dump(payload_json, f, ensure_ascii=False, indent=2)
            print(f"[ech={ech_label}] saved payload to {json_payload_path}")
        except OSError as e:
            print(f"[ech={ech_label}] failed to save payload: {e}")

        if ech_val == "2":
            next_token = extract_token(payload_json)
            if next_token:
                print(f"[ech=3+] initial pagination token: {next_token}")
                paged_records, _ = _paginate_requests(
                    url,
                    next_token,
                    max_pages,
                    headers=chrome_headers,
                    cookies=browser_cookies,
                )
                ech2plus_records.extend(paged_records)
            else:
                print("[ech=2] no pagination token found in payload")

        # Use extractor2 for all payloads, mirroring direct runs.
        def run_extractor(func, payload_obj):
            with tempfile.NamedTemporaryFile(
                "w", delete=False, suffix=".json", encoding="utf-8"
            ) as tmp:
                json.dump(payload_obj, tmp, ensure_ascii=False)
                tmp_path = tmp.name
            try:
                return func(tmp_path)
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        if ech_val == "1":
            ech1_records.extend(run_extractor(extract_companies_advanced, payload_json))
        else:
            ech2plus_records.extend(
                run_extractor(extract_companies_advanced, payload_json)
            )

    # inject meta (city/niche) into all records
    def _apply_meta(recs: List[Dict[str, Any]]):
        for r in recs:
            if meta_city:
                r["City"] = meta_city
            if meta_niche:
                r["Niche"] = meta_niche
        return recs

    ech1_records = _apply_meta(ech1_records)
    ech2plus_records = _apply_meta(ech2plus_records)

    extracted_path = os.path.join("output", "extracted_reviews.json")

    def _drop_address(recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for r in recs:
            r.pop("Address", None)
        return recs

    _drop_address(ech1_records)
    _drop_address(ech2plus_records)

    # Save per-ech raw outputs before dedupe
    ech1_out = os.path.join("output", "extracted_reviews_ech1.json")
    ech2_out = os.path.join("output", "extracted_reviews_ech2plus.json")
    with open(ech1_out, "w", encoding="utf-8") as f:
        json.dump(ech1_records, f, ensure_ascii=False, indent=2)
    with open(ech2_out, "w", encoding="utf-8") as f:
        json.dump(ech2plus_records, f, ensure_ascii=False, indent=2)

    deduped = _dedupe(ech1_records + ech2plus_records)

    # Normalize Reviews to integer with default 0
    def _normalize_reviews(recs: List[Dict[str, Any]]):
        for r in recs:
            val = r.get("Reviews")
            if val in (None, "N/A", "", [], {}):
                r["Reviews"] = 0
                continue
            try:
                if isinstance(val, str):
                    val = val.replace(",", "")
                r["Reviews"] = int(float(val))
            except Exception:
                r["Reviews"] = 0
        return recs

    deduped = _normalize_reviews(deduped)
    with open(extracted_path, "w", encoding="utf-8") as f:
        json.dump(deduped, f, ensure_ascii=False, indent=2)

    # Also save to CSV for spreadsheet-friendly consumption
    def save_csv(records: List[Dict[str, Any]], *, niche: str, city: str):
        from datetime import datetime

        # Sanitize parts for filename
        def safe_part(text: str) -> str:
            if not text:
                return "unknown"
            return "".join(
                ch for ch in text.lower() if ch.isalnum() or ch in ("-", "_")
            )

        date_part = datetime.utcnow().strftime("%Y%m%d")
        niche_part = safe_part(niche)
        city_part = safe_part(city)
        fname = f"{date_part}_{niche_part}_{city_part}.csv"
        csv_path = os.path.join("output", fname)
        try:
            pd.DataFrame(records).to_csv(csv_path, index=False)
            print(f"Saved CSV to {csv_path}")
        except Exception as e:
            print(f"Failed to save CSV: {e}")

    # Try to infer niche/city from first record if present
    niche_guess = meta_niche or (deduped[0].get("Niche") if deduped else "")
    city_guess = meta_city or (deduped[0].get("City") if deduped else "")
    save_csv(deduped, niche=niche_guess or "niche", city=city_guess or "city")

    return extracted_path, len(deduped), deduped
