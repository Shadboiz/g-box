import json
import os
import tempfile

from .extractor2 import extract_companies_advanced


def extract(json_data, *, force_extractor2: bool = False) -> list[dict]:
    """
    Extracts company information from Google Maps JSON data using extractor2.

    The force_extractor2 flag is kept for compatibility but no longer changes behavior.
    """
    # Parse JSON if needed
    if isinstance(json_data, str):
        try:
            data = json.loads(json_data)
        except json.JSONDecodeError:
            if json_data.startswith(")]}'"):
                json_data = json_data[4:]
            data = json.loads(json_data)
    else:
        data = json_data

    def run_extractor(func, payload):
        if isinstance(payload, (str, bytes, os.PathLike)):
            return func(payload)
        # write temp file for extractor API
        with tempfile.NamedTemporaryFile(
            "w", delete=False, suffix=".json", encoding="utf-8"
        ) as tmp:
            json.dump(payload, tmp, ensure_ascii=False)
            tmp_path = tmp.name
        try:
            return func(tmp_path)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    companies = run_extractor(extract_companies_advanced, data)

    normalized = []
    for c in companies:
        normalized.append(
            {
                "company_name": c.get("Name"),
                "profile_url": c.get("Profile"),
                "company_website": c.get("Website"),
                "company_phone": c.get("Phone"),
                "rating_of_reviews": c.get("Rating"),
                "number_of_reviews": c.get("Reviews"),
            }
        )
    return normalized
