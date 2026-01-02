import sys

from scraper import initial_request


def prompt_locations() -> list[str]:
    raw = input("City or cities (comma-separated): ").strip()
    if not raw:
        return []
    return [c.strip() for c in raw.split(",") if c.strip()]


def main():
    niche = input("Niche to search for: ").strip()
    cities = prompt_locations()
    if not cities:
        print("No cities provided; exiting.")
        sys.exit(1)

    combined_records = []
    for city in cities:
        print(f"\n=== Processing {city} ===")
        recs = initial_request(data={"niche": niche, "city": city}) or []
        combined_records.extend(recs)

    # Save combined CSV across all cities
    if combined_records:
        from datetime import datetime
        import os
        import pandas as pd

        os.makedirs("output", exist_ok=True)

        def safe_part(text: str) -> str:
            if not text:
                return "unknown"
            return "".join(
                ch for ch in text.lower() if ch.isalnum() or ch in ("-", "_")
            )

        date_part = datetime.utcnow().strftime("%Y%m%d")
        niche_part = safe_part(niche)
        fname = f"{date_part}_{niche_part}_all.csv"
        df = pd.DataFrame(combined_records)
        if "City" not in df.columns:
            df["City"] = ""
        out_path = os.path.join("output", fname)
        df.to_csv(out_path, index=False)
        print(f"\nCombined CSV saved to {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
