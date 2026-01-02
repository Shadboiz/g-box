import json
import os
import re
from collections import deque


def _load_payload(json_source):
    """Return a parsed JSON object from either a path or an in-memory payload."""
    # Accept already-parsed structures (list/dict)
    if isinstance(json_source, (list, dict)):
        return json_source

    # If bytes, decode to string for further handling
    if isinstance(json_source, (bytes, bytearray)):
        json_source = json_source.decode("utf-8")

    # If it's a path, load from disk
    if isinstance(json_source, (str, os.PathLike)) and os.path.exists(json_source):
        with open(json_source, "r", encoding="utf-8") as file:
            return json.load(file)

    # Fallback: try to parse string JSON content directly
    if isinstance(json_source, str):
        return json.loads(json_source)

    raise TypeError(f"Unsupported payload type: {type(json_source)}")


def extract_companies_advanced(json_source):
    """
    More advanced extraction that handles various structures in the new format.

    Accepts either a path to a JSON file or an already-parsed JSON payload.
    """

    data = _load_payload(json_source)

    companies = []

    def is_notg_company(obj) -> bool:
        text = str(obj).lower()
        keywords = ["lgbtq", "lgbt", "lgbtq+", "queer", "transgender", "safe space"]
        return any(k in text for k in keywords)

    def find_place_id(obj):
        queue = deque([obj])
        while queue:
            item = queue.popleft()
            if isinstance(item, str) and item.startswith("ChI") and len(item) > 10:
                return item
            if isinstance(item, list):
                queue.extend(item)
            elif isinstance(item, dict):
                queue.extend(item.values())
        return None

    def find_tel(obj):
        queue = deque([obj])
        while queue:
            item = queue.popleft()
            if isinstance(item, str) and "tel:" in item:
                part = item.split("tel:", 1)[1]
                digits = "".join(ch for ch in part if ch.isdigit())
                if len(digits) >= 6:
                    return digits
            if isinstance(item, list):
                queue.extend(item)
            elif isinstance(item, dict):
                queue.extend(item.values())
        return None

    def scan_rating_reviews(block):
        """Walk nested structures to find rating (1-5) and reviews (>5 or 'X reviews')."""
        rating_val = None
        reviews_val = None

        def walk(obj):
            nonlocal rating_val, reviews_val
            if isinstance(obj, (int, float)):
                if 1 <= obj <= 5 and rating_val is None:
                    rating_val = obj
                elif isinstance(obj, int) and obj > 5 and reviews_val is None:
                    reviews_val = obj
            elif isinstance(obj, str):
                low = obj.lower()
                if "review" in low:
                    m = re.search(r"(\d{1,7})", obj)
                    if m and reviews_val is None:
                        reviews_val = int(m.group(1))
            if isinstance(obj, list):
                for v in obj:
                    walk(v)
            elif isinstance(obj, dict):
                for v in obj.values():
                    walk(v)

        walk(block)
        return rating_val, reviews_val

    def is_lgbtq_company(obj) -> bool:
        text = str(obj).lower()
        keywords = ["lgbtq", "lgbt", "lgbtq+", "queer", "transgender", "safe space"]
        return any(k in text for k in keywords)

    def safe_get(obj, path, default="N/A"):
        """Safely get value from nested structure"""
        try:
            for key in path:
                if isinstance(obj, list) and isinstance(key, int):
                    if key < len(obj):
                        obj = obj[key]
                    else:
                        return default
                elif isinstance(obj, dict) and key in obj:
                    obj = obj[key]
                else:
                    return default
            return obj if obj is not None else default
        except:
            return default

    # Check if data[64] exists and is a list
    if isinstance(data, list) and len(data) > 64:
        companies_list = data[64]

        if isinstance(companies_list, list):
            print(f"Processing {len(companies_list)} potential company entries")

            for i, entry in enumerate(companies_list):
                # Each entry should be [null, company_data]
                if not isinstance(entry, list) or len(entry) < 2:
                    continue

                company_data = entry[1]
                if not isinstance(company_data, list):
                    continue

                if is_notg_company(company_data):
                    continue

                if is_lgbtq_company(company_data):
                    continue

                # Extract name - try multiple indices
                name = None
                for idx in [11, 12, 13, 14]:
                    name_candidate = safe_get(company_data, [idx], None)
                    if (
                        isinstance(name_candidate, str)
                        and name_candidate
                        and name_candidate != "N/A"
                    ):
                        name = name_candidate
                        break

                if not name:
                    continue

                # Extract rating and reviews (tolerant of different layouts)
                rating_info = safe_get(company_data, [4], [])

                rating = safe_get(rating_info, [7], None)
                reviews = safe_get(rating_info, [8], None)
                fallback_rating, fallback_reviews = scan_rating_reviews(rating_info)
                if isinstance(rating_info, list):
                    # Explicit indices for ech1/ech2 style: rating at 7 or 8, reviews at 8 or 9
                    if rating is None and len(rating_info) > 7 and isinstance(
                        rating_info[7], (int, float)
                    ):
                        rating = rating_info[7]
                    if rating is None and len(rating_info) > 8 and isinstance(
                        rating_info[8], (int, float)
                    ):
                        rating = rating_info[8]
                    cand8 = (
                        int(rating_info[8])
                        if len(rating_info) > 8 and isinstance(rating_info[8], (int, float))
                        else None
                    )
                    cand9 = (
                        int(rating_info[9])
                        if len(rating_info) > 9 and isinstance(rating_info[9], (int, float))
                        else None
                    )
                    for cand in (cand8, cand9):
                        if cand is None:
                            continue
                        if reviews is None or (isinstance(reviews, (int, float)) and cand > reviews):
                            reviews = cand
                if rating in (None, "N/A") and fallback_rating is not None:
                    rating = fallback_rating
                if reviews in (None, "N/A") and fallback_reviews is not None:
                    reviews = fallback_reviews
                if rating is None:
                    rating = "N/A"
                if reviews is None:
                    reviews = "N/A"

                # Extract website - index 8 usually contains website info
                website_data = safe_get(company_data, [7], [])
                website = "N/A"

                if isinstance(website_data, str):
                    website = website_data
                elif isinstance(website_data, list) and len(website_data) > 0:
                    # Try to find URL in the list
                    for item in website_data:
                        if isinstance(item, str) and (
                            "http://" in item or "https://" in item or "www." in item
                        ):
                            website = item
                            break
                        elif (
                            isinstance(item, list)
                            and len(item) > 0
                            and isinstance(item[0], str)
                        ):
                            if (
                                "http://" in item[0]
                                or "https://" in item[0]
                                or "www." in item[0]
                            ):
                                website = item[0]
                                break

                def clean_url(url: str) -> str:
                    if not url:
                        return "N/A"
                    if url.startswith("/url?q="):
                        url = url[len("/url?q=") :]
                    if "&" in url:
                        url = url.split("&", 1)[0]
                    url = re.sub(r"^https?://", "", url)
                    url = re.sub(r"^www\.", "", url)
                    return url.rstrip("/")

                if website != "N/A":
                    website = clean_url(website)

                # Extract phone - prefer tel:
                phone = find_tel(company_data) or "N/A"
                if phone == "N/A":
                    for phone_idx in [186, 187, 188, 189, 185]:
                        if len(company_data) > phone_idx:
                            phone_candidate = company_data[phone_idx]
                            if isinstance(phone_candidate, str) and "tel:" in phone_candidate:
                                digits = "".join(ch for ch in phone_candidate if ch.isdigit())
                                if len(digits) >= 6:
                                    phone = digits
                                    break
                            elif (
                                isinstance(phone_candidate, list)
                                and len(phone_candidate) > 0
                            ):
                                for sub_item in phone_candidate:
                                    if isinstance(sub_item, str) and "tel:" in sub_item:
                                        digits = "".join(ch for ch in sub_item if ch.isdigit())
                                        if len(digits) >= 6:
                                            phone = digits
                                            break
                                    elif (
                                        isinstance(sub_item, list)
                                        and len(sub_item) > 0
                                        and isinstance(sub_item[0], str)
                                        and "tel:" in sub_item[0]
                                    ):
                                        digits = "".join(ch for ch in sub_item[0] if ch.isdigit())
                                        if len(digits) >= 6:
                                            phone = digits
                                            break
                            if phone != "N/A":
                                break

                # Extract address
                address_parts = safe_get(company_data, [2], [])
                full_address = safe_get(company_data, [18], "N/A")

                # If we have address parts but no full address, construct it
                if (
                    full_address == "N/A"
                    and isinstance(address_parts, list)
                    and len(address_parts) > 0
                ):
                    full_address = ", ".join(
                        [str(part) for part in address_parts if part]
                    )

                place_id = find_place_id(company_data)
                profile_url = (
                    f"https://www.google.com/maps/place/?q=place_id:{place_id}"
                    if place_id
                    else f"https://www.google.com/maps/search/?api=1&query={name.replace(' ', '+')}"
                )

                company = {
                    "Name": name,
                    "Profile": profile_url,
                    "Website": website,
                    "Phone": phone,
                    "Rating": rating,
                    "Reviews": reviews,
                    "Address": full_address,
                }
                companies.append(company)

    return companies


def save_companies_to_json(companies, output_file):
    """Save extracted companies to a JSON file"""
    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(companies, file, indent=2, ensure_ascii=False)


def print_companies_table(companies):
    """Print companies in a table format"""
    if not companies:
        print("No companies found.")
        return

    print(f"\nFound {len(companies)} companies:")
    print("-" * 140)
    print(
        f"{'#':<3} {'Name':<40} {'Phone':<15} {'Rating':<8} {'Reviews':<10} {'Website':<25} {'Address'}"
    )
    print("-" * 140)

    for i, company in enumerate(companies, 1):
        name = (
            company["Name"][:37] + "..."
            if len(company["Name"]) > 40
            else company["Name"]
        )
        website = (
            company["Website"][:22] + "..."
            if len(company["Website"]) > 25 and company["Website"] != "N/A"
            else company["Website"]
        )
        address = (
            str(company.get("Address", "N/A"))[:30] + "..."
            if len(str(company.get("Address", "N/A"))) > 33
            else str(company.get("Address", "N/A"))
        )
        print(
            f"{i:<3} {name:<40} {company['Phone']:<15} {str(company['Rating']):<8} {str(company['Reviews']):<10} {website:<25} {address}"
        )


def print_exact_format(companies):
    """Print companies in the exact format requested"""
    if not companies:
        print("[]")
        return

    print("[")
    for i, company in enumerate(companies):
        comma = "," if i < len(companies) - 1 else ""
        print(f"  {{")
        print(f'    "Name": "{company["Name"]}",')
        print(f'    "Profile": "{company["Profile"]}",')
        print(f'    "Website": "{company["Website"]}",')
        print(f'    "Phone": "{company["Phone"]}",')

        # Handle numeric values
        try:
            rating = float(company["Rating"]) if company["Rating"] != "N/A" else "null"
        except:
            rating = "null"

        try:
            reviews = int(company["Reviews"]) if company["Reviews"] != "N/A" else "null"
        except:
            reviews = "null"

        print(f'    "Rating": {rating},')
        print(f'    "Reviews": {reviews}')
        print(f"  }}{comma}")
    print("]")


# Main execution
if __name__ == "__main__":
    # Specify your input and output file paths
    input_file = "ech2_payload_23044.530.json"  # Replace with your actual file
    output_file = "extracted_companies.json"

    try:
        print("Extracting companies from new JSON structure...")

        # Try the advanced extraction first
        companies = extract_companies_advanced(input_file)

        # Save to JSON file
        save_companies_to_json(companies, output_file)

        # Print results in table format
        print_companies_table(companies)

        # Print in the exact requested format
        print("\n\nIn requested format:")
        print_exact_format(companies)

        print(f"\n\nData saved to: {output_file}")

        # Show summary
        print(f"\n\nSummary:")
        print(f"Total companies extracted: {len(companies)}")
        companies_with_website = sum(1 for c in companies if c["Website"] != "N/A")
        companies_with_phone = sum(1 for c in companies if c["Phone"] != "N/A")
        print(f"Companies with website: {companies_with_website}")
        print(f"Companies with phone: {companies_with_phone}")

    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except json.JSONDecodeError:
        print(f"Error: '{input_file}' is not a valid JSON file.")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()
