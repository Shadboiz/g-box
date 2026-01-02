import os
import time

from botasaurus.browser import Driver, browser

from utils.capture import build_capture_tracker
from utils.payloads import process_captured_payloads

# Cap how many paginated "requests" pages we will fetch after ech=2.
MAX_PAGINATION_PAGES = int(os.getenv("MAX_PAGINATION_PAGES", "5"))


@browser(reuse_driver=True, headless=True)
def initial_request(driver: Driver, data):
    # Open Maps and search for the niche/city
    driver.google_get("https://www.google.com/maps/", accept_google_cookies=True)
    niche = (data or {}).get("niche") or input("Niche to search for: ")
    city = (data or {}).get("city") or input("City to target: ")

    wait_seconds = 60
    search_box = driver.wait_for_element("input#searchboxinput", wait=wait_seconds)
    search_btn = driver.wait_for_element(
        'button[aria-label="Search"]', wait=wait_seconds
    )

    captured, response_handler = build_capture_tracker()
    driver.after_response_received(response_handler)

    search_box.type(f"{niche} in {city}")
    search_btn.click()
    driver.sleep(3)  # give Maps time to fire network calls

    # Scroll until we see an ech=2 response (pagination trigger) or timeout
    start_scroll = time.time()
    while not any(val == "2" for val in captured["ech_map"].values()):
        try:
            driver.scroll(selector='div[role="feed"][aria-label^="Results for"]')
        except Exception:
            pass
        driver.sleep(1)
        if time.time() - start_scroll > 30:
            break

    # Wait until no new endpoints for 10 seconds

    cookies = driver.get_cookies()
    captured["last_seen"] = time.time()
    while time.time() - captured["last_seen"] < 1:
        driver.sleep(1)

    if not captured["request_ids"]:
        print("No business page endpoints (ech=2/3) captured within the wait window.")
        driver.prompt()
        return

    extracted_path, count, records = process_captured_payloads(
        captured,
        driver,
        max_pages=MAX_PAGINATION_PAGES,
        meta={"city": city, "niche": niche},
    )
    print(f"Saved structured data to {extracted_path} ({count} records)")
    return records


# Initiate the web scraping task
if __name__ == "__main__":
    initial_request()
