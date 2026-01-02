import requests
from utils.payloads import parse_payload

TEST_URL = "https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=uk&pb=!4m12!1m3!1d22032.594291905654!2d-4.210688!3d55.8759936!2m3!1f0!2f0!3f0!3m2!1i1920!2i945!4f13.1!7i20!8i20!10b1!12m25!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1!10b1!12b1!13b1!16b1!17m1!3e1!20m3!5e2!6b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m65!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m16!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20!22m5!1sz1lWab6YONGDhbIP5Jaw6QI%3A121!2s1i%3A0%2Ct%3A246204%2Cp%3Az1lWab6YONGDhbIP5Jaw6QI%3A121!7e81!12e22!17sz1lWab6YONGDhbIP5Jaw6QI%3A122!24m109!1m30!13m9!2b1!3b1!4b1!6i1!8b1!9b1!14b1!20b1!25b1!18m19!3b1!4b1!5b1!6b1!9b1!13b1!14b1!17b1!20b1!21b1!22b1!27m1!1b0!28b0!32b1!33m1!1b1!34b1!36e2!10m1!8e3!11m1!3e1!14m1!3b0!17b1!20m2!1e3!1e6!24b1!25b1!26b1!27b1!29b1!30m1!2b1!36b1!37b1!39m3!2m2!2i1!3i1!43b1!52b1!54m1!1b1!55b1!56m1!1b1!61m2!1m1!1e1!65m5!3m4!1m3!1m2!1i224!2i298!72m22!1m8!2b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!4b1!8m10!1m6!4m1!1e1!4m1!1e3!4m1!1e4!3sother_user_google_review_posts__and__hotel_and_vr_partner_review_posts!6m1!1e1!9b1!89b1!98m3!1b1!2b1!3b1!103b1!113b1!114m3!1b1!2m1!1b1!117b1!122m1!1b1!126b1!127b1!26m4!2m3!1i80!2i92!4i8!30m28!1m6!1m2!1i0!2i0!2m2!1i530!2i945!1m6!1m2!1i1870!2i0!2m2!1i1920!2i945!1m6!1m2!1i0!2i0!2m2!1i1920!2i20!1m6!1m2!1i0!2i925!2m2!1i1920!2i945!34m19!2b1!3b1!4b1!6b1!8m6!1b1!3b1!4b1!5b1!6b1!7b1!9b1!12b1!14b1!20b1!23b1!25b1!26b1!31b1!37m1!1e81!42b1!46m1!1e3!47m0!49m10!3b1!6m2!1b1!2b1!7m2!1e3!2b1!8b1!9b1!10e2!50m16!1m11!2m7!1u3!4sOpen+now!5e1!9s0ahUKEwjO_I3nneqRAxUZQkEAHfvFAKUQ_KkBCAYoAg!10m2!3m1!1e1!3m1!1u3!4BIAE!2e2!3m2!1b1!3b1!59BQ2dBd0Fn!67m5!7b1!10b1!14b1!15m1!1b0!69i761&q=removals%20in%20glasgow&tch=1&ech=3&psi=z1lWab6YONGDhbIP5Jaw6QI.1767266769580.1"


def count_review_strings(obj):
    """Count strings containing 'review' (case-insensitive)."""
    count = 0
    stack = [obj]
    while stack:
        node = stack.pop()
        if isinstance(node, str) and "review" in node.lower():
            count += 1
        elif isinstance(node, list):
            stack.extend(node)
        elif isinstance(node, dict):
            stack.extend(node.values())
    return count


def main():
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": "https://www.google.com/",
        "priority": "u=1, i",
        # High-fidelity extras copied from live request; cookies likely to expire/rotate.
        "x-browser-channel": "stable",
        "x-browser-copyright": "Copyright 2025 Google LLC. All Rights reserved.",
        "x-browser-validation": "UujAs0GAwdnCJ9nvrswZ+O+oco0=",
        "x-browser-year": "2025",
        "x-client-data": "CJP+ygE=",
        "x-maps-diversion-context-bin": "CAE=",
    }

    cookies = {
        "AEC": "AaJma5v1TvMdZ_UAZPRwlaQ2n9-1G5ji6LdeTUxt-te5vMeJwDQ7iye4_A",
        "__Secure-BUCKET": "COUD",
        "SOCS": "CAISHAgBEhJnd3NfMjAyNTEyMTAtMF9SQzEaAmVuIAEaBgiA8tbKBg",
        "__Secure-STRP": "AD6Dogt4A2iJAACHUGREEdBdmSo9thmzdA11FMShsy-_18euabU_fmEHBSDOq4GE3Q_ocF62lKRnDMn8F6Vc77VbUTeuqZFZbA",
        "NID": "527=fLtiVQA8IQtE9oEgHJImTvz8eIqdbodm4Wh6IsMA83Esss10jlKqSolrFrqj_Lcj5CPRhmkd8jjBYSheOLWW7D4qXHAB5hsc3h9bqpLRKldJz1D9xfq2gVQuYT8ovURIvriE7RwGbMttWcgUG7L7YNniBrKDoKFM0JcaKIVKO5o2lYkACkV9BCG75N5_RyUCCqB6N-Zrw5soEBcUyPPck6GGCiASkAFQQGRgpcjzDl-xFjuUKirR8XizhRbl0zo7SnG4IwkzaHKS42gumWFSaNflwdc",
        "DV": "UzSHswCFDcQUAJonGalrPPXF-XeVtxk",
    }

    resp = requests.get(TEST_URL, headers=headers, cookies=cookies)
    resp.raise_for_status()

    payload = parse_payload(resp.text)
    review_count = count_review_strings(payload)
    print(f"Found {review_count} strings containing 'review'")
    if review_count >= 5:
        print("✅ Payload contains at least 5 review strings.")
    else:
        print("❌ Payload contains fewer than 5 review strings.")


if __name__ == "__main__":
    main()
