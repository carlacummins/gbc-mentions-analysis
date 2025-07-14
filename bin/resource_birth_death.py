import requests
import sys

def get_wayback_dates(url):
    """
    Query the Wayback CDX API for a given URL.
    Returns the first and last archived timestamps, or None if not archived.
    """
    cdx_url = "http://web.archive.org/cdx/search/cdx"
    params = {
        "url": url,
        "output": "json",
        "fl": "timestamp,original",
        "collapse": "digest"  # remove identical content versions
    }

    response = requests.get(cdx_url, params=params)
    if response.status_code != 200:
        raise Exception(f"CDX API error: {response.status_code}")

    data = response.json()
    if len(data) <= 1:
        return None  # No archive data available

    timestamps = [row[0] for row in data[1:]]  # skip header row
    timestamps.sort()
    first_seen = timestamps[0]
    last_seen = timestamps[-1]

    # Convert to readable dates (YYYY-MM-DD)
    from datetime import datetime
    first_date = datetime.strptime(first_seen, "%Y%m%d%H%M%S").date()
    last_date = datetime.strptime(last_seen, "%Y%m%d%H%M%S").date()

    return {
        "url": url,
        "first_seen": first_date,
        "last_seen": last_date
    }

# Example usage:
if __name__ == "__main__":
    # test_url = "http://example.com"
    test_url = sys.argv[1] if len(sys.argv) > 1 else "http://example.com"
    print(f"Querying Wayback for URL: {test_url}")
    result = get_wayback_dates(test_url)
    if result:
        print(f"URL: {result['url']}")
        print(f"First seen: {result['first_seen']}")
        print(f"Last seen: {result['last_seen']}")
    else:
        print("No archive data found for this URL.")