import asyncio
import os
from crawl4ai import AsyncWebCrawler
from astrapy import DataAPIClient
from datetime import datetime

# ------------------ ASTRA DB SETUP ------------------
ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_TOKEN")
ASTRA_DB_ENDPOINT = os.getenv("ASTRA_DB_ENDPOINT")
COLLECTION_NAME = "pib_press_releases"

client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_ENDPOINT)
collection = db.get_collection(COLLECTION_NAME)
# ---------------------------------------------------

BASE_URL = "https://www.pib.gov.in"

def normalize_pib_url(raw_link: str) -> str | None:
    """
    Converts any PIB link into a valid absolute URL.
    Returns None if link is invalid.
    """
    if not raw_link:
        return None

    raw_link = raw_link.strip()

    # Case 1: already full URL
    if raw_link.startswith("http://") or raw_link.startswith("https://"):
        return raw_link

    # Case 2: relative PIB link
    if raw_link.startswith("/"):
        return BASE_URL + raw_link

    # Anything else is invalid
    return None


async def main():
    print("⏳ Fetching PIB press releases...")

    added_count = 0
    skipped_count = 0

    url = "https://www.pib.gov.in/allRel.aspx?reg=3&lang=1"

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url)

    text = result.markdown

    if "Displaying" not in text:
        print("No news found")
        return

    # Trim header/footer junk
    text = text.split("Displaying", 1)[1]
    for stop in ["![Link mygov.in]", "RTI and Contact Us"]:
        if stop in text:
            text = text.split(stop, 1)[0]

    current_ministry = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        clean = line.lstrip("*# ").strip()

        # -------- MINISTRY DETECTION --------
        is_ministry = (
            clean.startswith("Ministry")
            or clean.endswith("Office")
            or clean == "AYUSH"
            or clean == "PIB Headquarters"
            or clean.isupper()
        )

        if is_ministry and len(clean) < 80:
            current_ministry = clean
            continue

        # -------- NEWS ITEM --------
        if not (line.startswith("* [") and current_ministry):
            continue

        title = line.split("](", 1)[0].replace("* [", "").strip()
        raw_link = line.split("](", 1)[1].split(")", 1)[0]

        link = normalize_pib_url(raw_link)

        # 🚨 CRITICAL SAFETY CHECK
        if not link:
            continue

        if not link.startswith("https://www.pib.gov.in/PressReleasePage.aspx"):
            continue

        existing = collection.find_one({"url": link})

        if existing:
            skipped_count += 1
        else:
            collection.insert_one({
                "ministry": current_ministry,
                "title": title,
                "url": link,
                "source": "PIB",
                "date": datetime.utcnow().strftime("%Y-%m-%d")
            })
            added_count += 1

    print("\n✅ Done")
    print(f"🆕 New items added: {added_count}")
    print(f"⏭️ Skipped (already existed): {skipped_count}")


if __name__ == "__main__":
    asyncio.run(main())
