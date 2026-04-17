import json
from engines.selenium_scraper_v30 import scrape_product_v30

TARGET_URL = "https://www.goldenscent.com/en/p/saeed-salah-heaven-on-earth-eau-de-parfum-for-men-and-women.html"
STORE_URL = "https://www.goldenscent.com/"


def main():
    result = scrape_product_v30(TARGET_URL, store_url=STORE_URL)
    print(json.dumps({
        "target": TARGET_URL,
        "success": result.get("success"),
        "name": result.get("name"),
        "price": result.get("price"),
        "source": result.get("source"),
        "error": result.get("error"),
        "elapsed_sec": result.get("elapsed_sec"),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
