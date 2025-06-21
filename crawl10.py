# crawl10.py
import asyncio
import json
import pandas as pd
from pathlib import Path
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode

CSV = "hoteles10.csv"
OUTPUT_DIR = Path("crawl_results")
OUTPUT_DIR.mkdir(exist_ok=True)

browser_cfg = BrowserConfig(headless=True, verbose=False)
run_cfg = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    word_count_threshold=20,
    exclude_external_links=True,
    screenshot=False
)

async def crawl_hotel(url, hotel_id):
    try:
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            result = await crawler.arun(url=url, config=run_cfg)
            out = {
                "url": result.url,
                "success": result.success,
                "status_code": result.status_code,
                "markdown": result.markdown.raw_markdown,
                "links": result.links,
                "media": result.media
            }
            path = OUTPUT_DIR / f"{hotel_id}.json"
            path.write_text(json.dumps(out, indent=2, ensure_ascii=False))
            print(f"✅ [{hotel_id}] Guardado: {path}")
    except Exception as e:
        print(f"❌ [{hotel_id}] Error procesando {url}: {e}")

async def main():
    df = pd.read_csv(CSV)
    subset = df.dropna(subset=['url_principal']).head(10)

    tasks = [
        crawl_hotel(row.url_principal, row.id_hotel)
        for _, row in subset.iterrows()
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
