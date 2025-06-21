from duckduckgo_search import DDGS

with DDGS() as ddgs:
    results = ddgs.text("hotel Le Meridien Santiago site:marriott.com", max_results=3)
    for r in results:
        print(r["href"])
