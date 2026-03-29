"""
sudan_news.py
Fetches Sudan-focused news from English-language RSS feeds, categorizes
stories, and writes them to docs/sudan_news.json — capped at 20 per
category, max age 7 days, oldest entries replaced first.
No external APIs are used. All sources publish in English.
Note: The New Humanitarian feed covers all countries — Sudan-specific
stories are filtered by keyword matching in the classify() function.
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateparser
import feedparser

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = "docs"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "sudan_news.json")
MAX_PER_CATEGORY = 20
MAX_AGE_DAYS = 7
CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# Sudan-specific anchor terms used to filter feeds that cover multiple countries
SUDAN_ANCHORS = [
    "sudan", "sudanese", "khartoum", "darfur", "saf", "rsf",
    "rapid support forces", "omdurman", "port sudan", "el fasher",
    "janjaweed", "al-burhan", "dagalo", "hemedti",
]

# RSS feeds — all free, English-language, Sudan-focused, no APIs
FEEDS = [
    # Sudan Tribune — leading English-language Sudan news portal (mirror URL)
    {"source": "Sudan Tribune", "url": "https://sudantribune.net/feed/",
     "require_sudan": False},
    # Dabanga Radio — independent Sudanese broadcaster, English service
    {"source": "Dabanga", "url": "https://www.dabangasudan.org/en/feed/",
     "require_sudan": False},
    # Al Jazeera — dedicated Sudan section (English)
    {"source": "Al Jazeera", "url": "https://www.aljazeera.com/where/sudan/feed",
     "require_sudan": False},
    # AllAfrica — Sudan-specific aggregator feed (100+ African sources)
    {"source": "AllAfrica", "url": "https://allafrica.com/tools/headlines/rdf/sudan/index.xml",
     "require_sudan": False},
    # The New Humanitarian — all-countries feed, filtered by Sudan keywords
    {"source": "The New Humanitarian", "url": "https://www.thenewhumanitarian.org/rss/all.xml",
     "require_sudan": True},
    # France 24 — Sudan tag feed (English)
    {"source": "France 24", "url": "https://www.france24.com/en/tag/sudan/rss",
     "require_sudan": False},
]

# ---------------------------------------------------------------------------
# Category keyword mapping (Sudan-contextualised)
# ---------------------------------------------------------------------------

CATEGORY_KEYWORDS = {
    "Diplomacy": [
        "diplomacy", "diplomatic", "foreign policy", "embassy", "ambassador",
        "treaty", "bilateral", "multilateral", "united nations", "un security",
        "foreign minister", "foreign affairs", "summit", "sanctions",
        "international relations", "geopolitical", "arab league", "igad",
        "african union", "accord", "alliance", "envoy", "consul",
        "peace talks", "ceasefire", "ceasefire deal", "negotiations",
        "al-burhan", "hemedti", "dagalo", "sudanese government",
        "sudan and egypt", "sudan and ethiopia", "sudan and chad",
        "sudan and uae", "sudan and russia", "sudan and us",
        "un resolution", "un envoy", "un mission", "unamis",
        "jeddah talks", "jeddah negotiations", "peace process",
        "transitional government", "civilian government",
    ],
    "Military": [
        "military", "army", "navy", "air force", "defence", "defense",
        "troops", "soldier", "weapons", "missile", "armed forces",
        "war", "combat", "conflict", "bomb", "explosion", "airstrike",
        "shelling", "gunfire", "saf", "rapid support forces", "rsf",
        "janjaweed", "militia", "paramilitary", "fighting", "battle",
        "offensive", "attack", "ambush", "siege", "blockade",
        "killed", "casualties", "wounded", "martyrs",
        "khartoum fighting", "darfur conflict", "el fasher",
        "omdurman battle", "north darfur", "south darfur",
        "west darfur", "kordofan", "blue nile conflict",
        "genocide", "war crimes", "atrocities", "massacre",
    ],
    "Energy": [
        "energy", "oil", "gas", "petroleum", "sudanese oil",
        "greater nile", "gnpoc", "pipelines", "refinery",
        "renewable", "solar", "wind", "electricity", "power grid",
        "blackout", "power cut", "fuel", "diesel", "fuel shortage",
        "climate", "emissions", "environment", "nile water",
        "gerd", "grand renaissance dam", "nile dam",
        "nile basin", "water rights", "irrigation",
        "energy crisis", "power station", "generator",
        "humanitarian fuel", "fuel blockade",
    ],
    "Economy": [
        "economy", "economic", "gdp", "inflation", "unemployment",
        "jobs", "budget", "finance", "tax", "investment", "business",
        "trade", "sudanese pound", "sdg", "currency", "exchange rate",
        "imf", "world bank", "donor", "aid", "humanitarian aid",
        "reconstruction", "development", "debt relief",
        "gold", "gold mining", "agricultural", "exports", "imports",
        "food security", "famine", "hunger", "starvation",
        "world food programme", "wfp", "sanctions relief",
        "economic collapse", "hyperinflation", "poverty",
        "banking", "financial system", "remittance",
        "port sudan economy", "trade routes",
    ],
    "Local Events": [
        "local", "state", "province", "community", "hospital", "school",
        "crime", "court", "flood", "drought", "fire", "transport",
        "protest", "displacement", "refugee", "idp",
        "khartoum", "omdurman", "port sudan", "el fasher",
        "el obeid", "kassala", "wad medani", "gedaref",
        "atbara", "dongola", "nyala", "zalingei",
        "darfur", "kordofan", "blue nile", "white nile",
        "red sea state", "gezira", "al jazirah",
        "displaced people", "internally displaced",
        "humanitarian crisis", "humanitarian access",
        "civilian", "civilian casualties", "civilian harm",
        "rape", "sexual violence", "gbv", "looting",
        "famine", "malnutrition", "cholera", "disease outbreak",
        "camp", "refugee camp", "shelter", "aid delivery",
        "election", "transitional", "civil society", "protest",
    ],
}


def is_sudan_story(title: str, description: str) -> bool:
    """Check whether a story is meaningfully about Sudan."""
    text = (title + " " + (description or "")).lower()
    return any(anchor in text for anchor in SUDAN_ANCHORS)


def classify(title: str, description: str):
    """Return the best-matching category for a story, or None if no match."""
    text = (title + " " + (description or "")).lower()
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if re.search(r'\b' + re.escape(kw) + r'\b', text):
                scores[cat] += 1
    best_cat = max(scores, key=scores.get)
    return best_cat if scores[best_cat] > 0 else None


def strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def parse_date(entry):
    """Parse a feed entry's published date into a UTC-aware datetime."""
    raw = entry.get("published") or entry.get("updated") or entry.get("created")
    if not raw:
        struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if struct:
            return datetime(*struct[:6], tzinfo=timezone.utc)
        return None
    try:
        dt = dateparser.parse(raw)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc) if dt else None
    except Exception:
        return None


def fetch_feed(feed_cfg: dict) -> list:
    """Fetch a single RSS feed and return a list of story dicts."""
    source = feed_cfg["source"]
    url = feed_cfg["url"]
    require_sudan = feed_cfg.get("require_sudan", False)
    stories = []
    try:
        parsed = feedparser.parse(url)
        if parsed.bozo and not parsed.entries:
            log.warning("Bozo feed (%s): %s", source, url)
            return stories
        cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        for entry in parsed.entries:
            pub_date = parse_date(entry)
            if pub_date and pub_date < cutoff:
                continue
            title = strip_html(entry.get("title", "")).strip()
            desc = strip_html(entry.get("summary", "")).strip()
            if not title:
                continue
            # For multi-country feeds, require Sudan mention
            if require_sudan and not is_sudan_story(title, desc):
                continue
            category = classify(title, desc)
            if not category:
                continue
            story = {
                "title": title,
                "source": source,
                "url": entry.get("link", ""),
                "published_date": pub_date.isoformat() if pub_date else None,
                "category": category,
            }
            stories.append(story)
    except Exception as exc:
        log.error("Failed to fetch %s (%s): %s", source, url, exc)
    return stories


def load_existing() -> dict:
    """Load the current JSON file, grouped by category."""
    if not os.path.exists(OUTPUT_FILE):
        return {cat: [] for cat in CATEGORIES}
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {cat: [] for cat in CATEGORIES}

    grouped = {cat: [] for cat in CATEGORIES}
    stories = data.get("stories", data) if isinstance(data, dict) else data
    if isinstance(stories, list):
        for story in stories:
            cat = story.get("category")
            if cat in grouped:
                grouped[cat].append(story)
    return grouped


def merge(existing: dict, fresh: list) -> dict:
    """
    Merge fresh stories into the existing pool.
    - De-duplicate by URL.
    - Discard stories older than MAX_AGE_DAYS.
    - Replace oldest entries first when over MAX_PER_CATEGORY.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)

    existing_urls = set()
    for stories in existing.values():
        for s in stories:
            if s.get("url"):
                existing_urls.add(s["url"])

    for story in fresh:
        cat = story.get("category")
        if cat not in existing:
            continue
        if story["url"] in existing_urls:
            continue
        existing[cat].append(story)
        existing_urls.add(story["url"])

    for cat in CATEGORIES:
        pool = existing[cat]
        # Drop expired stories
        pool = [
            s for s in pool
            if s.get("published_date") and
               dateparser.parse(s["published_date"]).astimezone(timezone.utc) >= cutoff
        ]
        # Sort newest-first, cap at limit (oldest replaced first)
        pool.sort(key=lambda s: s.get("published_date") or "", reverse=True)
        existing[cat] = pool[:MAX_PER_CATEGORY]

    return existing


def write_output(grouped: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    flat = []
    for stories in grouped.values():
        flat.extend(stories)
    output = {
        "country": "Sudan",
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "story_count": len(flat),
        "stories": flat,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)
    log.info("Wrote %d stories to %s", len(flat), OUTPUT_FILE)


def main():
    log.info("Loading existing data ...")
    existing = load_existing()

    log.info("Fetching %d RSS feeds ...", len(FEEDS))
    fresh = []
    for cfg in FEEDS:
        results = fetch_feed(cfg)
        log.info("  %s — %d stories from %s", cfg["source"], len(results), cfg["url"])
        fresh.extend(results)
        time.sleep(0.5)  # polite crawl delay

    log.info("Merging %d fresh stories ...", len(fresh))
    merged = merge(existing, fresh)

    counts = {cat: len(merged[cat]) for cat in CATEGORIES}
    log.info("Category totals: %s", counts)

    write_output(merged)


if __name__ == "__main__":
    main()
