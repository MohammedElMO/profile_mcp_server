
from scraper import KaggleScraper, ORCIDScraper, GitHubScraper, StackOverflowScraper, logger
from db_manager import DBManager
import time
import random


if __name__ == "__main__":
    db = DBManager().connect()
    col = db['profiles']

    print("=== STARTING STEALTH MASS SCRAPE (Target: 10,000+) ===")

    scrapers = [
        (StackOverflowScraper(col), 3000, "scrape_n_users"),
        (GitHubScraper(col), 5000, "discover_active_users"),
        (ORCIDScraper(col), 2000, "scrape_by_keywords"),
        (KaggleScraper(col), 500, "discover_and_scrape")
    ]

    for scraper_inst, target, method_name in scrapers:
        method = getattr(scraper_inst, method_name)
        try:
            method(target)
        except Exception as e:
            logger.error(
                f"Critical error in {scraper_inst.__class__.__name__}: {e}")
        time.sleep(random.randint(10, 20))

    print("=== STEALTH MASS SCRAPE COMPLETE ===")
