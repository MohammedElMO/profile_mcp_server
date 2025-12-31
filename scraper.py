import requests
import time
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from db_manager import DBManager
from pymongo.errors import DuplicateKeyError
import logging
import os
import re
from pymongo import ASCENDING

# Configuration for MCP-ready Data Quality
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1'
]

TECH_KEYWORDS = [
    "Python", "JavaScript", "TypeScript", "React", "Node.js", "Go", "Rust", "C++", "Java", "Kotlin",
    "Machine Learning", "AI", "Deep Learning", "TensorFlow", "PyTorch", "AWS", "Azure", "GCP",
    "Docker", "Kubernetes", "SQL", "NoSQL", "MongoDB", "PostgreSQL", "Solidity", "Blockchain",
    "Data Science", "DevOps", "Cybersecurity", "Terraform", "Ansible", "Vue", "Angular", "Swift"
]


class Normalizer:
    @staticmethod
    def clean_str(value):
        return str(value).strip() if value else ""

    @staticmethod
    def clean_int(value):
        if value is None:
            return -1
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('+', '')
            return int(value)
        except (ValueError, TypeError):
            return -1

    @staticmethod
    def extract_skills(text):
        """Heuristic to extract skills from text content."""
        if not text:
            return []
        found = []
        for kw in TECH_KEYWORDS:
            if re.search(rf'\b{re.escape(kw)}\b', text, re.IGNORECASE):
                found.append(kw)
        return list(set(found))


class BaseScraper:
    def __init__(self, db_collection):
        self.collection = db_collection
        self.session = requests.Session()
        self.consecutive_429 = 0
        self.consecutive_duplicates = 0
        # If we hit 50 duplicates in a row, stop scraping
        self.MAX_DUPLICATES_BEFORE_STOP = 50

    def get_headers(self, referer=None):
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'application/json, text/plain, */*',
        }
        if referer:
            headers['Referer'] = referer
        return headers

    def handle_rate_limit(self, response):
        """Returns True if rate limited, and handles backoff. Aborts if too many retries."""
        if response.status_code == 403 or response.status_code == 429:
            self.consecutive_429 += 1
            if self.consecutive_429 > 3:
                logger.critical(
                    f"🛑 Too many Rate Limits ({self.consecutive_429}). Aborting this scraper to protect IP.")
                raise Exception("Rate Limit Exceeded")

            wait_time = random.randint(60, 120) * self.consecutive_429
            logger.warning(
                f"⚠️ Rate limited (Status {response.status_code}). Sleeping for {wait_time}s...")
            time.sleep(wait_time)
            return True

        # Reset counter on success
        self.consecutive_429 = 0
        return False

    def check_duplicate_stop(self):
        """Checks if we should stop due to finding too many existing profiles."""
        if self.consecutive_duplicates >= self.MAX_DUPLICATES_BEFORE_STOP:
            logger.info(
                f"🛑 Hit {self.consecutive_duplicates} duplicates in a row. Assuming data is up to date. Stopping.")
            return True
        return False

# --- 1. GITHUB SCRAPER ---


class GitHubScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.token = os.getenv("SCRAPE_GITHUB_TOKEN")

    def discover_active_users(self, target=5000):
        topics = [
            'python', 'javascript', 'machine-learning', 'react', 'go', 'rust',
            'data-science', 'devops', 'web3', 'cybersecurity', 'android', 'ios'
        ]
        total_saved = 0

        for topic in topics:
            if total_saved >= target:
                break
            if self.check_duplicate_stop():
                break  # Stop if we are just rescanning

            logger.info(f"GitHub: Searching topic: {topic}")
            search_url = f"https://api.github.com/search/repositories?q=topic:{topic}&sort=stars&order=desc"

            headers = self.get_headers(referer="https://github.com/search")
            if self.token:
                headers['Authorization'] = f'token {self.token}'

            try:
                resp = self.session.get(search_url, headers=headers)
                if self.handle_rate_limit(resp):
                    continue

                if resp.status_code == 200:
                    repos = resp.json().get('items', [])
                    for repo in repos:
                        if self.check_duplicate_stop():
                            break

                        if self.fetch_user_detail(repo['owner']['url']):
                            total_saved += 1
                            self.consecutive_duplicates = 0  # Reset on success
                        else:
                            self.consecutive_duplicates += 1

                        if total_saved >= target:
                            break
                        time.sleep(random.uniform(1.0, 2.5))
            except Exception as e:
                if str(e) == "Rate Limit Exceeded":
                    break
                logger.error(f"GitHub Search failed: {e}")

    def fetch_user_detail(self, url):
        headers = self.get_headers(referer="https://github.com/")
        if self.token:
            headers['Authorization'] = f'token {self.token}'
        try:
            resp = self.session.get(url, headers=headers)
            if resp.status_code == 200:
                return self.normalize_and_save(resp.json())
            self.handle_rate_limit(resp)
        except Exception:
            pass
        return False

    def normalize_and_save(self, raw):
        username = raw.get("login") or "unknown"
        email = Normalizer.clean_str(raw.get("email"))
        if "@" not in email:
            email = f"{username}@no-email.github.com"

        bio = Normalizer.clean_str(raw.get("bio"))
        skills = Normalizer.extract_skills(bio)

        norm = {
            "source_platform": "GitHub",
            "source_id": str(raw.get("id")),
            "basics": {
                "name": Normalizer.clean_str(raw.get("name") or username),
                "headline": bio,
                "location": Normalizer.clean_str(raw.get("location")),
                "current_affiliation": Normalizer.clean_str(raw.get("company")),
                "website": Normalizer.clean_str(raw.get("blog")),
                "email": email
            },
            "metrics": {
                "followers": Normalizer.clean_int(raw.get("followers")),
                "following": Normalizer.clean_int(raw.get("following")),
                "contribution_count": Normalizer.clean_int(raw.get("public_repos")),
                "reputation_score": -1
            },
            "skills": skills, "affiliations": [], "publications": []
        }
        return self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(f"[DUPLICATED] Skipping GitHub: {doc['basics']['name']}")
                return False
            self.collection.update_one(
                {'source_platform': doc['source_platform'],
                    'source_id': doc['source_id']},
                {'$set': doc}, upsert=True
            )
            print(f"[NEW]        Saved GitHub: {doc['basics']['name']}")
            return True
        except Exception:
            return False

# --- 2. STACKOVERFLOW SCRAPER ---


class StackOverflowScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.api_url = "https://api.stackexchange.com/2.3/users"

    def get_lowest_reputation(self):
        """Finds the lowest reputation score in our DB to resume scraping from."""
        try:
            record = self.collection.find_one(
                {"source_platform": "StackOverflow"},
                # Get the lowest
                sort=[("metrics.reputation_score", ASCENDING)]
            )
            if record and record.get('metrics'):
                return record['metrics'].get('reputation_score')
        except Exception:
            pass
        return None

    def scrape_n_users(self, target=3000):
        # 1. SMART RESUME: Start from where we left off (lowest reputation)
        last_rep = self.get_lowest_reputation()

        page = 1
        total_saved = 0

        logger.info(
            f"StackOverflow: Starting scrape. Lowest known reputation in DB: {last_rep}")

        while total_saved < target:
            if self.check_duplicate_stop():
                break

            params = {
                "site": "stackoverflow",
                "page": page,
                "pagesize": 100,
                "order": "desc",
                "sort": "reputation"
            }
            # If we have data, only ask for users WORSE than our worst user
            # This skips everyone we already have.
            if last_rep:
                params['max'] = last_rep - 1

            try:
                resp = self.session.get(
                    self.api_url, params=params, headers=self.get_headers())
                if self.handle_rate_limit(resp):
                    continue

                if resp.status_code == 200:
                    items = resp.json().get('items', [])
                    if not items:
                        logger.info(
                            "StackOverflow: No more users returned (likely hit end of list).")
                        break

                    page_new_count = 0
                    for item in items:
                        if self.normalize_and_save(item):
                            total_saved += 1
                            page_new_count += 1
                            self.consecutive_duplicates = 0
                        else:
                            self.consecutive_duplicates += 1

                    # Update resume point for next iteration if we found new lowest
                    if items:
                        current_min = min(
                            [x.get('reputation', 999999) for x in items])
                        if last_rep is None or current_min < last_rep:
                            last_rep = current_min

                    logger.info(
                        f"StackOverflow: Page {page} done. New: {page_new_count}. Total Saved: {total_saved}. Min Rep: {last_rep}")

                    if page_new_count == 0:
                        logger.warning(
                            "StackOverflow: Entire page was duplicates. Consider checking DB sync.")

                    page += 1
                    time.sleep(2.0)
                else:
                    time.sleep(10)
            except Exception as e:
                if str(e) == "Rate Limit Exceeded":
                    break
                logger.error(f"SO Error: {e}")
                break

    def normalize_and_save(self, raw):
        user_id = str(raw.get("user_id"))
        norm = {
            "source_platform": "StackOverflow",
            "source_id": user_id,
            "basics": {
                "name": Normalizer.clean_str(raw.get("display_name")),
                "headline": "Professional Developer",
                "location": Normalizer.clean_str(raw.get("location")),
                "current_affiliation": "",
                "website": Normalizer.clean_str(raw.get("website_url")),
                "email": f"user{user_id}@no-email.stackoverflow.com"
            },
            "metrics": {
                "reputation_score": Normalizer.clean_int(raw.get("reputation")),
                "profile_views": Normalizer.clean_int(raw.get("view_count")),
                "followers": -1, "following": -1, "contribution_count": -1
            },
            "skills": ["Software Development"], "affiliations": [], "publications": []
        }
        return self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(f"[DUPLICATED] Skipping SO: {doc['basics']['name']}")
                return False
            self.collection.update_one({'source_platform': doc['source_platform'], 'source_id': doc['source_id']}, {
                                       '$set': doc}, upsert=True)
            print(f"[NEW]        Saved SO: {doc['basics']['name']}")
            return True
        except Exception:
            return False

# --- 3. ORCID SCRAPER ---


class ORCIDScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.search_url = "https://pub.orcid.org/v3.0/search"
        self.base_url = "https://pub.orcid.org/v3.0"

    def scrape_by_keywords(self, target=2000):
        keywords = ["Machine Learning", "Quantum",
                    "Bioinformatics", "Climate", "Cryptography"]
        total = 0
        for kw in keywords:
            if total >= target:
                break
            if self.check_duplicate_stop():
                break

            logger.info(f"ORCID: Querying {kw}")
            start = 0
            while start < 400:
                params = {"q": kw, "rows": 50, "start": start}
                try:
                    resp = self.session.get(
                        self.search_url, headers=self.get_headers(), params=params)
                    if self.handle_rate_limit(resp):
                        continue

                    results = resp.json().get('result', [])
                    if not results:
                        break

                    found_new_on_page = False
                    for r in results:
                        oid = r['orcid-identifier']['path']
                        if self.fetch_details(oid):
                            total += 1
                            self.consecutive_duplicates = 0
                            found_new_on_page = True
                        else:
                            self.consecutive_duplicates += 1

                        if total >= target:
                            break

                    if not found_new_on_page and len(results) > 0:
                        logger.info("ORCID: Page contained only duplicates.")

                    start += 50
                    time.sleep(1)
                except Exception as e:
                    if str(e) == "Rate Limit Exceeded":
                        break
                    break

    def fetch_details(self, orcid_id):
        headers = self.get_headers(referer="https://orcid.org/")
        headers['Accept'] = 'application/json'
        try:
            resp = self.session.get(
                f"{self.base_url}/{orcid_id}", headers=headers)
            if resp.status_code == 200:
                return self.normalize_and_save(resp.json(), orcid_id)
            self.handle_rate_limit(resp)
        except Exception:
            pass
        return False

    def normalize_and_save(self, raw, orcid_id):
        person = raw.get('person', {})
        name = person.get('name', {})
        full_name = f"{name.get('given-names', {}).get('value', '')} {name.get('family-name', {}).get('value', '')}".strip()
        activities = raw.get('activities-summary', {})
        bio_text = person.get('biography', {}).get('content', '')
        skills = Normalizer.extract_skills(bio_text)

        norm = {
            "source_platform": "ORCID",
            "source_id": orcid_id,
            "basics": {
                "name": full_name, "headline": "Researcher", "location": "",
                "current_affiliation": "", "website": f"https://orcid.org/{orcid_id}",
                "email": f"{orcid_id}@no-email.orcid.org"
            },
            "metrics": {
                "publication_count": len(activities.get('works', {}).get('group', [])),
                "followers": -1, "following": -1, "reputation_score": -1
            },
            "skills": skills, "affiliations": [], "publications": []
        }
        return self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(f"[DUPLICATED] Skipping ORCID: {doc['basics']['name']}")
                return False
            self.collection.update_one({'source_platform': doc['source_platform'], 'source_id': doc['source_id']}, {
                                       '$set': doc}, upsert=True)
            print(f"[NEW]        Saved ORCID: {doc['basics']['name']}")
            return True
        except Exception:
            return False

# --- 4. KAGGLE SCRAPER ---


class KaggleScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)

    def discover_and_scrape(self, limit=500):
        logger.info("Kaggle: Discovering users...")
        feed_url = "https://www.kaggle.com/code"
        try:
            resp = self.session.get(feed_url, headers=self.get_headers(
                referer="https://www.google.com"))
            if self.handle_rate_limit(resp):
                return

            soup = BeautifulSoup(resp.text, 'html.parser')
            users = set()
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.startswith('/') and href.count('/') == 1:
                    u = href.strip('/')
                    if len(u) > 3 and u not in ['code', 'learn', 'terms']:
                        users.add(u)

            count = 0
            for u in list(users)[:limit]:
                if self.check_duplicate_stop():
                    break
                if self.scrape_profile(u):
                    count += 1
                    self.consecutive_duplicates = 0
                else:
                    self.consecutive_duplicates += 1
                time.sleep(random.uniform(4, 7))
        except Exception:
            pass

    def scrape_profile(self, username):
        url = f"https://www.kaggle.com/{username}"
        try:
            resp = self.session.get(url, headers=self.get_headers())
            if resp.status_code == 200:
                return self.parse_html(resp.text, username)
            self.handle_rate_limit(resp)
        except Exception:
            pass
        return False

    def parse_html(self, html, username):
        soup = BeautifulSoup(html, 'html.parser')
        json_ld = soup.find('script', {'type': 'application/ld+json'})
        data = json.loads(json_ld.string) if json_ld else {}
        desc = data.get('description', "")

        norm = {
            "source_platform": "Kaggle", "source_id": username,
            "basics": {
                "name": data.get('name', username), "headline": desc, "location": "",
                "current_affiliation": "", "website": f"https://www.kaggle.com/{username}",
                "email": f"{username}@no-email.kaggle.com"
            },
            "metrics": {
                "tier": "Contributor", "followers": -1, "following": -1, "reputation_score": -1
            },
            "skills": Normalizer.extract_skills(desc), "affiliations": [], "publications": []
        }
        return self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(f"[DUPLICATED] Skipping Kaggle: {doc['basics']['name']}")
                return False
            self.collection.update_one({'source_platform': doc['source_platform'], 'source_id': doc['source_id']}, {
                                       '$set': doc}, upsert=True)
            print(f"[NEW]        Saved Kaggle: {doc['basics']['name']}")
            return True
        except Exception:
            return False

# --- 5. LINKEDIN SCRAPER (STEALTH) ---


class LinkedInScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.cookie = os.getenv("LINKEDIN_COOKIE")
        if not self.cookie:
            logger.warning(
                "LinkedInScraper initialized without Cookie. It will be skipped.")
        else:
            self.session.headers.update({
                "Cookie": f"li_at={self.cookie}",
                "Authority": "www.linkedin.com",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            })

    def search_and_scrape(self, keywords, limit=50):
        if not self.cookie:
            return
        total_scraped = 0
        for keyword in keywords:
            if total_scraped >= limit:
                break
            if self.check_duplicate_stop():
                break

            logger.info(f"LinkedIn: Searching for: {keyword}")
            search_url = f"https://www.linkedin.com/search/results/people/?keywords={keyword}&origin=SWITCH_SEARCH_VERTICAL"

            try:
                self.session.headers["Referer"] = "https://www.linkedin.com/feed/"
                resp = self.session.get(search_url)
                if self.handle_rate_limit(resp):
                    continue
                if "security-challenge" in resp.text:
                    logger.critical(
                        "🛑 LinkedIn Auth Wall detected! Stopping LinkedIn scrape.")
                    return

                soup = BeautifulSoup(resp.text, 'html.parser')
                profiles = set()
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if '/in/' in href and 'miniProfile' not in href:
                        profiles.add(href.split('?')[0])

                for purl in list(profiles):
                    if total_scraped >= limit:
                        break
                    if self.check_duplicate_stop():
                        break

                    time.sleep(random.uniform(25, 60))
                    if self.scrape_profile(purl):
                        total_scraped += 1
                        self.consecutive_duplicates = 0
                    else:
                        self.consecutive_duplicates += 1
            except Exception as e:
                if str(e) == "Rate Limit Exceeded":
                    break
                logger.error(f"LinkedIn error: {e}")

    def scrape_profile(self, profile_url):
        full_url = f"https://www.linkedin.com{profile_url}" if profile_url.startswith(
            '/') else profile_url
        try:
            resp = self.session.get(full_url)
            if resp.status_code == 200:
                return self.parse_and_save(resp.text, full_url)
        except Exception:
            pass
        return False

    def parse_and_save(self, html, url):
        soup = BeautifulSoup(html, 'html.parser')
        name = soup.find('meta', property='og:title')
        name = name['content'] if name else "Unknown"
        public_id = url.split('/in/')[-1].strip('/')
        skills = Normalizer.extract_skills(html)

        norm = {
            "source_platform": "LinkedIn",
            "source_id": public_id,
            "basics": {
                "name": Normalizer.clean_str(name),
                "headline": "LinkedIn Profile",
                "location": "", "current_affiliation": "", "website": url,
                "email": f"{public_id}@no-email.linkedin.com"
            },
            "metrics": {
                "followers": -1, "following": -1, "reputation_score": 100, "tier": "Professional"
            },
            "skills": skills, "affiliations": [], "publications": []
        }
        return self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(
                    f"[DUPLICATED] Skipping LinkedIn: {doc['basics']['name']}")
                return False
            self.collection.update_one({'source_platform': doc['source_platform'], 'source_id': doc['source_id']}, {
                                       '$set': doc}, upsert=True)
            print(f"[NEW]        Saved LinkedIn: {doc['basics']['name']}")
            return True
        except Exception:
            return False


if __name__ == "__main__":
    db = DBManager().connect()
    col = db['profiles']
    print("=== STARTING INTEGRATED MASS SCRAPE ===")

    # StackOverflowScraper(col).scrape_n_users(3000)
    GitHubScraper(col).discover_active_users(5000)
    ORCIDScraper(col).scrape_by_keywords(2000)
    KaggleScraper(col).discover_and_scrape(500)

    if os.getenv("LINKEDIN_COOKIE"):
        print("=== STARTING LINKEDIN SCRAPE ===")
        li = LinkedInScraper(col)
        li.search_and_scrape(
            ["Python", "Data Science", "React", "DevOps"], limit=50)
    else:
        logger.warning("Skipping LinkedIn: LINKEDIN_COOKIE not set.")

    print("=== MASS SCRAPE COMPLETE ===")
