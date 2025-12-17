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

# Common tech skills for extraction
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
        if response.status_code == 403 or response.status_code == 429:
            wait_time = random.randint(60, 120)
            logger.warning(
                f"Rate limited (Status {response.status_code}). Sleeping for {wait_time}s...")
            time.sleep(wait_time)
            return True
        return False


class GitHubScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.token = os.getenv("SCRAPE_GITHUB_TOKEN")

    def discover_active_users(self, target=5000):
        # Expanded topics to ensure 10k target
        topics = [
            'python', 'javascript', 'machine-learning', 'react', 'go', 'rust',
            'data-science', 'devops', 'web3', 'cybersecurity', 'android', 'ios',
            'cloud-native', 'ethereum', 'automation', 'backend', 'frontend'
        ]
        total_saved = 0

        for topic in topics:
            if total_saved >= target:
                break
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
                        if self.fetch_user_detail(repo['owner']['url']):
                            total_saved += 1
                        if total_saved >= target:
                            break
                        time.sleep(random.uniform(1.0, 2.5))
            except Exception as e:
                logger.error(f"GitHub Search failed: {e}")

    def fetch_user_detail(self, url):
        headers = self.get_headers(referer="https://github.com/")
        if self.token:
            headers['Authorization'] = f'token {self.token}'
        try:
            resp = self.session.get(url, headers=headers)
            if resp.status_code == 200:
                raw = resp.json()
                self.normalize_and_save(raw)
                return True
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
                "reputation_score": -1, "citation_count": -1, "h_index": -1, "publication_count": -1,
                "tier": "", "competitions_count": -1, "medals": {"gold": -1, "silver": -1, "bronze": -1},
                "profile_views": -1
            },
            "skills": skills, "affiliations": [], "publications": []
        }
        self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(f"[DUPLICATED] Skipping GitHub: {doc['basics']['name']}")
                return
            self.collection.update_one(
                {'source_platform': doc['source_platform'],
                    'source_id': doc['source_id']},
                {'$set': doc}, upsert=True
            )
            print(f"[NEW]        Saved GitHub: {doc['basics']['name']}")
        except Exception as e:
            logger.error(f"Error: {e}")


class StackOverflowScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.api_url = "https://api.stackexchange.com/2.3/users"

    def scrape_n_users(self, target=3000):
        page = 1
        total_saved = 0
        while total_saved < target:
            params = {"site": "stackoverflow", "page": page,
                      "pagesize": 100, "order": "desc", "sort": "reputation"}
            try:
                resp = self.session.get(
                    self.api_url, params=params, headers=self.get_headers())
                if self.handle_rate_limit(resp):
                    continue
                if resp.status_code == 200:
                    items = resp.json().get('items', [])
                    if not items:
                        break
                    for item in items:
                        self.normalize_and_save(item)
                        total_saved += 1
                        if total_saved >= target:
                            break
                    page += 1
                    time.sleep(1.5)
                else:
                    time.sleep(10)
            except Exception:
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
                "followers": -1, "following": -1, "contribution_count": -1, "citation_count": -1,
                "h_index": -1, "publication_count": -1, "tier": "", "competitions_count": -1,
                "medals": {"gold": -1, "silver": -1, "bronze": -1}
            },
            "skills": ["Software Development"], "affiliations": [], "publications": []
        }
        self.save_to_db(norm)

    def save_to_db(self, doc):
        exists = self.collection.find_one(
            {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
        if exists:
            print(f"[DUPLICATED] Skipping SO: {doc['basics']['name']}")
            return
        self.collection.update_one({'source_platform': doc['source_platform'], 'source_id': doc['source_id']}, {
                                   '$set': doc}, upsert=True)
        print(f"[NEW]        Saved SO: {doc['basics']['name']}")


class ORCIDScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.search_url = "https://pub.orcid.org/v3.0/search"
        self.base_url = "https://pub.orcid.org/v3.0"

    def scrape_by_keywords(self, target=2000):
        # Expanded keywords
        keywords = ["Machine Learning", "Quantum", "Bioinformatics",
                    "Climate", "Cryptography", "Robotics", "Genomics"]
        total = 0
        for kw in keywords:
            if total >= target:
                break
            logger.info(f"ORCID: Querying {kw}")
            start = 0
            while start < 400:
                params = {"q": kw, "rows": 50, "start": start}
                try:
                    resp = self.session.get(
                        self.search_url, headers=self.get_headers(), params=params)
                    results = resp.json().get('result', [])
                    if not results:
                        break
                    for r in results:
                        oid = r['orcid-identifier']['path']
                        if self.fetch_details(oid):
                            total += 1
                        if total >= target:
                            break
                    start += 50
                    time.sleep(1)
                except:
                    break

    def fetch_details(self, orcid_id):
        headers = self.get_headers(referer="https://orcid.org/")
        headers['Accept'] = 'application/json'
        try:
            resp = self.session.get(
                f"{self.base_url}/{orcid_id}", headers=headers)
            if resp.status_code == 200:
                self.normalize_and_save(resp.json(), orcid_id)
                return True
        except Exception:
            pass
        return False

    def normalize_and_save(self, raw, orcid_id):
        person = raw.get('person', {})
        name = person.get('name', {})
        full_name = f"{name.get('given-names', {}).get('value', '')} {name.get('family-name', {}).get('value', '')}".strip()
        activities = raw.get('activities-summary', {})

        # Skill extraction from researcher bio
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
                "followers": -1, "following": -1, "reputation_score": -1, "contribution_count": -1,
                "citation_count": -1, "h_index": -1, "tier": "", "competitions_count": -1,
                "medals": {"gold": -1, "silver": -1, "bronze": -1}, "profile_views": -1
            },
            "skills": skills, "affiliations": [], "publications": []
        }
        self.save_to_db(norm)

    def save_to_db(self, doc):
        exists = self.collection.find_one(
            {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
        if exists:
            return
        self.collection.update_one({'source_platform': doc['source_platform'], 'source_id': doc['source_id']}, {
                                   '$set': doc}, upsert=True)
        print(f"[NEW]        Saved ORCID: {doc['basics']['name']}")


class KaggleScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)

    def discover_and_scrape(self, limit=500):
        logger.info("Kaggle: Discovering users...")
        feed_url = "https://www.kaggle.com/code"
        try:
            resp = self.session.get(feed_url, headers=self.get_headers(
                referer="https://www.google.com"))
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
                if self.scrape_profile(u):
                    count += 1
                time.sleep(random.uniform(4, 7))
        except Exception:
            pass

    def scrape_profile(self, username):
        url = f"https://www.kaggle.com/{username}"
        try:
            resp = self.session.get(url, headers=self.get_headers())
            if resp.status_code == 200:
                self.parse_html(resp.text, username)
                return True
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
                "tier": "Contributor", "followers": -1, "following": -1, "reputation_score": -1,
                "contribution_count": -1, "citation_count": -1, "h_index": -1, "publication_count": -1,
                "competitions_count": -1, "medals": {"gold": -1, "silver": -1, "bronze": -1}, "profile_views": -1
            },
            "skills": Normalizer.extract_skills(desc), "affiliations": [], "publications": []
        }
        self.save_to_db(norm)

    def save_to_db(self, doc):
        exists = self.collection.find_one(
            {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
        if exists:
            return
        self.collection.update_one({'source_platform': doc['source_platform'], 'source_id': doc['source_id']}, {
                                   '$set': doc}, upsert=True)
        print(f"[NEW]        Saved Kaggle: {doc['basics']['name']}")


if __name__ == "__main__":
    db = DBManager().connect()
    col = db['profiles']
    print("=== STARTING 10K+ ENHANCED SCRAPE ===")

    StackOverflowScraper(col).scrape_n_users(3000)
    GitHubScraper(col).discover_active_users(5000)
    ORCIDScraper(col).scrape_by_keywords(2000)
    KaggleScraper(col).discover_and_scrape(500)

    print("=== MASS SCRAPE COMPLETE ===")
