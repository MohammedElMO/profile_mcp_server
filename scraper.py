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
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
        if not self.token:
            logger.warning(
                "No GitHub Token found! You will be limited to 60 req/hr. Set SCRAPE_GITHUB_TOKEN.")

    def discover_active_users(self, target=5000):
        topics = ['python', 'javascript', 'machine-learning',
                  'react', 'go', 'rust', 'data-science', 'devops']
        total_saved = 0

        for topic in topics:
            if total_saved >= target:
                break
            logger.info(
                f"GitHub: Searching for active users in topic: {topic}")
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
                        owner_url = repo['owner']['url']
                        if self.fetch_user_detail(owner_url):
                            total_saved += 1
                        if total_saved >= target:
                            break
                        if total_saved % 10 == 0:
                            logger.info(
                                f"GitHub Progress: {total_saved}/{target}")
                        time.sleep(random.uniform(1.5, 3.0))
                else:
                    logger.error(
                        f"GitHub Search failed with status {resp.status_code}")
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
                if not raw.get('bio') and not raw.get('location') and not raw.get('company'):
                    return False
                self.normalize_and_save(raw)
                return True
            self.handle_rate_limit(resp)
        except Exception:
            pass
        return False

    def normalize_and_save(self, raw):
        username = raw.get("login") or "unknown"
        email = Normalizer.clean_str(raw.get("email"))

        # SCHEMA FIX: email is REQUIRED and must match pattern ^.+@.+$
        # If no email found, provide a valid-looking placeholder
        if "@" not in email:
            email = f"{username}@no-email.github.com"

        basics = {
            "name": Normalizer.clean_str(raw.get("name") or username),
            "headline": Normalizer.clean_str(raw.get("bio")),
            "location": Normalizer.clean_str(raw.get("location")),
            "current_affiliation": Normalizer.clean_str(raw.get("company")),
            "website": Normalizer.clean_str(raw.get("blog")),
            "email": email
        }

        norm = {
            "source_platform": "GitHub",
            "source_id": str(raw.get("id")),
            "basics": basics,
            "metrics": {
                "followers": Normalizer.clean_int(raw.get("followers")),
                "following": Normalizer.clean_int(raw.get("following")),
                "contribution_count": Normalizer.clean_int(raw.get("public_repos")),
                "reputation_score": -1, "citation_count": -1, "h_index": -1, "publication_count": -1,
                "tier": "", "competitions_count": -1, "medals": {"gold": -1, "silver": -1, "bronze": -1},
                "profile_views": -1
            },
            "skills": [], "affiliations": [], "publications": []
        }
        self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(
                    f"[DUPLICATED] Skipping GitHub: {doc['basics']['name']} (ID: {doc['source_id']})")
                return

            self.collection.update_one(
                {'source_platform': doc['source_platform'],
                    'source_id': doc['source_id']},
                {'$set': doc},
                upsert=True
            )
            print(f"[NEW]        Saved GitHub: {doc['basics']['name']}")
        except Exception as e:
            logger.error(f"Error saving GitHub profile: {e}")


class StackOverflowScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.api_url = "https://api.stackexchange.com/2.3/users"

    def scrape_n_users(self, target=3000):
        page = 1
        total_saved = 0
        while total_saved < target:
            params = {
                "site": "stackoverflow",
                "page": page,
                "pagesize": 100,
                "order": "desc",
                "sort": "reputation"
            }
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
                    logger.info(
                        f"StackOverflow Progress: {total_saved}/{target}")
                    time.sleep(random.uniform(2, 4))
                elif resp.status_code == 400:
                    break
                else:
                    time.sleep(10)
            except Exception as e:
                logger.error(f"SO Error: {e}")
                break

    def normalize_and_save(self, raw):
        user_id = str(raw.get("user_id"))
        # SO API doesn't provide emails
        email = f"user{user_id}@no-email.stackoverflow.com"

        basics = {
            "name": Normalizer.clean_str(raw.get("display_name")),
            "headline": "Professional Developer",
            "location": Normalizer.clean_str(raw.get("location")),
            "current_affiliation": "",
            "website": Normalizer.clean_str(raw.get("website_url")),
            "email": email
        }

        norm = {
            "source_platform": "StackOverflow",
            "source_id": user_id,
            "basics": basics,
            "metrics": {
                "reputation_score": Normalizer.clean_int(raw.get("reputation")),
                "profile_views": Normalizer.clean_int(raw.get("view_count")),
                "followers": -1, "following": -1, "contribution_count": -1, "citation_count": -1,
                "h_index": -1, "publication_count": -1, "tier": "", "competitions_count": -1,
                "medals": {"gold": -1, "silver": -1, "bronze": -1}
            },
            "skills": [], "affiliations": [], "publications": []
        }
        self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(
                    f"[DUPLICATED] Skipping SO: {doc['basics']['name']} (ID: {doc['source_id']})")
                return
            self.collection.update_one(
                {'source_platform': doc['source_platform'],
                    'source_id': doc['source_id']},
                {'$set': doc},
                upsert=True
            )
            print(f"[NEW]        Saved SO: {doc['basics']['name']}")
        except Exception:
            pass


class ORCIDScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)
        self.search_url = "https://pub.orcid.org/v3.0/search"
        self.base_url = "https://pub.orcid.org/v3.0"

    def scrape_by_keywords(self, target=2000):
        keywords = ["Machine Learning", "Quantum Computing",
                    "Data Science", "Bioinformatics", "Cybersecurity", "Blockchain"]
        total = 0
        for kw in keywords:
            if total >= target:
                break
            logger.info(f"ORCID: Querying for: {kw}")
            start = 0
            while start < 500:
                params = {"q": kw, "rows": 50, "start": start}
                try:
                    resp = self.session.get(
                        self.search_url, headers=self.get_headers(), params=params)
                    if self.handle_rate_limit(resp):
                        continue

                    results = resp.json().get('result', [])
                    if not results:
                        break
                    for r in results:
                        oid = r['orcid-identifier']['path']
                        self.fetch_details(oid)
                        total += 1
                        if total >= target:
                            break
                    start += 50
                    time.sleep(random.uniform(1, 2))
                except:
                    break

    def fetch_details(self, orcid_id):
        headers = self.get_headers(referer="https://orcid.org/")
        headers['Accept'] = 'application/json'
        try:
            resp = self.session.get(
                f"{self.base_url}/{orcid_id}", headers=headers)
            if resp.status_code == 200:
                raw = resp.json()
                if not raw.get('person', {}).get('name'):
                    return
                self.normalize_and_save(raw, orcid_id)
            self.handle_rate_limit(resp)
        except Exception:
            pass

    def normalize_and_save(self, raw, orcid_id):
        person = raw.get('person', {})
        name = person.get('name', {})
        full_name = f"{name.get('given-names', {}).get('value', '')} {name.get('family-name', {}).get('value', '')}".strip()

        activities = raw.get('activities-summary', {})
        employments = activities.get(
            'employments', {}).get('employment-summary', [])
        curr_aff = employments[0].get('organization', {}).get(
            'name', '') if employments else ""

        # Construct safe email
        email = f"{orcid_id}@no-email.orcid.org"
        email_list = person.get('emails', {}).get('email', [])
        if email_list:
            found_email = Normalizer.clean_str(email_list[0].get('email'))
            if "@" in found_email:
                email = found_email

        basics = {
            "name": full_name, "headline": "Researcher", "location": "",
            "current_affiliation": Normalizer.clean_str(curr_aff),
            "website": f"https://orcid.org/{orcid_id}",
            "email": email
        }

        norm = {
            "source_platform": "ORCID",
            "source_id": orcid_id,
            "basics": basics,
            "metrics": {
                "publication_count": len(activities.get('works', {}).get('group', [])),
                "followers": -1, "following": -1, "reputation_score": -1, "contribution_count": -1,
                "citation_count": -1, "h_index": -1, "tier": "", "competitions_count": -1,
                "medals": {"gold": -1, "silver": -1, "bronze": -1}, "profile_views": -1
            },
            "skills": [], "affiliations": [], "publications": []
        }
        self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(
                    f"[DUPLICATED] Skipping ORCID: {doc['basics']['name']} (ID: {doc['source_id']})")
                return
            self.collection.update_one(
                {'source_platform': doc['source_platform'],
                    'source_id': doc['source_id']},
                {'$set': doc},
                upsert=True
            )
            print(f"[NEW]        Saved ORCID: {doc['basics']['name']}")
        except:
            pass


class KaggleScraper(BaseScraper):
    def __init__(self, db_collection):
        super().__init__(db_collection)

    def discover_and_scrape(self, limit=500):
        logger.info("Kaggle: Discovering users from feed...")
        feed_url = "https://www.kaggle.com/code"
        discovered_users = set()

        try:
            resp = self.session.get(feed_url, headers=self.get_headers(
                referer="https://www.google.com"))
            if self.handle_rate_limit(resp):
                return

            soup = BeautifulSoup(resp.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.startswith('/') and href.count('/') == 1:
                    user = href.strip('/')
                    if user not in ['code', 'datasets', 'competitions', 'discussion', 'learn', 'terms', 'privacy', 'about']:
                        discovered_users.add(user)

            logger.info(
                f"Kaggle: Found {len(discovered_users)} candidates. Starting scrape...")
            count = 0
            for user in list(discovered_users):
                if count >= limit:
                    break
                if self.scrape_profile(user):
                    count += 1
                time.sleep(random.uniform(3, 6))
        except Exception as e:
            logger.error(f"Kaggle discovery failed: {e}")

    def scrape_profile(self, username):
        url = f"https://www.kaggle.com/{username}"
        try:
            resp = self.session.get(url, headers=self.get_headers(
                referer="https://www.kaggle.com/code"))
            if resp.status_code == 200:
                self.parse_html(resp.text, username)
                return True
            self.handle_rate_limit(resp)
        except Exception:
            pass
        return False

    def parse_html(self, html, username):
        soup = BeautifulSoup(html, 'html.parser')
        json_ld = soup.find('script', {'type': 'application/ld+json'})
        data = json.loads(json_ld.string) if json_ld else {}

        name = data.get('name') or username
        description = data.get('description') or ""
        tier = "Contributor"
        if "Grandmaster" in description:
            tier = "Grandmaster"
        elif "Master" in description:
            tier = "Master"

        # Construct safe email
        email = f"{username}@no-email.kaggle.com"
        found_email = Normalizer.clean_str(data.get("email"))
        if "@" in found_email:
            email = found_email

        basics = {
            "name": Normalizer.clean_str(name),
            "headline": Normalizer.clean_str(description),
            "location": "", "current_affiliation": "",
            "website": f"https://www.kaggle.com/{username}",
            "email": email
        }

        norm = {
            "source_platform": "Kaggle",
            "source_id": username,
            "basics": basics,
            "metrics": {
                "tier": tier,
                "followers": -1, "following": -1, "reputation_score": -1, "contribution_count": -1,
                "citation_count": -1, "h_index": -1, "publication_count": -1,
                "competitions_count": -1, "medals": {"gold": -1, "silver": -1, "bronze": -1},
                "profile_views": -1
            },
            "skills": ["Data Science"], "affiliations": [], "publications": []
        }
        self.save_to_db(norm)

    def save_to_db(self, doc):
        try:
            exists = self.collection.find_one(
                {'source_platform': doc['source_platform'], 'source_id': doc['source_id']})
            if exists:
                print(
                    f"[DUPLICATED] Skipping Kaggle: {doc['basics']['name']} (ID: {doc['source_id']})")
                return
            self.collection.update_one(
                {'source_platform': doc['source_platform'],
                    'source_id': doc['source_id']},
                {'$set': doc},
                upsert=True
            )
            print(f"[NEW]        Saved Kaggle: {doc['basics']['name']}")
        except:
            pass

