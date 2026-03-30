import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

COMPANIES = [
    "OpenAI", "Anthropic", "Notion", "Linear", "Vercel",
    "Figma", "Stripe", "Airbnb", "Databricks", "Snowflake",
    "GitHub", "Hugging Face", "Mistral", "xAI", "Perplexity",
    "Cursor", "Replit", "Warp", "n8n", "Supabase"
]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_github(company):
    url = "https://api.github.com/search/repositories"
    params = {
        "q": company,
        "sort": "stars",
        "order": "desc",
        "per_page": 10
    }
    headers = {"Accept": "application/vnd.github.v3+json"}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        data = response.json()
        return data.get('items', [])
    except Exception as e:
        print(f"Error fetching GitHub for {company}: {e}")
        return []

def publish_to_kafka(company, repos):
    for repo in repos:
        message = {
            "source": "github",
            "company": company,
            "text": repo.get("description") or repo.get("name", ""),
            "url": repo.get("html_url", ""),
            "score": repo.get("stargazers_count", 0),
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "repo_name": repo.get("full_name", ""),
                "language": repo.get("language", ""),
                "forks": repo.get("forks_count", 0),
                "open_issues": repo.get("open_issues_count", 0)
            }
        }
        producer.send('raw.github', value=message)
        print(f"Published: [{company}] {repo.get('full_name', '')} ⭐{message['score']}")

def run():
    print("Starting GitHub producer...")
    while True:
        for company in COMPANIES:
            repos = fetch_github(company)
            if repos:
                publish_to_kafka(company, repos)
            time.sleep(5)
        print("Cycle complete. Sleeping 30 minutes...")
        time.sleep(1800)

if __name__ == "__main__":
    run()