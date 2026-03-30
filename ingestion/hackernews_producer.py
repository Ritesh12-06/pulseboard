import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# Target companies to track
COMPANIES = [
    "OpenAI", "Anthropic", "Notion", "Linear", "Vercel",
    "Figma", "Stripe", "Airbnb", "Databricks", "Snowflake",
    "GitHub", "Hugging Face", "Mistral", "xAI", "Perplexity",
    "Cursor", "Replit", "Warp", "n8n", "Supabase"
]

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_hackernews(company):
    url = f"https://hn.algolia.com/api/v1/search?query={company}&tags=story&hitsPerPage=10"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        return data.get('hits', [])
    except Exception as e:
        print(f"Error fetching HN for {company}: {e}")
        return []

def publish_to_kafka(company, posts):
    for post in posts:
        message = {
            "source": "hackernews",
            "company": company,
            "text": post.get("title", ""),
            "url": post.get("url", ""),
            "score": post.get("points", 0),
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "author": post.get("author", ""),
                "num_comments": post.get("num_comments", 0),
                "hn_id": post.get("objectID", "")
            }
        }
        producer.send('raw.hackernews', value=message)
        print(f"Published: [{company}] {message['text'][:60]}...")

def run():
    print("Starting HackerNews producer...")
    while True:
        for company in COMPANIES:
            posts = fetch_hackernews(company)
            if posts:
                publish_to_kafka(company, posts)
            time.sleep(2)
        print("Cycle complete. Sleeping 5 minutes...")
        time.sleep(300)

if __name__ == "__main__":
    run()