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

HEADERS = {"User-Agent": "pulseboard/1.0"}

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_reddit(company):
    url = f"https://www.reddit.com/search.json?q={company}&sort=new&limit=10"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        data = response.json()
        posts = data.get('data', {}).get('children', [])
        return [p['data'] for p in posts]
    except Exception as e:
        print(f"Error fetching Reddit for {company}: {e}")
        return []

def publish_to_kafka(company, posts):
    for post in posts:
        message = {
            "source": "reddit",
            "company": company,
            "text": post.get("title", ""),
            "url": f"https://reddit.com{post.get('permalink', '')}",
            "score": post.get("score", 0),
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "author": post.get("author", ""),
                "subreddit": post.get("subreddit", ""),
                "num_comments": post.get("num_comments", 0),
                "upvote_ratio": post.get("upvote_ratio", 0)
            }
        }
        producer.send('raw.reddit', value=message)
        print(f"Published: [{company}] {message['text'][:60]}...")

def run():
    print("Starting Reddit producer...")
    while True:
        for company in COMPANIES:
            posts = fetch_reddit(company)
            if posts:
                publish_to_kafka(company, posts)
            time.sleep(3)
        print("Cycle complete. Sleeping 5 minutes...")
        time.sleep(300)

if __name__ == "__main__":
    run()