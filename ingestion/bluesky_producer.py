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

def fetch_bluesky(company):
    url = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
    params = {"q": company, "limit": 25}
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        return data.get('posts', [])
    except Exception as e:
        print(f"Error fetching Bluesky for {company}: {e}")
        return []

def publish_to_kafka(company, posts):
    for post in posts:
        record = post.get('record', {})
        author = post.get('author', {})
        message = {
            "source": "bluesky",
            "company": company,
            "text": record.get('text', ''),
            "url": f"https://bsky.app/profile/{author.get('handle', '')}/post/{post.get('uri', '').split('/')[-1]}",
            "score": post.get('likeCount', 0),
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "author": author.get('handle', ''),
                "repost_count": post.get('repostCount', 0),
                "reply_count": post.get('replyCount', 0)
            }
        }
        producer.send('raw.bluesky', value=message)
        print(f"Published: [{company}] {message['text'][:60]}...")

def run():
    print("Starting Bluesky producer...")
    while True:
        for company in COMPANIES:
            posts = fetch_bluesky(company)
            if posts:
                publish_to_kafka(company, posts)
            time.sleep(3)
        print("Cycle complete. Sleeping 5 minutes...")
        time.sleep(300)

if __name__ == "__main__":
    run()