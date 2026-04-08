from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
import os

app = FastAPI(title="PulseBoard API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# BigQuery client
credentials_path = os.path.expanduser("~/projects/pulseboard/credentials.json")
client = bigquery.Client.from_service_account_json(credentials_path)
PROJECT = "pulseboard-ai"
DATASET = "pulseboard_gold"

class ChatRequest(BaseModel):
    query: str

def run_query(sql):
    query_job = client.query(sql)
    return list(query_job.result())

@app.get("/")
def root():
    return {"status": "PulseBoard API is running", "version": "1.0"}

@app.get("/api/leaderboard")
def get_leaderboard():
    sql = f"""
        SELECT company, avg_sentiment, total_mentions, trending_score
        FROM `{PROJECT}.{DATASET}.company_sentiment`
        ORDER BY trending_score DESC
        LIMIT 10
    """
    rows = run_query(sql)
    return {
        "leaderboard": [
            {
                "company": row.company,
                "avg_sentiment": float(row.avg_sentiment),
                "total_mentions": int(row.total_mentions),
                "trending_score": float(row.trending_score)
            }
            for row in rows
        ]
    }

@app.get("/api/company/{name}/sentiment")
def get_company_sentiment(name: str):
    sql = f"""
        SELECT company, avg_sentiment, total_mentions, 
               avg_engagement_score, trending_score, last_updated
        FROM `{PROJECT}.{DATASET}.company_sentiment`
        WHERE LOWER(company) = LOWER('{name}')
        LIMIT 1
    """
    rows = run_query(sql)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")
    row = rows[0]
    sentiment = float(row.avg_sentiment)
    label = "positive" if sentiment >= 0.05 else "negative" if sentiment <= -0.05 else "neutral"
    return {
        "company": row.company,
        "avg_sentiment": sentiment,
        "sentiment_label": label,
        "total_mentions": int(row.total_mentions),
        "trending_score": float(row.trending_score),
        "last_updated": str(row.last_updated)
    }

@app.get("/api/company/{name}/sources")
def get_company_sources(name: str):
    sql = f"""
        SELECT source, mention_count, avg_sentiment
        FROM `{PROJECT}.{DATASET}.source_breakdown`
        WHERE LOWER(company) = LOWER('{name}')
        ORDER BY mention_count DESC
    """
    rows = run_query(sql)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")
    return {
        "company": name,
        "sources": [
            {
                "source": row.source,
                "mention_count": int(row.mention_count),
                "avg_sentiment": float(row.avg_sentiment)
            }
            for row in rows
        ]
    }

@app.get("/api/companies")
def get_all_companies():
    sql = f"""
        SELECT company, avg_sentiment, total_mentions, trending_score
        FROM `{PROJECT}.{DATASET}.company_sentiment`
        ORDER BY trending_score DESC
    """
    rows = run_query(sql)
    return {
        "companies": [
            {
                "company": row.company,
                "avg_sentiment": float(row.avg_sentiment),
                "total_mentions": int(row.total_mentions),
                "trending_score": float(row.trending_score)
            }
            for row in rows
        ]
    }

@app.post("/api/chat")
def chat(request: ChatRequest):
    query = request.query.lower()

    companies = [
        "openai", "anthropic", "notion", "linear", "vercel",
        "figma", "stripe", "airbnb", "databricks", "snowflake",
        "github", "hugging face", "mistral", "xai", "perplexity",
        "cursor", "replit", "warp", "n8n", "supabase"
    ]

    detected_company = None
    for company in companies:
        if company in query:
            detected_company = company
            break

    if any(word in query for word in ["leaderboard", "trending", "top", "hottest", "best"]):
        rows = run_query(f"""
            SELECT company, avg_sentiment, total_mentions, trending_score
            FROM `{PROJECT}.{DATASET}.company_sentiment`
            ORDER BY trending_score DESC LIMIT 5
        """)
        result = "\n".join([
            f"{i+1}. {r.company} — trending score: {float(r.trending_score):.2f}, sentiment: {float(r.avg_sentiment):.3f}"
            for i, r in enumerate(rows)
        ])
        return {"response": f"Top 5 trending companies right now:\n\n{result}"}

    if detected_company:
        rows = run_query(f"""
            SELECT company, avg_sentiment, total_mentions, trending_score
            FROM `{PROJECT}.{DATASET}.company_sentiment`
            WHERE LOWER(company) = '{detected_company}'
            LIMIT 1
        """)
        if rows:
            r = rows[0]
            sentiment = float(r.avg_sentiment)
            label = "positive" if sentiment >= 0.05 else "negative" if sentiment <= -0.05 else "neutral"
            return {
                "response": f"{r.company} has {label} sentiment (score: {sentiment:.3f}) "
                           f"with {int(r.total_mentions)} mentions. "
                           f"Trending score: {float(r.trending_score):.2f}."
            }

    return {
        "response": "I can answer questions about company sentiment and trending scores. "
                   "Try: 'What is the sentiment on Anthropic?' or 'Show me the leaderboard'"
    }