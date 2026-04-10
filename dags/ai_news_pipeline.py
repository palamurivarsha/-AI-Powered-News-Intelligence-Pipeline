from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import sqlite3
import os
import json

# ✅ Paste your Gemini API key here
GEMINI_API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxx"

NEWS_FEEDS = [
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://rss.reuters.com/reuters/worldNews",
    "https://feeds.skynews.com/feeds/rss/world.xml"
]

CSV_FILE    = "/opt/airflow/logs/ai_news.csv"
DB_FILE     = "/opt/airflow/logs/ai_news.db"


# ----------------------------
# HELPER - Call Gemini API
# ----------------------------
def call_gemini(prompt):
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/"
        f"models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    )
    response = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={"contents": [{"parts": [{"text": prompt}]}]}
    )
    data = response.json()
    return data["candidates"][0]["content"]["parts"][0]["text"].strip()


# ----------------------------
# TASK 1 - Fetch News
# ----------------------------
def fetch_news(ti):
    import xml.etree.ElementTree as ET

    all_articles = []

    for feed_url in NEWS_FEEDS:
        try:
            response = requests.get(feed_url, timeout=10)
            root = ET.fromstring(response.content)

            for item in root.findall(".//item"):
                title       = item.find("title")
                link        = item.find("link")
                pubdate     = item.find("pubDate")
                description = item.find("description")

                if title is not None:
                    article = {
                        "title": title.text.strip() if title.text else "No Title",
                        "link": link.text.strip() if link is not None and link.text else "",
                        "published": pubdate.text.strip() if pubdate is not None and pubdate.text else "",
                        "description": description.text.strip()[:300] if description is not None and description.text else "",
                        "source": feed_url.split("/")[2],
                        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    all_articles.append(article)

        except Exception as e:
            print(f"Warning: Could not fetch from {feed_url}: {e}")

    print(f"Total articles fetched: {len(all_articles)}")
    ti.xcom_push(key="raw_articles", value=all_articles)


# ----------------------------
# TASK 2 - Clean & Filter
# ----------------------------
def clean_news(ti):
    articles = ti.xcom_pull(key="raw_articles", task_ids="fetch_news")

    cleaned     = []
    seen_titles = set()

    for article in articles:
        if article["title"] in seen_titles:
            continue
        if article["title"] in ["No Title", ""]:
            continue

        seen_titles.add(article["title"])

        desc = article["description"]
        for tag in ["<p>", "</p>", "<b>", "</b>", "<br/>", "<br>"]:
            desc = desc.replace(tag, "")
        desc = desc.replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'")
        article["description"] = desc.strip()

        cleaned.append(article)

    print(f"Cleaned articles: {len(cleaned)}")
    ti.xcom_push(key="clean_articles", value=cleaned)


# ----------------------------
# TASK 3 - AI Analyze
# ----------------------------
def ai_analyze(ti):
    articles = ti.xcom_pull(key="clean_articles", task_ids="clean_news")

    # Analyze top 10 articles
    articles_to_analyze = articles[:10]
    analyzed = []

    for i, article in enumerate(articles_to_analyze):
        print(f"Analyzing {i+1}/{len(articles_to_analyze)}: {article['title'][:50]}...")

        prompt = f"""Analyze this news article. Respond in JSON format only. No extra text, no markdown.

Title: {article['title']}
Description: {article['description']}

Respond with exactly this JSON:
{{
  "summary": "one sentence summary in simple English, max 20 words",
  "sentiment": "positive or negative or neutral",
  "sentiment_reason": "one short reason, max 10 words",
  "category": "one of: politics, economy, conflict, environment, health, technology, culture"
}}"""

        try:
            response = call_gemini(prompt)
            response = response.replace("```json", "").replace("```", "").strip()
            ai_result = json.loads(response)

            article["ai_summary"]        = ai_result.get("summary", "N/A")
            article["sentiment"]         = ai_result.get("sentiment", "neutral")
            article["sentiment_reason"]  = ai_result.get("sentiment_reason", "N/A")
            article["category"]          = ai_result.get("category", "general")

        except Exception as e:
            print(f"AI analysis failed: {e}")
            article["ai_summary"]        = "Analysis unavailable"
            article["sentiment"]         = "neutral"
            article["sentiment_reason"]  = "N/A"
            article["category"]          = "general"

        analyzed.append(article)

    print(f"AI analyzed {len(analyzed)} articles")
    ti.xcom_push(key="analyzed_articles", value=analyzed)


# ----------------------------
# TASK 4 - Save to CSV
# ----------------------------
def save_to_csv(ti):
    articles = ti.xcom_pull(key="analyzed_articles", task_ids="ai_analyze")

    file_exists = os.path.isfile(CSV_FILE)
    fieldnames  = [
        "title", "link", "published", "description",
        "source", "fetched_at", "ai_summary",
        "sentiment", "sentiment_reason", "category"
    ]

    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for article in articles:
            writer.writerow(article)

    print(f"Saved {len(articles)} articles to CSV")


# ----------------------------
# TASK 5 - Save to SQLite
# ----------------------------
def save_to_sqlite(ti):
    articles = ti.xcom_pull(key="analyzed_articles", task_ids="ai_analyze")

    conn   = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_news (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            title            TEXT,
            link             TEXT,
            published        TEXT,
            description      TEXT,
            source           TEXT,
            fetched_at       TEXT,
            ai_summary       TEXT,
            sentiment        TEXT,
            sentiment_reason TEXT,
            category         TEXT
        )
    """)

    for article in articles:
        cursor.execute("""
            INSERT INTO ai_news (
                title, link, published, description,
                source, fetched_at, ai_summary,
                sentiment, sentiment_reason, category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            article["title"],
            article["link"],
            article["published"],
            article["description"],
            article["source"],
            article["fetched_at"],
            article["ai_summary"],
            article["sentiment"],
            article["sentiment_reason"],
            article["category"]
        ))

    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM ai_news")
    total = cursor.fetchone()[0]
    conn.close()

    print(f"Saved to SQLite. Total records: {total}")


# ----------------------------
# TASK 6 - AI Daily Report
# ----------------------------
def ai_daily_report(ti):
    articles = ti.xcom_pull(key="analyzed_articles", task_ids="ai_analyze")

    sentiments = {"positive": 0, "negative": 0, "neutral": 0}
    categories = {}

    for article in articles:
        s = article.get("sentiment", "neutral")
        sentiments[s] = sentiments.get(s, 0) + 1
        c = article.get("category", "general")
        categories[c] = categories.get(c, 0) + 1

    headlines = "\n".join([
        f"- [{article['sentiment'].upper()}] {article['title']}"
        for article in articles[:10]
    ])

    prompt = f"""You are a world news analyst. Write a short professional daily news briefing.

Headlines:
{headlines}

Sentiment breakdown:
- Positive: {sentiments['positive']}
- Negative: {sentiments['negative']}
- Neutral: {sentiments['neutral']}

Write 3-4 sentences that:
1. Summarize the overall mood of world news today
2. Highlight the most important theme
3. End with one forward-looking sentence

Be concise and professional."""

    try:
        report = call_gemini(prompt)
    except Exception as e:
        report = "Daily report generation failed."
        print(f"Report error: {e}")

    print("\n========= AI WORLD NEWS BRIEFING =========")
    print(f"  Date       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Articles   : {len(articles)}")
    print(f"  Positive   : {sentiments['positive']}")
    print(f"  Negative   : {sentiments['negative']}")
    print(f"  Neutral    : {sentiments['neutral']}")
    print(f"  Categories : {dict(sorted(categories.items(), key=lambda x: x[1], reverse=True))}")
    print(f"\n  AI BRIEFING:\n  {report}")
    print("\n  ARTICLE ANALYSIS:")
    print("  " + "-"*40)

    for i, article in enumerate(articles[:5]):
        print(f"\n  {i+1}. {article['title'][:60]}")
        print(f"     Summary   : {article['ai_summary']}")
        print(f"     Sentiment : {article['sentiment'].upper()} - {article['sentiment_reason']}")
        print(f"     Category  : {article['category']}")

    print("\n==========================================")


# ----------------------------
# DEFINE THE DAG
# ----------------------------
with DAG(
    dag_id="ai_news_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["ai", "news", "nlp", "gemini", "free"]
) as dag:

    task1 = PythonOperator(
        task_id="fetch_news",
        python_callable=fetch_news
    )

    task2 = PythonOperator(
        task_id="clean_news",
        python_callable=clean_news
    )

    task3 = PythonOperator(
        task_id="ai_analyze",
        python_callable=ai_analyze
    )

    task4 = PythonOperator(
        task_id="save_to_csv",
        python_callable=save_to_csv
    )

    task5 = PythonOperator(
        task_id="save_to_sqlite",
        python_callable=save_to_sqlite
    )

    task6 = PythonOperator(
        task_id="ai_daily_report",
        python_callable=ai_daily_report
    )

    task1 >> task2 >> task3 >> task4 >> task5 >> task6
