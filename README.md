**AI-Powered News Intelligence Pipeline**
An end-to-end automated data pipeline that collects world news every hour, analyzes it using Google Gemini AI, and generates professional 
intelligence briefings — fully autonomously with zero human intervention.

**SYSTEM ARCHITECTURE**
┌─────────────────────────────────────────────────────────────────┐
│                     APACHE AIRFLOW (Docker)                      │
│                                                                   │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐      │
│  │  FETCH  │───▶│  CLEAN  │───▶│   AI    │───▶│  SAVE   │      │
│  │  NEWS   │    │ FILTER  │    │ ANALYZE │    │  DATA   │      │
│  └─────────┘    └─────────┘    └─────────┘    └────┬────┘      │
│       │                             │               │            │
│  3 RSS Feeds               Gemini AI API      ┌────▼────┐      │
│  BBC, Reuters,             Summary +          │   CSV   │      │
│  Sky News                  Sentiment +        │ SQLite  │      │
│                            Category           └─────────┘      │
│                                                     │            │
│                                               ┌─────▼─────┐    │
│                                               │  AI DAILY │    │
│                                               │  REPORT   │    │
│                                               └───────────┘    │
└─────────────────────────────────────────────────────────────────┘
**Pipeline Flow — Step by Step**
TASK 1: FETCH NEWS
BBC World RSS ──┐
Reuters RSS ────┼──▶ Parse XML ──▶ Extract Fields ──▶ XCom Push
Sky News RSS ───┘

Fields extracted:
- title
- description
- link
- published date
- source
- fetched timestamp

**Task 2 — clean_news**
Raw Articles ──▶ Remove Duplicates ──▶ Remove Empty Titles
                        ──▶ Strip HTML Tags
                        ──▶ Decode Special Characters
                        ──▶ Clean Articles ──▶ XCom Push

**TASK 3 - AI-analyze**
Clean Articles ──▶ For each article:
                        │
                        ▼
                   Call Gemini API
                        │
                        ▼
                   Returns JSON:
                   {
                     "summary": "...",
                     "sentiment": "positive/negative/neutral",
                     "sentiment_reason": "...",
                     "category": "politics/economy/..."
                   }
                        │
                        ▼
                   Analyzed Articles ──▶ XCom Push

**Task 4 — save_to_csv**

Analyzed Articles ──▶ Check if file exists
                            │
                       ┌────▼────┐
                       │  New?   │
                       └────┬────┘
                    Yes ─────┴───── No
                     │               │
               Write Header    Append Only
                     │               │
                     └───────┬───────┘
                             ▼
                      world_news.csv 

**Task 5 — save_to_sqlite**

Analyzed Articles ──▶ Connect to SQLite
                            │
                            ▼
                     CREATE TABLE IF
                     NOT EXISTS ai_news
                            │
                            ▼
                     INSERT each article
                            │
                            ▼
                     Commit & Close 

**Task 6 — ai_daily_report**

Analyzed Articles ──▶ Count sentiments
                            │
                            ▼
                     Build headlines text
                            │
                            ▼
                     Call Gemini API
                            │
                            ▼
                     AI writes 3-4 sentence
                     professional briefing
                            │
                            ▼
                     Print full report
                     to Airflow logs 
 
 
 **PROJECT STRUCTURE** 
 
  airflow-test/
├── docker-compose.yaml        # Airflow Docker setup
├── .env                       # Environment variables
├── dags/
│   ├── weather_pipeline.py    # Project 1 - Weather Pipeline
│   └── ai_news_pipeline.py    # Project 2 - AI News Pipeline
├── logs/
│   ├── ai_news.csv            # News data export
│   └── ai_news.db             # SQLite database
└── plugins/                   # Airflow plugins

HOW TO RUN:
# Step 1 - Clone the repo
git clone https://github.com/yourusername/airflow-test

# Step 2 - Start Airflow
docker compose up -d

# Step 3 - Open UI
http://localhost:8080

# Step 4 - Enable DAGs and trigger
# Toggle ON → ai_news_pipeline → Click ▶️
