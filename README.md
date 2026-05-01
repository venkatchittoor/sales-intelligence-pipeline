# Sales Intelligence Pipeline

An end-to-end sales analytics pipeline built with **Claude Code** and the Databricks **Data Intelligence Platform** — demonstrating AI-assisted development, Delta Lake ingestion, SQL analytics, and natural language insights.

> *The entire pipeline was built via natural language prompts — no code written manually. This is what AI-assisted data engineering looks like in practice.*
---

## What This Project Does

1. **Generates** 500 realistic sales transactions (products, regions, sales reps, revenue) across 90 days
2. **Ingests** the data into Databricks as a governed Delta table via Unity Catalog
3. **Analyses** it with 3 SQL queries — revenue by region, top products, sales rep leaderboard
4. **Explains** the results with an AI-generated executive summary (Claude API)

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Claude Code** | AI coding agent — wrote the entire pipeline via natural language prompts |
| **Databricks SQL Warehouse** | Serverless query engine |
| **Delta Lake** | ACID-compliant table storage with versioning & time travel |
| **Unity Catalog** | Data governance & lineage tracking |
| **AI/BI Genie** | Natural language querying over the Delta table |
| **Python** | Pipeline orchestration |
| **anthropic SDK** | AI narrative generation |
| **databricks-sql-connector** | Databricks connection from Python |

---

## Project Structure

```
sales-intelligence-pipeline/
├── main.py              # Main pipeline — connect, ingest, query, AI insights
├── generate_data.py     # Generates 500-row realistic sales CSV
├── requirements.txt     # Python dependencies
├── .env.example         # Environment variable template
└── .gitignore           # Keeps credentials out of GitHub
```

---

## Setup & Run

### Prerequisites
- Python 3.9+
- Databricks account with a SQL Warehouse
- Anthropic API key (optional — falls back to mock insights)
- Claude Code installed (`npm install -g @anthropic-ai/claude-code`)

### Installation

```bash
# Clone the repo
git clone https://github.com/venkatchittoor/sales-intelligence-pipeline.git
cd sales-intelligence-pipeline

# Install dependencies
pip3 install -r requirements.txt

# Set up credentials
cp .env.example .env
# Fill in your Databricks and Anthropic credentials in .env
```

### Run

```bash
# Generate sample data
python3 generate_data.py

# Run the full pipeline
python3 main.py
```

---

## What the Pipeline Produces

### Revenue by Region
| Region | Total Revenue | Orders |
|---|---|---|
| LATAM | $5.77M | 140 |
| North America | $5.55M | 131 |
| EMEA | $4.03M | 115 |
| APAC | $3.95M | 114 |

### Top Products by Revenue
| Product | Total Revenue |
|---|---|
| Mosaic AI | $7.41M |
| Databricks Runtime Pro | $4.30M |
| Delta Live Tables | $2.65M |

### AI Executive Summary (Claude)
> *LATAM leads all regions at $5.8M. Mosaic AI dominates the portfolio at $7.4M — nearly 2x the next product. Databricks SQL generates the highest unit volume but lowest revenue, suggesting potential underpricing.*

---

## Databricks Features Demonstrated

- **Delta Lake** — ACID transactions, time travel (`VERSION AS OF`), history tracking
- **Unity Catalog** — table registration, column lineage, data discovery
- **Databricks SQL** — serverless analytics on cloud storage
- **AI/BI Genie** — plain English querying over the Delta table
- **Medallion Architecture** — governed data storage pattern

---

## Author

**Venkat Chittoor**  
[github.com/venkatchittoor](https://github.com/venkatchittoor)
