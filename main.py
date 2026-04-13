"""
main.py — Databricks Sales Intelligence Demo
Flow: connect → ingest → query → ai_insights
"""

import os

import anthropic
import pandas as pd
from databricks import sql
from databricks.sql.client import Connection
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()

# ---------------------------------------------------------------------------
# SECTION 1: CONNECT
# Establish a connection to Databricks SQL Warehouse using env credentials.
# ---------------------------------------------------------------------------

def connect() -> Connection:
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    print("Connected to Databricks.")
    return conn


# ---------------------------------------------------------------------------
# SECTION 2: INGEST
# Upload sales_data.csv into a Databricks Delta table for the demo.
# Drops and recreates the table on each run so the demo stays idempotent.
# ---------------------------------------------------------------------------

TABLE_NAME = "sales_demo.sales_data"
CSV_FILE = "sales_data.csv"


def ingest(conn: Connection) -> None:
    df = pd.read_csv(CSV_FILE)
    cursor = conn.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS sales_demo")
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    # Build the table from the CSV column types
    cursor.execute(f"""
        CREATE TABLE {TABLE_NAME} (
            order_id   STRING,
            date       DATE,
            product    STRING,
            region     STRING,
            sales_rep  STRING,
            quantity   INT,
            unit_price DOUBLE,
            revenue    DOUBLE
        )
        USING DELTA
    """)

    # Insert rows in batches of 100 to stay within SQL parameter limits
    batch_size = 100
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start : start + batch_size]
        def esc(s: str) -> str:
            return str(s).replace("'", "''")

        values = ", ".join(
            f"('{esc(r.order_id)}', '{esc(r.date)}', '{esc(r.product)}', '{esc(r.region)}', "
            f"'{esc(r.sales_rep)}', {r.quantity}, {r.unit_price}, {r.revenue})"
            for r in batch.itertuples()
        )
        cursor.execute(f"INSERT INTO {TABLE_NAME} VALUES {values}")

    cursor.close()
    print(f"Ingested {len(df)} rows into {TABLE_NAME}.")


# ---------------------------------------------------------------------------
# SECTION 3: QUERY
# Run analytical SQL queries and display results as formatted tables.
# ---------------------------------------------------------------------------

QUERIES = {
    "Revenue by Region": f"""
        SELECT region,
               SUM(revenue)          AS total_revenue,
               COUNT(DISTINCT order_id) AS orders
        FROM {TABLE_NAME}
        GROUP BY region
        ORDER BY total_revenue DESC
    """,
    "Top 5 Products by Revenue": f"""
        SELECT product,
               SUM(revenue)  AS total_revenue,
               SUM(quantity) AS units_sold
        FROM {TABLE_NAME}
        GROUP BY product
        ORDER BY total_revenue DESC
        LIMIT 5
    """,
    "Top Sales Reps (Last 30 Days)": f"""
        SELECT sales_rep,
               SUM(revenue) AS total_revenue,
               COUNT(*)     AS deals_closed
        FROM {TABLE_NAME}
        WHERE date >= CURRENT_DATE - INTERVAL 30 DAYS
        GROUP BY sales_rep
        ORDER BY total_revenue DESC
        LIMIT 5
    """,
}


def query(conn: Connection) -> dict[str, pd.DataFrame]:
    results: dict[str, pd.DataFrame] = {}
    cursor = conn.cursor()

    for title, sql_text in QUERIES.items():
        cursor.execute(sql_text)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=cols)
        results[title] = df
        print(f"\n=== {title} ===")
        print(tabulate(df, headers="keys", tablefmt="rounded_outline", showindex=False))

    cursor.close()
    return results


# ---------------------------------------------------------------------------
# SECTION 4: AI INSIGHTS
# Send query results to Claude and ask for executive-level sales commentary.
# Falls back to a formatted mock insight if the API key is missing or has no
# credits, so the demo runs end-to-end without requiring API access.
# ---------------------------------------------------------------------------

AI_SYSTEM_PROMPT = (
    "You are a senior sales analyst. Given structured sales data, produce a "
    "concise executive summary (≤200 words) with: key wins, underperforming "
    "areas, and 2–3 concrete next-step recommendations. Be direct."
)


def _mock_ai_insights(results: dict[str, pd.DataFrame]) -> None:
    """Print a formatted mock insight derived from the actual query results."""
    lines = ["[Mock insight — Claude API unavailable]", ""]

    if "Revenue by Region" in results:
        df = results["Revenue by Region"]
        if not df.empty:
            top = df.iloc[0]
            lines.append(
                f"**Key Win:** {top['region']} leads all regions with "
                f"${top['total_revenue']:,.0f} in revenue across {top['orders']} orders."
            )
            if len(df) > 1:
                bottom = df.iloc[-1]
                lines.append(
                    f"**Underperforming Area:** {bottom['region']} trails at "
                    f"${bottom['total_revenue']:,.0f} — investigate pipeline gaps."
                )

    if "Top 5 Products by Revenue" in results:
        df = results["Top 5 Products by Revenue"]
        if not df.empty:
            top = df.iloc[0]
            lines.append(
                f"**Top Product:** {top['product']} drives ${top['total_revenue']:,.0f} "
                f"with {top['units_sold']} units sold."
            )
            if len(df) > 1:
                bottom = df.iloc[-1]
                lines.append(
                    f"**Lowest-Ranked Product:** {bottom['product']} at "
                    f"${bottom['total_revenue']:,.0f} — consider a promotional bundle."
                )

    if "Top Sales Reps (Last 30 Days)" in results:
        df = results["Top Sales Reps (Last 30 Days)"]
        if not df.empty:
            top = df.iloc[0]
            lines.append(
                f"**Top Rep (30d):** {top['sales_rep']} closed {top['deals_closed']} "
                f"deals for ${top['total_revenue']:,.0f}."
            )

    lines += [
        "",
        "**Recommendations:**",
        "  1. Expand rep headcount in the leading region to capitalise on momentum.",
        "  2. Bundle the top product with slower-moving SKUs to lift attach rate.",
        "  3. Schedule coaching sessions for reps outside the top 5 to close the gap.",
    ]

    print("\n".join(lines))
    print()


def ai_insights(results: dict[str, pd.DataFrame]) -> None:
    # Serialize each result table as a readable text block for the prompt
    data_block = "\n\n".join(
        f"### {title}\n{df.to_markdown(index=False)}"
        for title, df in results.items()
    )

    print("\n=== AI Sales Insights (Claude) ===")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            client = anthropic.Anthropic(api_key=api_key)
            # Stream the response so the insight appears token-by-token
            with client.messages.stream(
                model="claude-sonnet-4-6",
                max_tokens=512,
                system=AI_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": data_block}],
            ) as stream:
                for text in stream.text_stream:
                    print(text, end="", flush=True)
            print()  # trailing newline after stream ends
            return
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError, anthropic.BadRequestError) as e:
            print(f"[Claude unavailable: {e.message} — showing mock insight]\n")

    _mock_ai_insights(results)


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main() -> None:
    conn = connect()
    try:
        ingest(conn)
        results = query(conn)
        ai_insights(results)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
