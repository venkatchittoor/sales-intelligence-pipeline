"""
generate_data.py — Generates a realistic 500-row sales CSV for the demo.
Run once before main.py: python generate_data.py
"""

import random
from datetime import datetime, timedelta

import pandas as pd

PRODUCTS = [
    ("Databricks Runtime Pro", 4500),
    ("Unity Catalog", 2200),
    ("Delta Live Tables", 3100),
    ("Mosaic AI", 8900),
    ("Databricks SQL", 1800),
    ("Lakehouse Monitoring", 1200),
]

REGIONS = ["North America", "EMEA", "APAC", "LATAM"]

SALES_REPS = [
    "Alice Chen", "Bob Martinez", "Clara Osei", "David Kim",
    "Elena Vasquez", "Frank Müller", "Grace Lin", "Hiro Tanaka",
    "Isabel Ferreira", "James O'Brien",
]

RANDOM_SEED = 42
NUM_ROWS = 500
OUTPUT_FILE = "sales_data.csv"


def random_date(days_back: int = 90) -> str:
    today = datetime.today()
    delta = timedelta(days=random.randint(0, days_back))
    return (today - delta).strftime("%Y-%m-%d")


def build_row(order_id: int) -> dict:
    product, base_price = random.choice(PRODUCTS)
    # Add ±20% price variance per deal
    unit_price = round(base_price * random.uniform(0.80, 1.20), 2)
    quantity = random.randint(1, 20)
    revenue = round(unit_price * quantity, 2)
    return {
        "order_id": f"ORD-{order_id:05d}",
        "date": random_date(),
        "product": product,
        "region": random.choice(REGIONS),
        "sales_rep": random.choice(SALES_REPS),
        "quantity": quantity,
        "unit_price": unit_price,
        "revenue": revenue,
    }


def main() -> None:
    random.seed(RANDOM_SEED)
    rows = [build_row(i + 1) for i in range(NUM_ROWS)]
    df = pd.DataFrame(rows)
    df = df.sort_values("date").reset_index(drop=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Generated {len(df)} rows → {OUTPUT_FILE}")
    print(df.describe(include="all").to_string())


if __name__ == "__main__":
    main()
