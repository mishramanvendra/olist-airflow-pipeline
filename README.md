# Olist Daily Sales Pipeline — Apache Airflow

A daily batch ETL pipeline built with Apache Airflow (TaskFlow API) that ingests real
e-commerce transaction data, joins it into an analytics-ready fact table, aggregates it
into daily sales metrics, and loads the result into Postgres.

Built as a hands-on project to learn Airflow's DAG/task model, XCom data passing,
parallel task execution, external API integration, and database loading — using a real,
publicly available 100k-order dataset rather than synthetic toy data.

## What it does
Kaggle API ──► download_dataset

│

┌───────────────────┼─────────────────────┬─────────────────────┐

▼                   ▼                    ▼                     ▼

extract_orders  extract_items  extract_products  extract_customers

└───────────────────┴─────────────────────┴─────────────────────┘

│

▼

transform_join  (joins 4 tables into one fact table)

│

▼

transform_aggregate  (daily revenue/orders by category + state)

│

▼

load  (writes to Postgres)

1. **`download_dataset`** — pulls the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
   fresh from Kaggle via their API on every run (credentials stored as an Airflow Variable,
   never hardcoded)
2. **Four parallel extract tasks** — load orders, order items, products, and customers;
   Airflow runs these concurrently since none depends on another's output
3. **`transform_join`** — merges the four tables into a single fact table (one row per
   item sold, with category and customer state attached)
4. **`transform_aggregate`** — groups by day, product category, and customer state to
   compute total revenue, freight cost, order count, and average order value
5. **`load`** — writes the result into a Postgres table (`daily_sales_summary`) via
   Airflow's `PostgresHook`

## Real-world data

This uses the **Brazilian E-Commerce Public Dataset by Olist** — ~100,000 real, anonymized
orders placed on the Olist marketplace between 2016 and 2018. It's a genuinely messy,
real dataset (missing values, varied formats), not a clean toy CSV — which is part of
the point: the pipeline had to handle real-world data quality issues (e.g. `NaN` values
breaking JSON serialization between Airflow tasks) rather than assume clean input.

## Tech stack

- **Apache Airflow** (TaskFlow API / `@dag` and `@task` decorators)
- **Astro CLI + Docker** for local orchestration (mirrors a managed Airflow deployment)
- **Postgres** as the analytics-ready output store
- **Kaggle API** for live data ingestion
- **pandas** for the join/transform logic

## Setup

```bash
# 1. Clone and enter the repo
git clone https://github.com/YOUR-USERNAME/olist-airflow-pipeline.git
cd olist-airflow-pipeline

# 2. Start Airflow locally via the Astro CLI
astro dev start

# 3. Add Kaggle credentials as an Airflow Variable
#    (Admin → Variables in the Airflow UI)
#    Key: kaggle_token
#    Val: <your Kaggle API token>

# 4. Confirm the Postgres connection exists
#    (Admin → Connections → postgres_default — pre-configured by Astro)

# 5. Trigger the DAG
#    Open http://localhost:8080 (or whichever port Astro assigns),
#    enable `olist_daily_sales_pipeline`, and trigger a run
```

## Sample output
order_date  | product_category_name | customer_state | total_revenue | order_count | avg_order_value

2016-10-03  | moveis_decoracao      | MG              | 74.90          | 1            | 74.90

2016-10-03  | esporte_lazer         | RS              | 58.39          | 2            | 29.20

2016-09-15  | beleza_saude          | SP              | 134.97         | 1            | 134.97

## What I learned building this

- **TaskFlow API & XCom** — passing data between tasks, and the JSON-serialization
  constraint XCom imposes (had to convert `NaN` → `None` / use `df.to_json()` rather
  than `df.to_dict()` to avoid serialization failures on real-world missing data)
- **Automatic parallelism** — Airflow infers which tasks can run concurrently purely
  from the dependency graph (which task's output feeds which task's input) — no manual
  parallelization code needed
- **Secrets management** — storing API credentials as Airflow Variables rather than
  hardcoding them, and using `PostgresHook` to avoid ever putting database credentials
  in code
- **Docker-based local dev** — debugging `requirements.txt` changes that require a full
  image rebuild (`astro dev stop && astro dev start`) vs. a simple restart
