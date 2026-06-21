from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import json
import os
import zipfile

@dag(
    dag_id="olist_daily_sales_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "portfolio", "etl"],
)

def olist_daily_sales_pipeline():

    @task
    def download_dataset():
        # pull the token from Airflow's secure Variable store
        token = Variable.get("kaggle_token")
        os.environ["KAGGLE_API_TOKEN"] = token

        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()

        download_path = "/usr/local/airflow/include"
        api.dataset_download_files(
            "olistbr/brazilian-ecommerce",
            path=download_path,
            unzip=True
        )
        print("Dataset downloaded and unzipped to", download_path)

    downloaded = download_dataset()

    @task
    def extract_orders(_dep):
        path = "/usr/local/airflow/include/olist_orders_dataset.csv"
        df = pd.read_csv(path)
        records = json.loads(df.to_json(orient="records"))
        print(f"Loaded {len(df):,} orders")
        return records

    @task
    def extract_order_items(_dep):
        path = "/usr/local/airflow/include/olist_order_items_dataset.csv"
        df = pd.read_csv(path)
        records = json.loads(df.to_json(orient="records"))
        print(f"Loaded {len(df):,} order items")
        return records

    @task
    def extract_products(_dep):
        path = "/usr/local/airflow/include/olist_products_dataset.csv"
        df = pd.read_csv(path)
        records = json.loads(df.to_json(orient="records"))
        print(f"Loaded {len(df):,} products")
        return records

    @task
    def extract_customers(_dep):
        path = "/usr/local/airflow/include/olist_customers_dataset.csv"
        df = pd.read_csv(path)
        records = json.loads(df.to_json(orient="records"))
        print(f"Loaded {len(df):,} customers")
        return records

    @task
    def transform_join(orders_data, order_items_data, products_data, customers_data):
        orders_df = pd.DataFrame(orders_data)
        order_items_df = pd.DataFrame(order_items_data)
        products_df = pd.DataFrame(products_data)
        customers_df = pd.DataFrame(customers_data)

        # join order_items -> orders (each item belongs to one order)
        fact = order_items_df.merge(orders_df, on="order_id", how="left")

        # join in product category
        fact = fact.merge(
            products_df[["product_id", "product_category_name"]],
            on="product_id", how="left"
        )

        # join in customer state
        fact = fact.merge(
            customers_df[["customer_id", "customer_state"]],
            on="customer_id", how="left"
        )

        print(f"Joined fact table: {len(fact):,} rows")
        return json.loads(fact.to_json(orient="records"))
    
    @task
    def transform_aggregate(fact_data):
        df = pd.DataFrame(fact_data)

        df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
        df["order_date"] = df["order_purchase_timestamp"].dt.date

        summary = (
            df.groupby(["order_date", "product_category_name", "customer_state"])
            .agg(
                total_revenue=("price", "sum"),
                total_freight=("freight_value", "sum"),
                order_count=("order_id", "nunique"),
                item_count=("order_id", "count"),
            )
            .reset_index()
        )
        summary["avg_order_value"] = (summary["total_revenue"] / summary["order_count"]).round(2)

        # convert date to a plain string BEFORE the JSON round-trip,
        # so it doesn't get turned into a millisecond timestamp
        summary["order_date"] = summary["order_date"].astype(str)

        print(f"Aggregated summary: {len(summary):,} rows "
              f"({summary['order_date'].nunique()} distinct days)")
        return json.loads(summary.to_json(orient="records"))
    
    @task

    # to load summary as csv run this function

    # def load(summary_data):
    #     df = pd.DataFrame(summary_data)
    #     out_path = "/usr/local/airflow/include/daily_sales_summary.csv"
    #     df.to_csv(out_path, index=False)
    #     print(f"Loaded {len(df):,} rows to {out_path}")
    #     print(df.head(10).to_string())


    # To load summary as postgres connection use this 

    def load(summary_data):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = pd.DataFrame(summary_data)

        hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = hook.get_sqlalchemy_engine()

        df.to_sql(
            "daily_sales_summary",
            engine,
            if_exists="replace",
            index=False
        )
        print(f"Loaded {len(df):,} rows into Postgres table 'daily_sales_summary'")

    orders = extract_orders(downloaded)
    order_items = extract_order_items(downloaded)
    products = extract_products(downloaded)
    customers = extract_customers(downloaded)
    fact_table = transform_join(orders, order_items, products, customers)
    daily_summary = transform_aggregate(fact_table)
    load(daily_summary)

olist_daily_sales_pipeline()