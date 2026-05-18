import requests
import pandas as pd
import os
from datetime import datetime

url = "http://96.9.212.102:8000/orders"

def fetch_orders_data():
    print(f"Fetching orders data from {url}...")
    headers = {
        "User-Agent": "AirflowOrdersPipeline/1.0"
    }

    try:
        response = requests.get(
            url=url,
            headers=headers,
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"\n❌ Failed to fetch data: {e}")
        raise

    orders = data.get("orders", [])
    if not orders:
        print("⚠️ No orders found in API response. Exiting.")
        return

    print(f"✅ Successfully fetched {len(orders)} orders")

    parsed_data = []
    for order in orders:
        order_info = {
            "order_id": order.get("order_id"),
            "user_id": order.get("user_id"),
            "order_number": order.get("order_number"),
            "order_dow": order.get("order_dow"),
            "order_hour_of_day": order.get("order_hour_of_day"),
            "days_since_prior_order": order.get("days_since_prior_order"),
            "eval_set": order.get("eval_set")
        }
        # flatten products in orders into separate rows
        for product in order.get("products", []):
            row = {
                **order_info, # copy order info into row
                "product_id": product.get("product_id"),
                "product_name": product.get("product_name"),
                "aisle_id": product.get("aisle_id"),
                "aisle": product.get("aisle"),
                "department_id": product.get("department_id"),
                "department": product.get("department"),
                "add_to_cart_order": product.get("add_to_cart_order"),
                "reordered": product.get("reordered")
            }
            parsed_data.append(row)

    if not parsed_data:
        print("⚠️ No product data found in orders. Exiting.")
        return

    df = pd.DataFrame(parsed_data)
    print("\nPreview data:")
    print(df.head())
    print("\nDataFrame columns:")
    print(df.columns.tolist())
    print(f"\nTotal rows after flatten: {len(df)}")

    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = (
        f"/opt/airflow/data_lake/orders/"
        f"orders_{current_time}.parquet"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    temp_path = output_path + ".tmp"

    # save to parquet with temp file to ensure atomic write
    try:
        df.to_parquet(temp_path, index=False)
        os.replace(temp_path, output_path)
        print(f"\n✅ Successfully saved parquet:")
        print(output_path)
    except Exception as e:
        print(f"\n❌ Failed to save parquet: {e}")
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise

if __name__ == "__main__":
    fetch_orders_data()
