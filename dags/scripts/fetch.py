import requests
import pandas as pd
import os
from datetime import datetime

def fetch_orders_data():
    print("Membuka data lake: Orders API...")

    url = "http://96.9.212.102:8000/orders"

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
        orders = data["orders"]

        print(f"✅ Berhasil mengambil {len(orders)} orders")

        parsed_data = []

        for order in orders:
            # Informasi level order
            order_info = {
                "order_id": order.get("order_id"),
                "user_id": order.get("user_id"),
                "order_number": order.get("order_number"),
                "order_dow": order.get("order_dow"),
                "order_hour_of_day": order.get("order_hour_of_day"),
                "days_since_prior_order": order.get("days_since_prior_order"),
                "eval_set": order.get("eval_set")
            }

            # Ambil semua product dalam order
            products = order.get("products", [])

            # Flatten:
            # 1 product = 1 row
            for product in products:

                row = {
                    # Order fields
                    **order_info,

                    # Product fields
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

        df = pd.DataFrame(parsed_data)

        print("\nPreview data:")
        print(df.head())

        print("\nKolom dataframe:")
        print(df.columns.tolist())

        print(f"\nTotal rows setelah flatten: {len(df)}")

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_path = (
            f"/opt/airflow/data_lake/orders/"
            f"orders_{current_time}.parquet"
        )
        os.makedirs(
            os.path.dirname(output_path),
            exist_ok=True
        )
        df.to_parquet(
            output_path,
            index=False
        )
        print(f"\n✅ Sukses menyimpan parquet:")
        print(output_path)

    except Exception as e:
        print(f"\n❌ Gagal fetch data: {e}")
        raise

if __name__ == "__main__":
    fetch_orders_data()