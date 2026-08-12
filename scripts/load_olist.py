import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATA_DIR = "data/olist"

# (csv file, table, columns in the SAME order as the CSV's columns)
LOADS = [
    ("olist_customers_dataset.csv", "customers",
     "customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state"),
    ("olist_sellers_dataset.csv", "sellers",
     "seller_id, seller_zip_code_prefix, seller_city, seller_state"),
    ("product_category_name_translation.csv", "product_category_name_translation",
     "product_category_name, product_category_name_english"),
    ("olist_products_dataset.csv", "products",
     "product_id, product_category_name, product_name_lenght, product_description_lenght, "
     "product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm"),
    ("olist_geolocation_dataset.csv", "geolocation",
     "geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state"),
    ("olist_orders_dataset.csv", "orders",
     "order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, "
     "order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date"),
    ("olist_order_items_dataset.csv", "order_items",
     "order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value"),
    ("olist_order_payments_dataset.csv", "order_payments",
     "order_id, payment_sequential, payment_type, payment_installments, payment_value"),
    ("olist_order_reviews_dataset.csv", "order_reviews",
     "review_id, order_id, review_score, review_comment_title, review_comment_message, "
     "review_creation_date, review_answer_timestamp"),
]


def main():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    cur = conn.cursor()

    print("Creating schema...")
    with open("sql/schema.sql") as f:
        cur.execute(f.read())

    for filename, table, columns in LOADS:
        path = os.path.join(DATA_DIR, filename)
        print(f"Loading {filename} -> {table} ...")
        with open(path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)", f
            )

    conn.commit()

    print("\nRow counts:")
    for _, table, _ in LOADS:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:40s} {cur.fetchone()[0]:>10,}")

    cur.close()
    conn.close()
    print("\n✅ Olist loaded.")


if __name__ == "__main__":
    main()