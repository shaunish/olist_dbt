import duckdb

con = duckdb.connect("./olist.duckdb")

con.execute("create schema if not exists olist_raw")

data_path = "./data"

views = {
    "orders":         "olist_orders_dataset.csv",
    "order_items":    "olist_order_items_dataset.csv",
    "customers":      "olist_customers_dataset.csv",
    "products":       "olist_products_dataset.csv",
    "sellers":        "olist_sellers_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews":  "olist_order_reviews_dataset.csv",
}

for view_name, filename in views.items():
    con.execute(f"""
        create or replace view olist_raw.{view_name} as
        select * from read_csv_auto('{data_path}/{filename}')
    """)
    print(f"✓ created view olist_raw.{view_name}")

# verify
result = con.execute("show schemas").fetchall()
print("\nSchemas in DB:", result)

result = con.execute("show tables in olist_raw").fetchall()  
print("Views in olist_raw:", result)

con.close()