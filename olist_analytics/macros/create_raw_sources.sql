{% macro create_raw_sources() %}
  {% set data_path = var('olist_data_path') %}

  create schema if not exists olist_raw;

  create or replace view olist_raw.orders as
    select * from read_csv_auto('{{ data_path }}/olist_orders_dataset.csv');

  create or replace view olist_raw.order_items as
    select * from read_csv_auto('{{ data_path }}/olist_order_items_dataset.csv');

  create or replace view olist_raw.customers as
    select * from read_csv_auto('{{ data_path }}/olist_customers_dataset.csv');

  create or replace view olist_raw.products as
    select * from read_csv_auto('{{ data_path }}/olist_products_dataset.csv');

  create or replace view olist_raw.sellers as
    select * from read_csv_auto('{{ data_path }}/olist_sellers_dataset.csv');

  create or replace view olist_raw.order_payments as
    select * from read_csv_auto('{{ data_path }}/olist_order_payments_dataset.csv');

  create or replace view olist_raw.order_reviews as
    select * from read_csv_auto('{{ data_path }}/olist_order_reviews_dataset.csv');
{% endmacro %}