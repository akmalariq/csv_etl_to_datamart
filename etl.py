import csv
from datetime import datetime
import psycopg2

# connect to Postgres
conn = psycopg2.connect(
    database="db_database",
    user="db_user",
    password="db_password",
    host="db_host",
    port="db_port",
)
cursor = conn.cursor()

#################### STAGING ####################

########## Customers ##########

# Create staging table
print("Create table customers_staging")
cursor.execute("drop table if exists customers_staging")
cursor.execute(
    """
    create table if not exists customers_staging(
        customer_id VARCHAR(32) PRIMARY KEY,
        customer_unique_id VARCHAR(32),
        customer_zip_code_prefix VARCHAR(5),
        customer_city VARCHAR(64),
        customer_state VARCHAR(2)
        );
    """
)

# Load CSV into staging table
print("Loading data to table customers_staging")
with open("data/customers_dataset.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        cursor.execute("INSERT INTO customers_staging VALUES (%s, %s, %s, %s, %s)", row)


########## Geolocation ##########

# Create staging table
print("Create table geolocation_staging")
cursor.execute("drop table if exists geolocation_staging")
cursor.execute(
    """
    create table if not exists geolocation_staging(
        geolocation_zip_code_prefix VARCHAR(5),
        geolocation_latitude FLOAT,
        geolocation_longitude FLOAT,
        geolocation_city VARCHAR(64),
        geolocation_state VARCHAR(2)
        );
    """
)

# Load CSV into staging table
print("Loading data to table geolocation_staging")
with open("data/geolocation_dataset.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        cursor.execute(
            "INSERT INTO geolocation_staging VALUES (%s, %s, %s, %s, %s)", row
        )

########## Products ##########

# Create staging table
print("Create table products_staging")
cursor.execute("drop table if exists products_staging")
cursor.execute(
    """
    create table if not exists products_staging(
        product_id VARCHAR(32) PRIMARY KEY,
        product_category_name VARCHAR(64),
        product_name_lenght FLOAT,
        product_description_lenght FLOAT,
        product_photos_qty FLOAT,
        product_weight_g FLOAT,
        product_length_cm FLOAT,
        product_height_cm FLOAT,
        product_width_cm FLOAT
        );
    """
)

# Load CSV into staging table
print("Loading data to table products_staging")
with open("data/products_dataset.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        row = [val if val != "" else None for val in row]
        cursor.execute(
            "INSERT INTO products_staging VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            row,
        )

########## Sellers ##########

# Create staging table
print("Create table sellers_staging")
cursor.execute("drop table if exists sellers_staging")
cursor.execute(
    """
    create table if not exists sellers_staging(
        seller_id VARCHAR(32) PRIMARY KEY,
        seller_zip_code_prefix VARCHAR(5),
        seller_city VARCHAR(64),
        seller_state VARCHAR(2)
        );
    """
)

# Load CSV into staging table
print("Loading data to table sellers_staging")
with open("data/sellers_dataset.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        row = [val if val != "" else None for val in row]
        cursor.execute(
            "INSERT INTO sellers_staging VALUES (%s, %s, %s, %s)",
            row,
        )

########## Orders ##########

# Create staging table
print("Create table orders_staging")
cursor.execute("drop table if exists orders_staging")
cursor.execute(
    """
    create table if not exists orders_staging(
        order_id VARCHAR(32) PRIMARY KEY,
        customer_id VARCHAR(32),
        order_status VARCHAR(11),
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP
        );
    """
)

# Load CSV into staging table
print("Loading data to table orders_staging")
with open("data/orders_dataset.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        row = [val if val != "" else None for val in row]
        cursor.execute(
            "INSERT INTO orders_staging VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", row
        )

########## Order Items ##########

# Create staging table
print("Create table order_items_staging")
cursor.execute("drop table if exists order_items_staging")
cursor.execute(
    """
    create table if not exists order_items_staging(
        order_id VARCHAR(32),
        order_item_id INT,
        product_id VARCHAR(32),
        seller_id VARCHAR(32),
        shipping_limit_date TIMESTAMP,
        price FLOAT,
        freight_value FLOAT,
        PRIMARY KEY(order_id, order_item_id)
        );
    """
)

# Load CSV into staging table
print("Loading data to table order_items_staging")
with open("data/order_items_dataset.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        cursor.execute(
            "INSERT INTO order_items_staging VALUES (%s, %s, %s, %s, %s, %s, %s)", row
        )

########## Order Items ##########

# Create staging table
print("Create table order_payments_staging")
cursor.execute("drop table if exists order_payments_staging")
cursor.execute(
    """
    create table if not exists order_payments_staging(
        order_id VARCHAR(32),
        payment_sequential INT,
        payment_type VARCHAR(11),
        payment_installments INT,
        payment_value FLOAT,
        PRIMARY KEY(order_id, payment_sequential)
        );
    """
)

# Load CSV into staging table
print("Loading data to table order_payments_staging")
with open("data/order_payments_dataset.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        cursor.execute(
            "INSERT INTO order_payments_staging VALUES (%s, %s, %s, %s, %s)", row
        )

########## Order Reviews ##########

# Create staging table
print("Create table order_reviews_staging")
cursor.execute("drop table if exists order_reviews_staging")
cursor.execute(
    """
    create table if not exists order_reviews_staging(
        review_id VARCHAR(32),
        order_id VARCHAR(32),
        review_score INT,
        review_comment_title VARCHAR(64),
        review_comment_message VARCHAR(256),
        review_creation_date TIMESTAMP,
        review_answer_timestamp TIMESTAMP,
        PRIMARY KEY(review_id, order_id)
        );
    """
)

# Load CSV into staging table
print("Loading data to table order_reviews_staging")
with open("data/order_reviews_dataset.csv", "r", encoding="utf-8") as f:

    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        row = [val if val != "" else None for val in row]
        cursor.execute(
            "INSERT INTO order_reviews_staging VALUES (%s, %s, %s, %s, %s, %s, %s)", row
        )

########## Category Translation ##########

# Create staging table
print("Create table product_category_name_translation_staging")
cursor.execute("drop table if exists product_category_name_translation_staging")
cursor.execute(
    """
    create table if not exists product_category_name_translation_staging(
        product_category_name VARCHAR(64),
        product_category_name_english VARCHAR(64));
    """
)

# Load CSV into staging table
print("Loading data to table product_category_name_translation_staging")
with open("data/product_category_name_translation.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    for row in reader:
        row = [val if val != "" else None for val in row]
        cursor.execute(
            "INSERT INTO product_category_name_translation_staging VALUES (%s, %s)",
            row,
        )

#################### PRESENTATION ####################

# Perform cleaning, transformation, and Denormalize
print("Loading data to table sales_presentation")
cursor.execute("DROP TABLE IF EXISTS sales_presentation")
cursor.execute(
    """
    CREATE TABLE sales_presentation AS (
        SELECT
            o.order_id,
            o.order_purchase_timestamp,
            o.order_approved_at,
            UPPER(o.order_status) AS order_status,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            oi.shipping_limit_date,
            oi.price,
            oi.freight_value,
            op.payment_sequential,
            UPPER(op.payment_type) AS payment_type,
            op.payment_installments,
            op.payment_value,
            oi.order_item_id,
            oi.product_id,
            UPPER(p.product_category_name) AS product_category_name,
            UPPER(ptr.product_category_name_english) AS product_category_name_english,
            p.product_name_lenght AS product_name_length,
            p.product_description_lenght AS product_description_length,
            p.product_photos_qty AS product_photos_quantity,
            p.product_weight_g AS product_weight_in_gram,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm,
            oi.seller_id,
            s.seller_zip_code_prefix,
            UPPER(s.seller_city) AS seller_city,
            UPPER(s.seller_state) AS seller_state,
            o.customer_id,
            c.customer_unique_id,
            c.customer_zip_code_prefix,
            UPPER(c.customer_city) AS customer_city,
            UPPER(c.customer_state) AS customer_state,
            orv.review_id,
            orv.review_score,
            UPPER(orv.review_comment_title) AS review_comment_title,
            UPPER(orv.review_comment_message) AS review_comment_message,
            orv.review_creation_date,
            orv.review_answer_timestamp
        FROM 
            orders_staging o
        INNER JOIN
            customers_staging c ON (o.customer_id = c.customer_id)
        INNER JOIN
            order_payments_staging op ON (op.order_id = o.order_id)
        INNER JOIN
            order_items_staging oi ON (oi.order_id = o.order_id)
        INNER JOIN
            products_staging p ON (p.product_id = oi.product_id)
        INNER JOIN
            product_category_name_translation_staging ptr ON (ptr.product_category_name = p.product_category_name)
        INNER JOIN
            sellers_staging s ON (s.seller_id = oi.seller_id)
        INNER JOIN
            order_reviews_staging orv ON (orv.order_id = o.order_id));
    """
)

#################### DATA MART ####################

# Create Total Sales mart table
print("Loading data to table total_sales_mart")
cursor.execute("DROP TABLE IF EXISTS total_sales_mart")
cursor.execute(
    """
    CREATE TABLE total_sales_mart AS (
        SELECT
            ROUND(sum(payment_value)::numeric, 2) as total_sales_in_real,
            ROUND(avg(payment_value)::numeric, 2) as total_avg_sales_in_real
        FROM
            sales_presentation
        );
    """
)

# Create Product Categories mart table
print("Loading data to table product_categories_mart")
cursor.execute("DROP TABLE IF EXISTS product_categories_mart")
cursor.execute(
    """
    CREATE TABLE product_categories_mart AS (
        SELECT
            product_category_name_english as category,
            ROUND(sum(payment_value)::numeric, 2 ) as total_paid,
            ROUND(avg(payment_value)::numeric, 2 ) as average_paid
        FROM
            sales_presentation
        GROUP BY
            category
        );
    """
)

# Create Products mart table
print("Loading data to table products_mart")
cursor.execute("DROP TABLE IF EXISTS products_mart")
cursor.execute(
    """
    CREATE TABLE products_mart AS (
        SELECT
            product_id,
            ROUND(sum(payment_value)::numeric, 2 ) as total_paid,
            ROUND(avg(payment_value)::numeric, 2 ) as average_paid
        FROM
            sales_presentation
        GROUP BY
            product_id
        );
    """
)

# Create Sellers mart table
print("Loading data to table sellers_mart")
cursor.execute("DROP TABLE IF EXISTS sellers_mart")
cursor.execute(
    """
    CREATE TABLE sellers_mart AS (
        SELECT
            seller_id,
            ROUND(sum(payment_value)::numeric, 2 ) as total_paid,
            ROUND(avg(payment_value)::numeric, 2 ) as average_paid
        FROM
            sales_presentation
        GROUP BY
            seller_id
        );
    """
)

# commit changes and close connection
conn.commit()
conn.close()
