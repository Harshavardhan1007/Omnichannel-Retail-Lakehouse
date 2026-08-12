-- Drop first so re-running this script is idempotent (safe to run again)
DROP TABLE IF EXISTS order_reviews, order_payments, order_items, orders,
    products, sellers, customers, geolocation,
    product_category_name_translation CASCADE;

-- Shared trigger function: stamp updated_at on every UPDATE
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------- Parent tables (no foreign keys) ----------
CREATE TABLE customers (
    customer_id              VARCHAR PRIMARY KEY,
    customer_unique_id       VARCHAR NOT NULL,
    customer_zip_code_prefix INTEGER,
    customer_city            VARCHAR,
    customer_state           VARCHAR,
    updated_at               TIMESTAMP DEFAULT now()
);

CREATE TABLE sellers (
    seller_id              VARCHAR PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city            VARCHAR,
    seller_state           VARCHAR,
    updated_at             TIMESTAMP DEFAULT now()
);

CREATE TABLE product_category_name_translation (
    product_category_name         VARCHAR PRIMARY KEY,
    product_category_name_english VARCHAR,
    updated_at                    TIMESTAMP DEFAULT now()
);

CREATE TABLE products (
    product_id                 VARCHAR PRIMARY KEY,
    product_category_name      VARCHAR,
    product_name_lenght        INTEGER,   -- (typo is in the original dataset)
    product_description_lenght INTEGER,
    product_photos_qty         INTEGER,
    product_weight_g           INTEGER,
    product_length_cm          INTEGER,
    product_height_cm          INTEGER,
    product_width_cm           INTEGER,
    updated_at                 TIMESTAMP DEFAULT now()
);

-- geolocation has no unique natural key -> surrogate serial PK
CREATE TABLE geolocation (
    geolocation_id              BIGSERIAL PRIMARY KEY,
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat             DOUBLE PRECISION,
    geolocation_lng             DOUBLE PRECISION,
    geolocation_city            VARCHAR,
    geolocation_state           VARCHAR,
    updated_at                  TIMESTAMP DEFAULT now()
);

-- ---------- Child tables (with foreign keys) ----------
CREATE TABLE orders (
    order_id                      VARCHAR PRIMARY KEY,
    customer_id                   VARCHAR NOT NULL REFERENCES customers(customer_id),
    order_status                  VARCHAR,
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    updated_at                    TIMESTAMP DEFAULT now()
);

CREATE TABLE order_items (
    order_id            VARCHAR NOT NULL REFERENCES orders(order_id),
    order_item_id       INTEGER NOT NULL,
    product_id          VARCHAR NOT NULL REFERENCES products(product_id),
    seller_id           VARCHAR NOT NULL REFERENCES sellers(seller_id),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2),
    updated_at          TIMESTAMP DEFAULT now(),
    PRIMARY KEY (order_id, order_item_id)          -- composite key
);

CREATE TABLE order_payments (
    order_id             VARCHAR NOT NULL REFERENCES orders(order_id),
    payment_sequential   INTEGER NOT NULL,
    payment_type         VARCHAR,
    payment_installments INTEGER,
    payment_value        NUMERIC(10,2),
    updated_at           TIMESTAMP DEFAULT now(),
    PRIMARY KEY (order_id, payment_sequential)     -- composite key
);

-- review_id is NOT unique in the source -> surrogate PK, keep review_id as a column
CREATE TABLE order_reviews (
    review_sk               BIGSERIAL PRIMARY KEY,
    review_id               VARCHAR,
    order_id                VARCHAR NOT NULL REFERENCES orders(order_id),
    review_score            INTEGER,
    review_comment_title    VARCHAR,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT now()
);

-- ---------- Indexes (speed up joins + incremental reads) ----------
CREATE INDEX idx_orders_customer_id   ON orders(customer_id);
CREATE INDEX idx_orders_updated_at    ON orders(updated_at);
CREATE INDEX idx_order_items_product  ON order_items(product_id);
CREATE INDEX idx_order_items_seller   ON order_items(seller_id);
CREATE INDEX idx_order_payments_order ON order_payments(order_id);
CREATE INDEX idx_order_reviews_order  ON order_reviews(order_id);

-- ---------- updated_at triggers on every table ----------
CREATE TRIGGER trg_customers   BEFORE UPDATE ON customers   FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_sellers     BEFORE UPDATE ON sellers     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_products    BEFORE UPDATE ON products    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_geolocation BEFORE UPDATE ON geolocation FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_orders      BEFORE UPDATE ON orders      FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_order_items BEFORE UPDATE ON order_items FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_payments    BEFORE UPDATE ON order_payments FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_reviews     BEFORE UPDATE ON order_reviews  FOR EACH ROW EXECUTE FUNCTION set_updated_at();