CREATE DATABASE test_db;

CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS excluded_schema;

CREATE TABLE IF NOT EXISTS public.users (
    id BIGSERIAL PRIMARY KEY,
    name varchar(255) NOT NULL,
    email varchar(255),
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES public.users(id),
    amount DECIMAL(10,2),
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS excluded_schema.internal_logs (
    id BIGSERIAL PRIMARY KEY,
    message TEXT,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.excluded_table (
    id BIGSERIAL PRIMARY KEY,
    data TEXT,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);
