#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-END

    ----------------------------------------Create DB SCHEMA--------------------------------
    CREATE SCHEMA IF NOT EXISTS rawlayer;
    CREATE SCHEMA IF NOT EXISTS cleanlayer;
    CREATE SCHEMA IF NOT EXISTS agglayer;

    ----------------------------------------Raw Layern cleanup--------------------------------------
		DROP TABLE IF EXISTS rawlayer.yelp_academic_dataset_business;
		DROP TABLE IF EXISTS rawlayer.yelp_academic_dataset_checkin;
		DROP TABLE IF EXISTS rawlayer.yelp_academic_dataset_review;
		DROP TABLE IF EXISTS rawlayer.yelp_academic_dataset_tip;
		DROP TABLE IF EXISTS rawlayer.yelp_academic_dataset_user;

    ----------------------------------------Clean Layer cleanup--------------------------------------
    DROP TABLE IF EXISTS cleanlayer.yelp_academic_dataset_business;
		DROP TABLE IF EXISTS cleanlayer.yelp_academic_dataset_checkin;
		DROP TABLE IF EXISTS cleanlayer.yelp_academic_dataset_review;
		DROP TABLE IF EXISTS cleanlayer.yelp_academic_dataset_tip;
		DROP TABLE IF EXISTS cleanlayer.yelp_academic_dataset_user;

		-----------------------------------------Create Tables RAW Layer--------------------------------------
		-- 1. Business Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_business(
        address text,
        attributes JSONB,
        business_id text,
        categories text,
        city text,
        hours JSONB,
        is_open bigint,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        name text,
        postal_code text,
        review_count bigint,
        stars DOUBLE PRECISION,
        state text,
        raw_json_text_data text,
        etl_load_date date
    );

    -- 2. Checkins Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_checkin(
            business_id text,
            date text,
            raw_json_text_data text,
            etl_load_date date
        );

    -- 3. Review Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_review(
            business_id text,
            cool bigint,
            date text,
            funny bigint,
            review_id text,
            stars DOUBLE PRECISION,
            text text,
            useful bigint,
            user_id text,
            raw_json_text_data text,
            etl_load_date date
        );

    -- 4. tip Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_tip(
            text text,
            date text,
            compliment_count bigint,
            business_id text,
            user_id text,
            raw_json_text_data text,
            etl_load_date date
        );

    -- 5. User Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_user(
            user_id text,
            name CHARACTER VARYING(20),
            review_count bigint,
            yelping_since text,
            friends text,
            useful bigint,
            funny bigint,
            cool bigint,
            fans bigint,
            elite text,
            average_stars DOUBLE PRECISION,
            compliment_hot bigint,
            compliment_more bigint,
            compliment_profile bigint,
            compliment_cute bigint,
            compliment_list bigint,
            compliment_note bigint,
            compliment_plain bigint,
            compliment_cool bigint,
            compliment_funny bigint,
            compliment_writer bigint,
            compliment_photos bigint,
            raw_json_text_data text,
            etl_load_date date
        );

-----------------------------------------Create Tables Clean Layer--------------------------------------
		-- 1. Business Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_business(
        address text,
        attributes JSONB,
        business_id text,
        categories text,
        city text,
        hours JSONB,
        is_open bigint,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        name text,
        postal_code text,
        review_count bigint,
        stars DOUBLE PRECISION,
        state text,
        week int,
        month int,
        year int,
        etl_load_date date
    );

    -- 2. Checkins Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_checkin(
            business_id text,
            date text,
            etl_load_date date
        );

    -- 3. Review Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_review(
            business_id text,
            cool bigint,
            date text,
            funny bigint,
            review_id text,
            stars DOUBLE PRECISION,
            text text,
            useful bigint,
            user_id text,
            etl_load_date date
        );

    -- 4. tip Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_tip(
            text text,
            date text,
            compliment_count bigint,
            business_id text,
            user_id text,
            etl_load_date date
        );

    -- 5. User Dataset
    CREATE TABLE rawlayer.yelp_academic_dataset_user(
            user_id text,
            name CHARACTER VARYING(20),
            review_count bigint,
            yelping_since text,
            friends text,
            useful bigint,
            funny bigint,
            cool bigint,
            fans bigint,
            elite text,
            average_stars DOUBLE PRECISION,
            compliment_hot bigint,
            compliment_more bigint,
            compliment_profile bigint,
            compliment_cute bigint,
            compliment_list bigint,
            compliment_note bigint,
            compliment_plain bigint,
            compliment_cool bigint,
            compliment_funny bigint,
            compliment_writer bigint,
            compliment_photos bigint,
            etl_load_date date
        );

-----------------------------------------Create Tables Agg Layer--------------------------------------
    -- 1. business_stars_weekly
    DROP TABLE IF EXISTS agglayer.business_stars_weekly;
    CREATE TABLE IF NOT EXISTS agglayer.business_stars_weekly
    (
        business_id text COLLATE pg_catalog."default",
        week integer,
        avg_stars double precision
    );

    -- 2. no_of_checkin_overall_rating
    DROP TABLE IF EXISTS agglayer.no_of_checkin_overall_rating;
    CREATE TABLE IF NOT EXISTS agglayer.no_of_checkin_overall_rating
    (
        business_id text COLLATE pg_catalog."default",
        no_of_checkin integer NOT NULL,
        overall_rating double precision
    );

    -- 3. popular_categories
    DROP TABLE IF EXISTS agglayer.popular_categories;
    CREATE TABLE IF NOT EXISTS agglayer.popular_categories
    (
        categories text COLLATE pg_catalog."default" NOT NULL,
        category_count bigint NOT NULL
    );


END