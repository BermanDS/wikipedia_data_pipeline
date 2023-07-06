#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
    
    /* -------------------------------- Create rolw ------------------------------------------------*/
    -- Create Role: bot_parser 
    CREATE ROLE bot_parser WITH
    LOGIN
    NOSUPERUSER
    INHERIT
    CREATEDB
    NOCREATEROLE
    NOREPLICATION
    ENCRYPTED PASSWORD 'md5bb28a329be706bc0b8994c783541873d';
    COMMENT ON ROLE bot_parser IS 'for parsing data by Streaming';
    /* -------------------- Create database ------------------------------------------------------------*/
    CREATE DATABASE wikipedia
    WITH 
    OWNER = admin
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;   
    
    /* -------------------------- Create schema ------------------------------------------------------*/
    -- SCHEMA: wikipedia_raw
    \connect wikipedia;
    CREATE SCHEMA wikipedia_raw
    AUTHORIZATION admin;
    GRANT ALL ON SCHEMA wikipedia_raw TO admin;
    GRANT ALL ON SCHEMA wikipedia_raw TO bot_parser;

    /*------------------------- Create tables ---------------------------------------------------------------*/
    -- Table: wikipedia_raw.recentchange
    CREATE TABLE IF NOT EXISTS wikipedia_raw.recentchange
    (
        title character varying COLLATE pg_catalog."default",
        username character varying COLLATE pg_catalog."default" NOT NULL,
        bot boolean,
        domain_ character varying COLLATE pg_catalog."default",
        type_ character varying COLLATE pg_catalog."default",
        parsedcomment character varying COLLATE pg_catalog."default",
        date_create timestamp without time zone
    ) PARTITION BY RANGE (date_create);

    ALTER TABLE wikipedia_raw.recentchange
        OWNER to admin;
    REVOKE ALL ON TABLE wikipedia_raw.recentchange FROM bot_parser;
    GRANT ALL ON TABLE wikipedia_raw.recentchange TO admin;
    GRANT DELETE, INSERT, SELECT, UPDATE ON TABLE wikipedia_raw.recentchange TO bot_parser;
    -- Index: date_create_inx_1
    -- DROP INDEX wikipedia_raw.date_create_1;
    CREATE INDEX date_create_inx_1
        ON wikipedia_raw.recentchange USING btree
    (date_create ASC NULLS LAST);
    -- Partitions SQL
    CREATE TABLE IF NOT EXISTS wikipedia_raw.recentchange_1900 PARTITION OF wikipedia_raw.recentchange DEFAULT;
    ALTER TABLE wikipedia_raw.recentchange_1900 OWNER to admin;
    CREATE TABLE IF NOT EXISTS wikipedia_raw.recentchange_202307 PARTITION OF wikipedia_raw.recentchange
        FOR VALUES FROM ('2023-07-01 00:00:00') TO ('2023-08-01 00:00:00');
    ALTER TABLE wikipedia_raw.recentchange_202307 OWNER to admin;
    CREATE TABLE IF NOT EXISTS wikipedia_raw.recentchange_202308 PARTITION OF wikipedia_raw.recentchange
        FOR VALUES FROM ('2023-08-01 00:00:00') TO ('2023-09-01 00:00:00');
    ALTER TABLE wikipedia_raw.recentchange_202308 OWNER to admin;
    
EOSQL