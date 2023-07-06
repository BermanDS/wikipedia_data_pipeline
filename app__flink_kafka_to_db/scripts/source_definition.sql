CREATE TABLE {table_source} (
            title_ VARCHAR,
            timestamp_ BIGINT,
            user_ VARCHAR,
            bot_ BOOLEAN,
            parsedcomment_ VARCHAR,
            type_ VARCHAR,
            domain_ VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA__TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA__HOST}:{KAFKA__PORT}',
            'format' = 'json'
        )