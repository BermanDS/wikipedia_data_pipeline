CREATE TABLE {table_target} (
  title VARCHAR,
  username VARCHAR,
  bot BOOLEAN,
  type_ VARCHAR,
  domain_ VARCHAR,
  parsedcomment STRING,
  date_create TIMESTAMP without time zone
) WITH (
 'connector' = 'jdbc',
 'url' = 'jdbc:postgresql://{PG__HOST}:{PG__PORT}/{PG__DBNAME}',
 'table-name' = '{PG__SCHEMA}.{PG__TABLE}',
 'username' = '{PG__USER_BOT}',
 'password' = '{PG__PASSW_BOT}'
)