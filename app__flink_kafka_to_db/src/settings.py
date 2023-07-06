from pydantic import BaseSettings


class Settings(BaseSettings):
    
    # ------------------------------------------------
    KAFKA__PORT: int = 9092
    KAFKA__PORT_EXT: int = 9093
    KAFKA__HOST: str = '10.77.120.21'
    
    KAFKA__USER: str = 'admin'
    KAFKA__PASSW: str = 'admin-secret'
    KAFKA__TOPIC: str = 'Topic1,Topic2'
    
    timezone: str = 'Europe/Moscow'

    PG__PORT: int =5432
    PG__HOST: str = '10.77.120.21'
    PG__USER_BOT: str = 'admin'
    PG__PASSW_BOT: str = 'secret'
    PG__DBNAME: str = 'db'
    PG__SCHEMA: str = 'wiki'
    PG__TABLE: str = 'table'
    
    timezone: str = 'Europe/Moscow'

    LOG__PATH: str = 'logs'
    JARS__DATA: str = 'jarlibs'
    SCRIPTS__DATA: str = 'scripts'

    table_source: str = 'source_ddl'
    table_target: str = 'target_ddl'
    
    class Config:
        env_file = "./.env"
        env_file_encoding = 'utf-8'


configs = Settings().dict()