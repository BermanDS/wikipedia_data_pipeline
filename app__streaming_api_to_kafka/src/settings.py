from pydantic import BaseSettings


class Settings(BaseSettings):
    
    URL_STREAM: str = ''
    # ------------------------------------------------
    KAFKA__PORT: int = 9092
    KAFKA__PORT_EXT: int = 9093
    KAFKA__HOST: str = '10.77.120.21'
    
    KAFKA__USER: str = 'admin'
    KAFKA__PASSW: str = 'admin-secret'
    KAFKA__TOPIC: str = 'Topic1'
    
    LOG__PATH_LOCAL: str = '/app/logs'
    timezone: str = 'Europe/Moscow'

    class Config:
        env_file = "./.env"
        env_file_encoding = 'utf-8'


configs = Settings().dict()