import logging

from pydantic import BaseSettings, Field
from dotenv import load_dotenv


load_dotenv()


class PostgresDSN(BaseSettings):
    dbname: str = Field(env='POSTGRES_DB')
    user: str = Field(env='POSTGRES_USER')
    password: str = Field(env="POSTGRES_PASSWORD")
    host: str = Field(env='POSTGRES_HOST')
    port: int = Field(env='POSTGRES_PORT')
    options: str = Field(env='POSTGRES_OPTIONS')


class ElasticConfig(BaseSettings):
    host: str = Field(env='ELASTICSEARCH_HOST')
    port: int = Field(env='ELASTICSEARCH_PORT')
    scheme: str = 'http'


class RedisConfig(BaseSettings):
    host: str = Field(env='REDIS_HOST')
    port: int = Field(env='REDIS_PORT')


class AppConfig(BaseSettings):
    batch_size: int = Field(env='BATCH_SIZE')
    frequency: int = Field(env='FREQUENCY')
    elastic_index: str = Field(env='ELASTICSEARCH_INDEX')


POSTGRES_DSN = PostgresDSN()
ELASTIC_CONFIG = ElasticConfig()
REDIS_CONFIG = RedisConfig()
APP_CONFIG = AppConfig()

LOGGER_SETTINGS = {
    'format': '%(asctime)s - %(name)s.%(funcName)s:%(lineno)d - %(levelname)s - %(message)s',
    'datefmt': "%Y-%m-%d %H:%M:%S",
    'level': logging.INFO,
    'handlers': [logging.StreamHandler()],
}
