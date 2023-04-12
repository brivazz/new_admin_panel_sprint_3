import logging
import time

from settings import (
    APP_CONFIG,
    ELASTIC_CONFIG,
    LOGGER_SETTINGS,
    POSTGRES_DSN,
    REDIS_CONFIG
)
from extract import PostgresExtractor
from load import ElasticLoader
from state import State
from query import get_query


itersize = APP_CONFIG.batch_size
freq = APP_CONFIG.frequency
index = APP_CONFIG.elastic_index

state = State(config=REDIS_CONFIG)
postgres_extractor = PostgresExtractor(dsn=POSTGRES_DSN)
elastic_loader = ElasticLoader(config=ELASTIC_CONFIG, state=state, index=index)
elastic_loader.create_index_if_not_exists()


def etl(query: str) -> None:
    """Загружает в elasticsearch данные пачками с помощью генераторов."""
    data_generator = postgres_extractor.extract_data(query, itersize)
    elastic_loader.bulk_update(data_generator)


if __name__ == '__main__':
    logging.basicConfig(**LOGGER_SETTINGS)
    logger = logging.getLogger(__name__)
    while True:
        logger.info('Starting etl...')
        modified = state.get_state('modified')

        try:
            query = get_query(modified)
            etl(query)

        except ValueError as er:
            logger.error('Error: %s', er)
            continue

        logger.info('Sleep %s seconds...', freq)
        time.sleep(freq)
