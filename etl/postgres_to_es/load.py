import json
import logging
from dataclasses import asdict
from typing import List, Iterator, Optional

import elasticsearch
from elasticsearch import Elasticsearch

from backoff_decorator import backoff
from settings import ElasticConfig
from state import State
from schema import ESMovies
from es_index import INDEX_MOVIES


logger = logging.getLogger(__name__)


class ElasticLoader:
    def __init__(
        self,
        state: State,
        config: ElasticConfig,
        index: str,
        elastic_conn: Optional[Elasticsearch] = None,
    ) -> None:
        self.state = state
        self.config = config
        self.index = index
        self.elastic_conn = elastic_conn

    @property
    def elastic_connection(self) -> Elasticsearch:
        """Вернуть текущее подключение для ES или инициализировать новое"""
        try:
            if self.elastic_conn is None or not self.elastic_conn.ping():
                self.elastic_conn = self.create_connection()
        except elasticsearch.exceptions.ConnectionError as er:
            logger.error('Elasticserarch connection error: %s', er)
            return
        return self.elastic_conn

    @backoff(start_sleep_time=0.5)
    def create_connection(self) -> Elasticsearch:
        """Создать новое подключение для ES"""
        return Elasticsearch(
            host=self.config.host,
            port=self.config.port,
            scheme=self.config.scheme
            )

    @backoff(start_sleep_time=0.5)
    def create_index_if_not_exists(self) -> None:
        """Создаёт индекс, если его не существовало."""
        try:
            self.elastic_connection.indices.create(
                index=self.index,
                body=INDEX_MOVIES
            )
        except elasticsearch.exceptions.RequestError as er:
            if er.error == 'resource_already_exists_exception':
                pass

    @backoff(start_sleep_time=0.5)
    def bulk_update(self, docs: Iterator[List[ESMovies]]) -> None:
        """Загружает данные в ES используя итератор"""
        body = ''
        for doc in docs:

            doc = asdict(doc)
            modified = doc.pop('modified')

            index = {'index': {'_index': self.index, '_id': doc['id']}}
            body += json.dumps(index) + '\n' + json.dumps(doc) + '\n'

            self.state.set_state('modified', str(modified))

        if body:
            try:
                results = self.elastic_connection.bulk(body)

                params = {'bytes': 'b', 'format': 'json'}
                res = self.elastic_conn.cat.indices(
                        self.index,
                        params=params)
                logger.info(
                    'Successful updated. Doc\'s count: %s', res[0]['docs.count']
                )
                if results['errors']:
                    error = [
                        result['index'] for result in results['items']
                        if result['index']['status'] != 200
                    ]
                    logger.info(error)
                    return
            except Exception:
                return
        else:
            logger.info("Nothing to update for index: %s", self.index)
            return
