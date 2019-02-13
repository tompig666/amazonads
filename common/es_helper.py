from django.conf import settings
from elasticsearch import Elasticsearch


es_username = settings.DATA_DB_ES_USERNAME
es_password = settings.DATA_DB_ES_PASSWORD
es_host = settings.DATA_DB_ES_HOST
es_port = settings.DATA_DB_ES_PORT


class EsHelper:
    """ Use to access es."""

    def __init__(self):
        self._conn = Elasticsearch(['%s:%s@%s:%s' %
                                    (es_username, es_password, es_host, es_port)])

    def search(self, index, body):
        return self._conn.search(index=index, body=body)
