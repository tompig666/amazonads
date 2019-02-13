import happybase
from django.conf import settings

db_host = settings.DATA_DB_HOST
db_port = 9090


class HbaseHelper:
    """ Use to access Hbase."""

    @classmethod
    def get_connpool(cls):
        return happybase.ConnectionPool(host=db_host, port=db_port, size=5)
