import time
import phoenixdb
import phoenixdb.cursor
from django.conf import settings
import logging

hbase_url = 'https://%s:%s' % \
    (settings.DATA_DB_HOST, settings.DATA_DB_PORT)

logger = logging.getLogger('amazonads')


class HBaseHelper:
    """use to operate hbase through pheonix."""

    @classmethod
    def execute(cls, sql_str, params=None):
        conn = phoenixdb.connect(hbase_url, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(sql_str, params)

    @classmethod
    def query(cls, sql_str, params=None):
        conn = phoenixdb.connect(hbase_url, autocommit=True)
        cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
        try:
            time_start = time.time()
            cursor.execute(sql_str, params)
            if time.time() - time_start > 30:
                HBaseHelper.clear_status_table(conn)
        except Exception as ex:
            HBaseHelper.clear_status_table(conn)
            raise ex
        return cursor.fetchall()

    @classmethod
    def clear_status_table(cls, conn):
        cursor = conn.cursor()
        cursor.execute(
            'delete from SYSTEM.STATS \
             where PHYSICAL_NAME =\'data_report_keywords\''
        )
        cursor.execute(
            'delete from SYSTEM.STATS \
             where PHYSICAL_NAME =\'data_report_product_ads\''
        )
        logger.info('clear system.status of hbase table')
