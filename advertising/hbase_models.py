from common.hbase_helper import HbaseHelper
from common.utils import revert_str


class baseModel(object):
    """
    base model for hbase models.

    More about functions.
    create_or_update - actually call happybase put func.
    """
    _table_name = ''
    _colunm = ''

    def __init__(self):
        self.__connpool = HbaseHelper.get_connpool()

    def format_data(self, data):
        """
        Use to format data before updating/creating
        More about params.
        data - format :{key:value, ...}
        """
        return {self._colunm + ':' + k: str(v) for k, v in data.items()}

    @classmethod
    def generate_rowkey(cls, profile_id, *args):
        """
        :param profile_id:
        :param args: can be adGroupId, keywordId, campaignId,
        keywordId, adId, targetId
        :return: rowkey.
        """
        rowkey = revert_str(profile_id)
        for part_id in args:
            rowkey += ':' + str(part_id)
        return rowkey

    @classmethod
    def __format_column(cls, fields):
        """
        Get columns by fields.
        """
        return None if not fields else \
            [cls._colunm + ':' + field for field in fields]

    @classmethod
    def __format_query_result(cls, byte_dic):
        return {
            key.decode('utf-8').split(':')[1]: value.decode('utf-8')
            for key, value in byte_dic.items()
        }

    def put(self, rowkey, data):
        with self.__connpool.connection() as conn:
            table = conn.table(self._table_name)
            table.put(rowkey, self.format_data(data))
            return table.row(rowkey)

    def put_batch(self, batch_data):
        """
        :param batch_data: [{"rowkey":"","value":""},{...},{...},...]
        :return:
        """
        with self.__connpool.connection() as conn:
            table = conn.table(self._table_name)
            b = table.batch()
            for data in batch_data:
                rowkey = data['rowkey']
                value = data['value']
                b.put(rowkey, self.format_data(value))
            b.send()

    def get(self, rowkey, fields=None):
        """
        :param rowkey:
        :param fields:
        for example ['name', 'campaignId', 'targetingType', 'state',
                   'dailyBudget', 'startDate']
        :return:
        """
        with self.__connpool.connection() as conn:
            table = conn.table(self._table_name)
            columns = self.format_cloumn(fields)
            data = table.row(rowkey, columns)
            return self.__format_query_result(data)

    def put_batch(self, batch_data):
        """
        :param batch_data: [{"rowkey":"","value":""},{...},{...},...]
        :return:
        """
        with self.__connpool.connection() as conn:
            table = conn.table(self._table_name)
            b = table.batch()
            for data in batch_data:
                rowkey = data['rowkey']
                value = data['value']
                b.put(rowkey, self.format_data(value))
            b.send()

    def scan(self, **kwargs):
        with self.__connpool.connection() as conn:
            table = conn.table(self._table_name)
            return [self.__format_query_result(row)
                    for key, row in table.scan(**kwargs)]

    def scan_to_dict(self, field_name, **kwargs):
        with self.__connpool.connection() as conn:
            table = conn.table(self._table_name)
            col_name = (self._colunm + ':' + field_name).encode('utf-8')
            return {
                row[col_name].decode('utf-8'): self.__format_query_result(row)
                for key, row in table.scan(**kwargs)}

    def rows_to_dict(self, field_name, **kwargs):
        with self.__connpool.connection() as conn:
            table = conn.table(self._table_name)
            col_name = (self._colunm + ':' + field_name).encode('utf-8')
            return {
                row[col_name].decode('utf-8'): self.__format_query_result(row)
                for key, row in table.rows(**kwargs)}


class AdGroupBidRec(baseModel):
    _table_name = 'sellection_advertising_adgroup_bid_rec'
    _colunm = 'bidRec'

    def get_ad_group_bidrec_all(self, profile_id):
        """用于:定时任务 去重adGroupId，去重后,从亚马逊再继续查询"""
        row_prefix = self.generate_rowkey(profile_id).encode('utf-8')
        adgroup_dic_list = self.scan(row_prefix=row_prefix)
        return [adgroup_dic["adGroupId"] for adgroup_dic in adgroup_dic_list]


class KeyWordBidRec(baseModel):
    _table_name = 'sellection_advertising_keyword_bid_rec'
    _column = 'bidRec'

    @classmethod
    def kw_rowkey_hash(cls, profile_id,  keyword, match_type):
        return cls.generate_rowkey(profile_id, hash(keyword+'_'+match_type))

    def get_raw_keyword_bidrec_all(self, profile_id):
        """
        会用于定时任务中，查询到所有的已经保存过的关键词竞价信息,去重后，从亚马逊再继续查询
        :param profile_id:
        :return:
        """
        row_prefix = self.generate_rowkey(profile_id).encode('utf-8')
        keyword_dic_list = self.scan(row_prefix=row_prefix)
        return [(keyword_dic["keywordText"], keyword_dic["matchType"],
                 keyword_dic["adGroupId"])
                for keyword_dic in keyword_dic_list]

    def format_kw_bidrec_batch(self, profile_id, bid_rec_list):
        """
        批量保存数据到hbase用到的中间数据处理,hash后生成rowkey
        :param profile_id:
        :param bid_rec_list:
        :return:
        """
        return [{"rowkey": self.kw_rowkey_hash(
                profile_id, bid_rec["keyword"], bid_rec["matchType"]),
                "value":bid_rec} for bid_rec in bid_rec_list]


class Campaign(baseModel):
    _table_name = 'data_report_entity_campaigns'
    _column = 'campaign'


class AdGroup(baseModel):
    _table_name = 'data_report_entity_ad_groups'
    _colunm = 'adGroup'

    def get_ad_group_all(self, profile_id):
        """用于:定时任务 去重adGroupId"""
        row_prefix = self.generate_rowkey(profile_id).encode('utf-8')
        adgroup_dic_list = self.scan(row_prefix=row_prefix)
        return [adgroup_dic["adGroupId"] for adgroup_dic in adgroup_dic_list]

    def get_ad_group_campaign(self, profile_id, campaign_id):
        row_prefix = self.generate_rowkey(profile_id).encode('utf-8')
        filter_str = "SingleColumnValueFilter('adGroup', \
                    'campaignId',=,'substring:%s')" % campaign_id
        adgroup_dic_list = self.scan(row_prefix=row_prefix,
                                     filter=bytes(filter_str.encode('utf-8')))
        return [adgroup_dic["adGroupId"] for adgroup_dic in adgroup_dic_list]


class ProductAds(baseModel):
    _table_name = 'data_report_entity_productAds'
    _colunm = 'adId'


class KeyWord(baseModel):
    _table_name = 'data_report_entity_keywords'
    _colunm = 'keyword'

    def get_keyword_id_all(self, profile_id):
        row_prefix = self.generate_rowkey(profile_id).encode('utf-8')
        keyword_dic_list = self.scan(row_prefix=row_prefix)
        return [
            (keyword_dic["keywordText"], keyword_dic["matchType"],
             keyword_dic["adGroupId"])
            for keyword_dic in keyword_dic_list
        ]

    def get_kw_campaign(self, profile_id, campaign_id):
        row_prefix = self.generate_rowkey(profile_id).encode('utf-8')
        filter_str = "SingleColumnValueFilter('keyword', \
            'campaignId',=,'substring:%s')" % campaign_id
        keyword_dic_list = self.scan(row_prefix=row_prefix, filter=filter_str)
        return keyword_dic_list


class CampaignNegativeKeyword(baseModel):
    _table_name = 'data_report_entity_campaign_negative_keywords'
    _colunm = 'keyword'

    def get_cp_negative_kw_all(self, profile_id, campaign_id):
        row_prefix = self.generate_rowkey(profile_id).encode('utf-8')
        filter_str = "SingleColumnValueFilter('keyword', \
            'campaignId',=,'substring:%s')" % campaign_id
        return self.scan(row_prefix=row_prefix, filter=filter_str)


class AdgroupNegativeKeyword(baseModel):
    _table_name = 'data_report_entity_negativeKeywords'
    _column = 'keyword'

    def get_adgroup_negative_kw_all(self, profile_id, adgroup_id):
        row_prefix = self.generate_rowkey(profile_id).encode('utf-8')
        filter_str = "SingleColumnValueFilter('keyword', \
            'adGroupId',=,'substring:%s')" % adgroup_id
        return self.scan(row_prefix=row_prefix, filter=filter_str)
