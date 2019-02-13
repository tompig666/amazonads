import codecs
import csv
import datetime

from django.http import HttpResponse

from advertising.hbase_models import Campaign, AdGroup, ProductAds,\
    KeyWord, KeyWordBidRec, AdGroupBidRec
from common.es_helper import EsHelper
from report.utils import format_report


class SellerReportRetriver:
    @staticmethod
    def getReportUpdateTime(profile_id):
        body = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [{
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "profile": {
                                            "query": profile_id
                                        }
                                    }
                                }]
                            }
                        }]
                    }
                },
                "aggs": {
                    "report_update_time": {
                        "stats": {
                            "field": "updatedAt"
                        }
                    }
                },
                "_source": {
                    "includes": ["updatedAt"]
                }
            }
        data = EsHelper().search(
            index='selection_amazonads_report_campaign', body=body)
        report_update_time = data['aggregations']['report_update_time']['max']
        return data['aggregations']['report_update_time']['max_as_string'] if\
            report_update_time else ''

    @staticmethod
    def getSellerCampaignSummaries(profile_id, min_date, max_date):
        body = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [{
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "profile": {
                                            "query": profile_id
                                        }
                                    }
                                }]
                            }
                        }, {
                            "range": {
                                "date": {
                                    "gte": min_date,
                                    "lte": max_date
                                }
                            }
                        }]
                    }
                },
                "aggs": {
                    "spend": {
                        "stats": {
                            "field": "cost"
                        }
                    },
                    "sales": {
                        "stats": {
                            "field": "attributedSales7d"
                        }
                    },
                    "orders": {
                        "stats": {
                            "field": "attributedConversions7d"
                        }
                    },
                    "clicks": {
                        "stats": {
                            "field": "clicks"
                        }
                    },
                    "impressions": {
                        "stats": {
                            "field": "impressions"
                        }
                    }
                },
                "_source": {
                    "includes": ["cost", "attributedSales7d",
                                 "attributedConversions7d",
                                 "clicks", "impressions"]
                }
            }
        res = EsHelper().search(
            index='selection_amazonads_report_campaign', body=body)
        if res['aggregations']['spend']['count'] == 0:
            return dict()
        data_dic = format_report({
            'acos': (res['aggregations']['spend']['sum'] /
                     res['aggregations']['sales']['sum']) * 100
            if res['aggregations']['sales']['sum'] != 0 else 0.00,
            'spend': res['aggregations']['spend']['sum'],
            'sales': res['aggregations']['sales']['sum'],
            'clicks': res['aggregations']['clicks']['sum'],
            'orders': res['aggregations']['orders']['sum'],
            'impressions': res['aggregations']['impressions']['sum'],
            'cpc': (res['aggregations']['spend']['sum'] /
                    res['aggregations']['clicks']['sum']) * 100
            if res['aggregations']['clicks']['sum'] != 0 else 0.00,
            'ctr': res['aggregations']['clicks']['sum'] /
                   res['aggregations']['impressions']['sum'] * 100
            if res['aggregations']['impressions']['sum'] != 0 else 0.00,
            'conversionRate': res['aggregations']['orders']['sum'] /
                              res['aggregations']['clicks']['sum'] * 100
            if res['aggregations']['clicks']['sum'] != 0 else 0.00
        })
        return data_dic

    @staticmethod
    def getSellerCampaignTrend(profile_id, min_date, max_date):
        body = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [{
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "profile": {
                                            "query": profile_id
                                        }
                                    }
                                }]
                            }
                        }, {
                            "range": {
                                "date": {
                                    "gte": min_date,
                                    "lte": max_date
                                }
                            }
                        }]
                    }
                },
                "aggs": {
                    "date": {
                        "terms": {
                            "field": "date",
                            "size": 2147483647
                        },
                        "aggs": {
                            "spend": {
                                "sum": {
                                    "field": "cost"
                                }
                            },
                            "sales": {
                                "sum": {
                                    "field": "attributedSales7d"
                                }
                            },
                            "orders": {
                                "sum": {
                                    "field": "attributedConversions7d"
                                }
                            },
                            "clicks": {
                                "sum": {
                                    "field": "clicks"
                                }
                            },
                            "impressions": {
                                "sum": {
                                    "field": "impressions"
                                }
                            },
                            "top": {
                                "top_hits": {
                                    "size": 1
                                }
                            }
                        }
                    }
                }
            }
        res = EsHelper().search(
            index='selection_amazonads_report_campaign', body=body)
        data_list = list(map(format_report, [{
            "impressions": data['impressions']['value'],
            "orders": data['orders']['value'],
            "sales": data['sales']['value'],
            "spend": data['spend']['value'],
            "clicks": data['clicks']['value'],
            'acos': data['spend']['value'] / data['sales']['value'] * 100
            if data['sales']['value'] != 0 else 0.00,
            'cpc': data['spend']['value'] / data['clicks']['value'] * 100
            if data['clicks']['value'] != 0 else 0.00,
            'ctr': data['clicks']['value'] / data['impressions']['value'] * 100
            if data['impressions']['value'] != 0 else 0.00,
            'conversionRate': data['orders']['value'] /
                              data['clicks']['value'] * 100
            if data['clicks']['value'] != 0 else 0.00,
            'date': data['key'][:4] + '-' + data['key'][4:6]
                    + '-' + data['key'][6:]
        }
            for data in res['aggregations']['date']['buckets']
        ]))
        min_date = min_date[:4] + '-' + min_date[4:6] + '-' + min_date[6:]
        max_date = max_date[:4] + '-' + max_date[4:6] + '-' + max_date[6:]
        getEveryDayData(min_date, max_date, data_list).sort(
            key=lambda value: value['date'])
        return data_list

    @staticmethod
    def getCampaignList(profile_id, min_date, max_date,
                        order_by, order_type, size, current,
                        filter_condition, download):
        fields = ['campaignId', 'dailyBudget', 'startDate',
                  'state', 'targetingType', 'endDate']
        if filter_condition:
            body = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [{
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "profile": {
                                                "query": profile_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "range": {
                                    "date": {
                                        "gte": min_date,
                                        "lte": max_date
                                    }
                                }
                            }]
                        }
                    },
                    "aggs": {
                        "campaignId": {
                            "terms": {
                                "field": "campaignId",
                                "size": 2147483647
                            },
                            "aggs": {
                                "spend": {
                                    "sum": {
                                        "field": "cost"
                                    }
                                },
                                "sales": {
                                    "sum": {
                                        "field": "attributedSales7d"
                                    }
                                },
                                "orders": {
                                    "sum": {
                                        "field": "attributedConversions7d"
                                    }
                                },
                                "clicks": {
                                    "sum": {
                                        "field": "clicks"
                                    }
                                },
                                "impressions": {
                                    "sum": {
                                        "field": "impressions"
                                    }
                                },
                                "top": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
            es_data = EsHelper().search(
                index='selection_amazonads_report_campaign', body=body)
            es_data_list = list(map(format_report, [{
                'campaignId': data['key'],
                "campaignName":
                    data['top']['hits']['hits'][0]['_source']['campaignName'],
                "impressions": data['impressions']['value'],
                "orders": data['orders']['value'],
                "sales": data['sales']['value'],
                "spend": data['spend']['value'],
                "clicks": data['clicks']['value'],
                'acos': data['spend']['value'] / data['sales']['value'] * 100
                if data['sales']['value'] != 0 else 0.00,
                'cpc': data['spend']['value'] / data['clicks']['value'] * 100
                if data['clicks']['value'] != 0 else 0.00,
                'ctr': data['clicks']['value'] /
                       data['impressions']['value'] * 100
                if data['impressions']['value'] != 0 else 0.00,
                'conversionRate': data['orders']['value'] /
                                  data['clicks']['value'] * 100
                if data['clicks']['value'] != 0 else 0.00
            }
                for data in es_data['aggregations']['campaignId']['buckets']
            ]))
            if 'dailyBudget' in filter_condition:
                filter_condition.pop('dailyBudget')
            es_data_dic = filterData(
                filter_condition, es_data_list, 'campaignId')
            total = len(list(es_data_dic.keys()))
            campaign_id_list = list(es_data_dic.keys())
            if download != "true":
                '''先计算总数，再分页，再hbase查询'''
                es_data_dic = {key: es_data_dic[key] for key in list(
                    es_data_dic.keys())[size * (current - 1):size * current]}
                campaign_id_list = list(es_data_dic.keys())

            rows = []
            for campaign_id in campaign_id_list:
                rowkey = Campaign().generate_rowkey(profile_id, campaign_id)
                rows.append(rowkey)
            columns = Campaign().format_cloumn(fields)
            hbase_data_dic = Campaign().rows_to_dict(
                field_name='campaignId', rows=rows, columns=columns)

        elif not filter_condition:
            row_prefix = str(profile_id[::-1]).encode('utf-8')
            columns = Campaign().format_cloumn(fields)
            hbase_data_dic = Campaign().scan_to_dict(
                field_name='campaignId', row_prefix=row_prefix, columns=columns)
            total = len(list(hbase_data_dic.keys()))
            campaign_id_list = list(hbase_data_dic.keys())
            if download != "true":
                """先统计总数，再分页，再进行es查询"""
                hbase_data_dic = {key: hbase_data_dic[key] for key in
                                  list(hbase_data_dic.keys())[
                                  size * (current - 1):size * current
                                  ]}
                campaign_id_list = list(hbase_data_dic.keys())
            body = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [{
                                "terms": {
                                    "campaignId": campaign_id_list
                                }
                            }, {
                                "range": {
                                    "date": {
                                        "gte": min_date,
                                        "lte": max_date
                                    }
                                }
                            }]
                        }
                    },
                    "aggs": {
                        "campaignId": {
                            "terms": {
                                "field": "campaignId",
                                "size": 2147483647
                            },
                            "aggs": {
                                "spend": {
                                    "sum": {
                                        "field": "cost"
                                    }
                                },
                                "sales": {
                                    "sum": {
                                        "field": "attributedSales7d"
                                    }
                                },
                                "orders": {
                                    "sum": {
                                        "field": "attributedConversions7d"
                                    }
                                },
                                "clicks": {
                                    "sum": {
                                        "field": "clicks"
                                    }
                                },
                                "impressions": {
                                    "sum": {
                                        "field": "impressions"
                                    }
                                },
                                "top": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
            es_data = EsHelper().search(
                index='selection_amazonads_report_campaign', body=body)
            es_data_dic = {data['key']: {
                'campaignId': data['key'],
                "campaignName":
                    data['top']['hits']['hits'][0]['_source']['campaignName'],
                "impressions": str(int(data['impressions']['value'])),
                "orders": str(int(data['orders']['value'])),
                "sales": str("%.2f" % data['sales']['value']),
                "spend": str("%.2f" % data['spend']['value']),
                "clicks": str(int(data['clicks']['value'])),
                'acos': str("%.2f" % (
                        data['spend']['value'] / data['sales']['value'] * 100))
                if data['sales']['value'] != 0 else "0.00",
                'cpc': str("%.2f" % (
                        data['spend']['value'] / data['clicks']['value'] * 100))
                if data['clicks']['value'] != 0 else "0.00",
                'ctr': str("%.2f" % (
                        data['clicks']['value'] /
                        data['impressions']['value'] * 100))
                if data['impressions']['value'] != 0 else "0.00",
                'conversionRate': str(
                    "%.2f" % (data['orders']['value'] /
                              data['clicks']['value'] * 100))
                if data['clicks']['value'] != 0 else "0.00"
            }
                for data in es_data['aggregations']['campaignId']['buckets']
            }

        data_list = []
        # 以hbase的结果为主，遍历hbase的keys进行合并
        for campaign_id in hbase_data_dic.keys():
            merged = dict(hbase_data_dic[campaign_id],
                          **(es_data_dic[campaign_id] if
                             campaign_id in es_data_dic else
                             {'impressions': '0', 'orders': '0',
                              'sales': '0.00', 'spend': '0.00', 'clicks': '0',
                              'acos': '0.00', 'cpc': '0.00', 'ctr': '0.00',
                              'conversionRate': '0.00'}))
            merged['startDate'] = \
                merged['startDate'][:4] + '/' + merged['startDate'][4:6] +\
                '/' + merged['startDate'][6:]
            merged['endDate'] = \
                merged['endDate'][:4] + '/' + merged['endDate'][4:6] + '/' +\
                merged['endDate'][6:] if 'endDate' in merged else ""
            data_list.append(merged)
        if download == "true":
            # Create the HttpResponse object with the appropriate CSV header.
            file_name = "%s-campaignList.csv" % datetime.datetime.now()
            response = downloadData(data_list, file_name, ['campaignId'])
            return response
        order_type = True if order_type == 'desc' else False
        if order_by == 'startDate':
            data_list.sort(key=lambda val: int(
                val['%s' % order_by].replace('/', '')), reverse=order_type)
        elif order_by == "endDate":
            pass
        else:
            data_list.sort(key=lambda val: float(
                val['%s' % order_by]), reverse=order_type)
        return total, data_list

    @staticmethod
    def getAdGroupSummaries(profile_id, campaign_id, min_date, max_date):
        body = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [{
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "profile": {
                                            "query": profile_id
                                        }
                                    }
                                }]
                            }
                        }, {
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "campaignId": {
                                            "query": campaign_id
                                        }
                                    }
                                }]
                            }
                        }, {
                            "range": {
                                "date": {
                                    "gte": min_date,
                                    "lte": max_date
                                }
                            }
                        }]
                    }
                },
                "aggs": {
                    "spend": {
                        "stats": {
                            "field": "cost"
                        }
                    },
                    "sales": {
                        "stats": {
                            "field": "attributedSales7d"
                        }
                    },
                    "orders": {
                        "stats": {
                            "field": "attributedConversions7d"
                        }
                    },
                    "clicks": {
                        "stats": {
                            "field": "clicks"
                        }
                    },
                    "impressions": {
                        "stats": {
                            "field": "impressions"
                        }
                    }
                },
                "_source": {
                    "includes": ["cost", "attributedSales7d",
                                 "attributedConversions7d",
                                 "clicks", "impressions"]
                }
            }
        res = EsHelper().search(
            index='selection_amazonads_report_adgroup', body=body)
        if res['aggregations']['spend']['count'] == 0:
            return dict()
        data_dic = format_report({
            'acos': (res['aggregations']['spend']['sum'] /
                     res['aggregations']['sales']['sum']) * 100
            if res['aggregations']['sales']['sum'] != 0 else 0.00,
            'spend': res['aggregations']['spend']['sum'],
            'sales': res['aggregations']['sales']['sum'],
            'clicks': res['aggregations']['clicks']['sum'],
            'orders': res['aggregations']['orders']['sum'],
            'impressions': res['aggregations']['impressions']['sum'],
            'cpc': (res['aggregations']['spend']['sum'] /
                    res['aggregations']['clicks']['sum']) * 100
            if res['aggregations']['clicks']['sum'] != 0 else 0.00,
            'ctr': res['aggregations']['clicks']['sum'] /
                   res['aggregations']['impressions']['sum'] * 100
            if res['aggregations']['impressions']['sum'] != 0 else 0.00,
            'conversionRate': res['aggregations']['orders']['sum'] /
                              res['aggregations']['clicks']['sum'] * 100
            if res['aggregations']['clicks']['sum'] != 0 else 0.00
        })
        return data_dic

    @staticmethod
    def getAdGroupTrend(profile_id, campaign_id, min_date, max_date):
        body = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [{
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "profile": {
                                            "query": profile_id
                                        }
                                    }
                                }]
                            }
                        }, {
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "campaignId": {
                                            "query": campaign_id
                                        }
                                    }
                                }]
                            }
                        }, {
                            "range": {
                                "date": {
                                    "gte": min_date,
                                    "lte": max_date
                                }
                            }
                        }]
                    }
                },
                "aggs": {
                    "date": {
                        "terms": {
                            "field": "date",
                            "size": 2147483647
                        },
                        "aggs": {
                            "spend": {
                                "sum": {
                                    "field": "cost"
                                }
                            },
                            "sales": {
                                "sum": {
                                    "field": "attributedSales7d"
                                }
                            },
                            "orders": {
                                "sum": {
                                    "field": "attributedConversions7d"
                                }
                            },
                            "clicks": {
                                "sum": {
                                    "field": "clicks"
                                }
                            },
                            "impressions": {
                                "sum": {
                                    "field": "impressions"
                                }
                            },
                            "top": {
                                "top_hits": {
                                    "size": 1
                                }
                            }
                        }
                    }
                }
            }
        res = EsHelper().search(
            index='selection_amazonads_report_adgroup', body=body)
        data_list = list(map(format_report, [{
            "impressions": data['impressions']['value'],
            "orders": data['orders']['value'],
            "sales": data['sales']['value'],
            "spend": data['spend']['value'],
            "clicks": data['clicks']['value'],
            'acos': data['spend']['value'] / data['sales']['value'] * 100
            if data['sales']['value'] != 0 else 0.00,
            'cpc': data['spend']['value'] / data['clicks']['value'] * 100
            if data['clicks']['value'] != 0 else 0.00,
            'ctr': data['clicks']['value'] / data['impressions']['value'] * 100
            if data['impressions']['value'] != 0 else 0.00,
            'conversionRate': data['orders']['value'] /
                              data['clicks']['value'] * 100
            if data['clicks']['value'] != 0 else 0.00,
            'date': data['key'][:4] + '-' + data['key'][4:6] + '-' +
                    data['key'][6:]
        }
            for data in res['aggregations']['date']['buckets']
        ]))
        min_date = min_date[:4] + '-' + min_date[4:6] + '-' + min_date[6:]
        max_date = max_date[:4] + '-' + max_date[4:6] + '-' + max_date[6:]
        getEveryDayData(min_date, max_date, data_list).sort(
            key=lambda value: value['date'])
        return data_list

    @staticmethod
    def getAdGroupList(profile_id, campaign_id, targeting_type,
                       min_date, max_date, order_by, order_type,
                       size, current, filter_condition, download):
        fields = ['adGroupId', 'defaultBid', 'state', 'campaignId']
        if filter_condition:
            body = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [{
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "profile": {
                                                "query": profile_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "campaignId": {
                                                "query": campaign_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "range": {
                                    "date": {
                                        "gte": min_date,
                                        "lte": max_date
                                    }
                                }
                            }]
                        }
                    },
                    "aggs": {
                        "adGroupId": {
                            "terms": {
                                "field": "adGroupId",
                                "size": 2147483647
                            },
                            "aggs": {
                                "spend": {
                                    "sum": {
                                        "field": "cost"
                                    }
                                },
                                "sales": {
                                    "sum": {
                                        "field": "attributedSales7d"
                                    }
                                },
                                "orders": {
                                    "sum": {
                                        "field": "attributedConversions7d"
                                    }
                                },
                                "clicks": {
                                    "sum": {
                                        "field": "clicks"
                                    }
                                },
                                "impressions": {
                                    "sum": {
                                        "field": "impressions"
                                    }
                                },
                                "top": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
            es_data = EsHelper().search(
                index='selection_amazonads_report_adgroup', body=body)
            es_data_list = list(map(format_report, [{
                'adGroupId': data['key'],
                "adGroupName":
                    data['top']['hits']['hits'][0]['_source']['adGroupName'],
                'targetingType': targeting_type,
                "impressions": data['impressions']['value'],
                "orders": data['orders']['value'],
                "sales": data['sales']['value'],
                "spend": data['spend']['value'],
                "clicks": data['clicks']['value'],
                'acos': data['spend']['value'] / data['sales']['value'] * 100
                if data['sales']['value'] != 0 else 0.00,
                'cpc': data['spend']['value'] / data['clicks']['value'] * 100
                if data['clicks']['value'] != 0 else 0.00,
                'ctr':
                    data['clicks']['value'] / data['impressions']['value'] * 100
                if data['impressions']['value'] != 0 else 0.00,
                'conversionRate':
                    data['orders']['value'] / data['clicks']['value'] * 100
                if data['clicks']['value'] != 0 else 0.00
            }
                for data in es_data['aggregations']['adGroupId']['buckets']
            ]))
            es_data_dic = filterData(filter_condition, es_data_list, 'adGroupId')
            ad_group_id_list = list(es_data_dic.keys())
            total = len(list(es_data_dic.keys()))
            if download != "true":
                '''先计算总数，再分页，再hbase查询'''
                es_data_dic = {key: es_data_dic[key] for key in list(
                    es_data_dic.keys())[size * (current - 1):size * current]}
                ad_group_id_list = list(es_data_dic.keys())

            rows = []
            for ad_group_id in ad_group_id_list:
                rowkey = AdGroup().generate_rowkey(profile_id, ad_group_id)
                rows.append(rowkey)
            columns = AdGroup().format_cloumn(fields)
            hbase_data_dic = AdGroup().rows_to_dict(
                field_name='adGroupId', rows=rows, columns=columns)

        elif not filter_condition:
            row_prefix = str(profile_id[::-1]).encode('utf-8')
            columns = AdGroup().format_cloumn(fields)
            # 根据profile前缀和campaign进行过滤查找adGroup
            filter_str = "SingleColumnValueFilter('adGroup', \
                                 'campaignId',=,'substring:%s')" % campaign_id
            hbase_data_dic = AdGroup().scan_to_dict(
                field_name='adGroupId', row_prefix=row_prefix,
                columns=columns, filter=filter_str)
            total = len(list(hbase_data_dic.keys()))
            ad_group_id_list = list(hbase_data_dic.keys())
            if download != "true":
                """先统计总数，再分页，再进行es查询"""
                hbase_data_dic = {key: hbase_data_dic[key] for key in
                                  list(hbase_data_dic.keys())
                                  [size * (current - 1):size * current]}
                ad_group_id_list = list(hbase_data_dic.keys())

            body = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [{
                                "terms": {
                                    "adGroupId": ad_group_id_list
                                }
                            }, {
                                "range": {
                                    "date": {
                                        "gte": min_date,
                                        "lte": max_date
                                    }
                                }
                            }]
                        }
                    },
                    "aggs": {
                        "adGroupId": {
                            "terms": {
                                "field": "adGroupId",
                                "size": 2147483647
                            },
                            "aggs": {
                                "spend": {
                                    "sum": {
                                        "field": "cost"
                                    }
                                },
                                "sales": {
                                    "sum": {
                                        "field": "attributedSales7d"
                                    }
                                },
                                "orders": {
                                    "sum": {
                                        "field": "attributedConversions7d"
                                    }
                                },
                                "clicks": {
                                    "sum": {
                                        "field": "clicks"
                                    }
                                },
                                "impressions": {
                                    "sum": {
                                        "field": "impressions"
                                    }
                                },
                                "top": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
            es_data = EsHelper().search(
                index='selection_amazonads_report_adgroup', body=body)
            es_data_dic = {data['key']: {
                'adGroupId': data['key'],
                "adGroupName":
                    data['top']['hits']['hits'][0]['_source']['campaignName'],
                'targetingType': targeting_type,
                "impressions": str(int(data['impressions']['value'])),
                "orders": str(int(data['orders']['value'])),
                "sales": str("%.2f" % data['sales']['value']),
                "spend": str("%.2f" % data['spend']['value']),
                "clicks": str(int(data['clicks']['value'])),
                'acos': str("%.2f" % (data['spend']['value'] /
                                      data['sales']['value'] * 100))
                if data['sales']['value'] != 0 else "0.00",
                'cpc': str("%.2f" % (data['spend']['value'] /
                                     data['clicks']['value'] * 100))
                if data['clicks']['value'] != 0 else "0.00",
                'ctr': str("%.2f" % (data['clicks']['value'] /
                                     data['impressions']['value'] * 100))
                if data['impressions']['value'] != 0 else "0.00",
                'conversionRate': str("%.2f" % (data['orders']['value'] /
                                                data['clicks']['value'] * 100))
                if data['clicks']['value'] != 0 else "0.00"
            }
                for data in es_data['aggregations']['adGroupId']['buckets']
            }

        # 从hbase的adgroupBidRec表中查询ad_group_bid_rec
        rows = []
        for ad_group_id in ad_group_id_list:
            rowkey = AdGroupBidRec().generate_rowkey(profile_id, ad_group_id)
            rows.append(rowkey)
        bid_rec_columns = AdGroupBidRec().format_cloumn(
            ['adGroupId', 'rangeEnd', 'rangeStart', 'suggested'])
        ad_group_bid_rec_dic = AdGroupBidRec().rows_to_dict(
            field_name='adGroupId', rows=rows, columns=bid_rec_columns)

        # 从hbase的campaign实体表查目标acos,一个campaign对应的adGroup的目标acos都相同
        rowkey = Campaign().generate_rowkey(profile_id, campaign_id)
        targeting_field = ['targetingAcos']
        targeting_acos_dic = Campaign().get(rowkey, targeting_field)
        targeting_acos = ""
        if "targetingAcos" in targeting_acos_dic:
            targeting_acos = targeting_acos_dic['targetingAcos']
        data_list = []
        # 以hbase的结果为主，遍历hbase的keys进行合并
        for ad_group_id in hbase_data_dic.keys():
            merged = dict(hbase_data_dic[ad_group_id],
                          **(es_data_dic[ad_group_id] if
                             ad_group_id in es_data_dic else
                             {'impressions': '0', 'orders': '0',
                              'sales': '0.00', 'spend': '0.00', 'clicks': '0',
                              'acos': '0.00', 'cpc': '0.00', 'ctr': '0.00',
                              'conversionRate': '0.00'}))
            merged = dict(merged, **ad_group_bid_rec_dic[ad_group_id] if
                            ad_group_id in ad_group_bid_rec_dic else
                            {"suggested": "", "rangeStart": "", "rangeEnd": ""})
            merged['targetingAcos'] = targeting_acos
            data_list.append(merged)
        if download == "true":
            # Create the HttpResponse object with the appropriate CSV header.
            file_name = "%s-adGroupList.csv" % datetime.datetime.now()
            response = downloadData(
                data_list, file_name, ['adGroupId', 'campaignId'])
            return response
        order_type = True if order_type == 'desc' else False
        if order_by in ["suggested", "targetingAcos"]:
            pass
        else:
            data_list.sort(key=lambda val: float(
                val['%s' % order_by]), reverse=order_type)
        return total, data_list

    @staticmethod
    def getProductAdSummaries(profile_id, ad_group_id, min_date, max_date):
        body = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [{
                        "bool": {
                            "must": [{
                                "match_phrase": {
                                    "profile": {
                                        "query": profile_id
                                    }
                                }
                            }]
                        }
                    }, {
                        "bool": {
                            "must": [{
                                "match_phrase": {
                                    "adGroupId": {
                                        "query": ad_group_id
                                    }
                                }
                            }]
                        }
                    }, {
                        "range": {
                            "date": {
                                "gte": min_date,
                                "lte": max_date
                            }
                        }
                    }]
                }
            },
            "aggs": {
                "spend": {
                    "stats": {
                        "field": "cost"
                    }
                },
                "sales": {
                    "stats": {
                        "field": "attributedSales7d"
                    }
                },
                "orders": {
                    "stats": {
                        "field": "attributedConversions7d"
                    }
                },
                "clicks": {
                    "stats": {
                        "field": "clicks"
                    }
                },
                "impressions": {
                    "stats": {
                        "field": "impressions"
                    }
                }
            },
            "_source": {
                "includes": ["cost", "attributedSales7d",
                             "attributedConversions7d", "clicks", "impressions"]
            }
        }
        res = EsHelper().search(
            index='selection_amazonads_report_productad', body=body)
        if res['aggregations']['spend']['count'] == 0:
            return dict()
        data_dic = format_report({
            'acos': (res['aggregations']['spend']['sum'] /
                     res['aggregations']['sales']['sum']) * 100
            if res['aggregations']['sales']['sum'] != 0 else 0.00,
            'spend': res['aggregations']['spend']['sum'],
            'sales': res['aggregations']['sales']['sum'],
            'clicks': res['aggregations']['clicks']['sum'],
            'orders': res['aggregations']['orders']['sum'],
            'impressions': res['aggregations']['impressions']['sum'],
            'cpc': (res['aggregations']['spend']['sum'] /
                    res['aggregations']['clicks']['sum']) * 100
            if res['aggregations']['clicks']['sum'] != 0 else 0.00,
            'ctr': res['aggregations']['clicks']['sum'] /
                   res['aggregations']['impressions']['sum'] * 100
            if res['aggregations']['impressions']['sum'] != 0 else 0.00,
            'conversionRate': res['aggregations']['orders']['sum'] /
                              res['aggregations']['clicks']['sum'] * 100
            if res['aggregations']['clicks']['sum'] != 0 else 0.00
        })
        return data_dic

    @staticmethod
    def getProductAdTrend(profile_id, ad_group_id, min_date, max_date):
        body = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [{
                        "bool": {
                            "must": [{
                                "match_phrase": {
                                    "profile": {
                                        "query": profile_id
                                    }
                                }
                            }]
                        }
                    }, {
                        "bool": {
                            "must": [{
                                "match_phrase": {
                                    "adGroupId": {
                                        "query": ad_group_id
                                    }
                                }
                            }]
                        }
                    }, {
                        "range": {
                            "date": {
                                "gte": min_date,
                                "lte": max_date
                            }
                        }
                    }]
                }
            },
            "aggs": {
                "date": {
                    "terms": {
                        "field": "date",
                        "size": 2147483647
                    },
                    "aggs": {
                        "spend": {
                            "sum": {
                                "field": "cost"
                            }
                        },
                        "sales": {
                            "sum": {
                                "field": "attributedSales7d"
                            }
                        },
                        "orders": {
                            "sum": {
                                "field": "attributedConversions7d"
                            }
                        },
                        "clicks": {
                            "sum": {
                                "field": "clicks"
                            }
                        },
                        "impressions": {
                            "sum": {
                                "field": "impressions"
                            }
                        },
                        "top": {
                            "top_hits": {
                                "size": 1
                            }
                        }
                    }
                }
            }
        }
        res = EsHelper().search(
            index='selection_amazonads_report_productad', body=body)
        data_list = list(map(format_report, [{
            "impressions": data['impressions']['value'],
            "orders": data['orders']['value'],
            "sales": data['sales']['value'],
            "spend": data['spend']['value'],
            "clicks": data['clicks']['value'],
            'acos': data['spend']['value'] / data['sales']['value'] * 100
            if data['sales']['value'] != 0 else 0.00,
            'cpc': data['spend']['value'] / data['clicks']['value'] * 100
            if data['clicks']['value'] != 0 else 0.00,
            'ctr': data['clicks']['value'] / data['impressions']['value'] * 100
            if data['impressions']['value'] != 0 else 0.00,
            'conversionRate': data['orders']['value'] /
                              data['clicks']['value'] * 100
            if data['clicks']['value'] != 0 else 0.00,
            'date': data['key'][:4] + '-' + data['key'][4:6] + '-' +
                    data['key'][6:]
        }
            for data in res['aggregations']['date']['buckets']
        ]))
        min_date = min_date[:4] + '-' + min_date[4:6] + '-' + min_date[6:]
        max_date = max_date[:4] + '-' + max_date[4:6] + '-' + max_date[6:]
        getEveryDayData(min_date, max_date, data_list).sort(
            key=lambda value: value['date'])
        return data_list

    @staticmethod
    def getAsinList(profile_id, ad_group_id, min_date, max_date, size,
                    current, order_by, order_type, filter_condition, download):
        fields = ['adGroupId', 'adId', 'state', 'asin']
        if filter_condition:
            body = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [{
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "profile": {
                                                "query": profile_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "adGroupId": {
                                                "query": ad_group_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "range": {
                                    "date": {
                                        "gte": min_date,
                                        "lte": max_date
                                    }
                                }
                            }]
                        }
                    },
                    "aggs": {
                        "asin": {
                            "terms": {
                                "field": "asin",
                                "size": 2147483647
                            },
                            "aggs": {
                                "spend": {
                                    "sum": {
                                        "field": "cost"
                                    }
                                },
                                "sales": {
                                    "sum": {
                                        "field": "attributedSales7d"
                                    }
                                },
                                "orders": {
                                    "sum": {
                                        "field": "attributedConversions7d"
                                    }
                                },
                                "clicks": {
                                    "sum": {
                                        "field": "clicks"
                                    }
                                },
                                "impressions": {
                                    "sum": {
                                        "field": "impressions"
                                    }
                                },
                                "top": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
            es_data = EsHelper().search(
                index='selection_amazonads_report_productad', body=body)
            es_data_list = list(map(format_report, [{
                "adId": str(data['top']['hits']['hits'][0]['_source']['adId']),
                "asin": data['key'],
                "impressions": data['impressions']['value'],
                "orders": data['orders']['value'],
                "sales": data['sales']['value'],
                "spend": data['spend']['value'],
                "clicks": data['clicks']['value'],
                'acos': data['spend']['value'] / data['sales']['value'] * 100
                if data['sales']['value'] != 0 else 0.00,
                'cpc': data['spend']['value'] / data['clicks']['value'] * 100
                if data['clicks']['value'] != 0 else 0.00,
                'ctr': data['clicks']['value'] /
                       data['impressions']['value'] * 100
                if data['impressions']['value'] != 0 else 0.00,
                'conversionRate': data['orders']['value'] /
                                  data['clicks']['value'] * 100
                if data['clicks']['value'] != 0 else 0.00
            }
                for data in es_data['aggregations']['asin']['buckets']
            ]))
            es_data_dic = filterData(filter_condition, es_data_list, 'adId')
            ad_id_list = list(es_data_dic.keys())
            total = len(list(es_data_dic.keys()))
            if download != "true":
                '''先计算总数，再分页，再hbase查询'''
                es_data_dic = {key: es_data_dic[key] for key in list(
                    es_data_dic.keys())[size * (current - 1):size * current]}
                ad_id_list = list(es_data_dic.keys())

            rows = []
            for ad_id in ad_id_list:
                rowkey = ProductAds().generate_rowkey(profile_id, ad_id)
                rows.append(rowkey)
            columns = ProductAds().format_cloumn(fields)
            hbase_data_dic = ProductAds().rows_to_dict(
                field_name='adId', rows=rows, columns=columns)

        elif not filter_condition:
            row_prefix = str(profile_id[::-1]).encode('utf-8')
            columns = ProductAds().format_cloumn(fields)
            # 根据profile前缀和adGroupId进行过滤查找asin
            filter_str = "SingleColumnValueFilter('productAd', \
                        'adGroupId',=,'substring:%s')" % ad_group_id
            hbase_data_dic = ProductAds().scan_to_dict(
                field_name='adId', row_prefix=row_prefix,
                columns=columns, filter=filter_str)
            ad_id_list = list(hbase_data_dic.keys())
            total = len(list(hbase_data_dic.keys()))
            if download != "true":
                """先统计总数，再分页，再进行es查询"""
                hbase_data_dic = {key: hbase_data_dic[key] for key in
                                  list(hbase_data_dic.keys())
                                  [size * (current - 1):size * current]}
                ad_id_list = list(hbase_data_dic.keys())

            body = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [{
                            "terms": {
                                "adId": ad_id_list
                            }
                        }, {
                            "range": {
                                "date": {
                                    "gte": min_date,
                                    "lte": max_date
                                }
                            }
                        }]
                    }
                },
                "aggs": {
                    "asin": {
                        "terms": {
                            "field": "asin",
                            "size": 2147483647
                        },
                        "aggs": {
                            "spend": {
                                "sum": {
                                    "field": "cost"
                                }
                            },
                            "sales": {
                                "sum": {
                                    "field": "attributedSales7d"
                                }
                            },
                            "orders": {
                                "sum": {
                                    "field": "attributedConversions7d"
                                }
                            },
                            "clicks": {
                                "sum": {
                                    "field": "clicks"
                                }
                            },
                            "impressions": {
                                "sum": {
                                    "field": "impressions"
                                }
                            },
                            "top": {
                                "top_hits": {
                                    "size": 1
                                }
                            }
                        }
                    }
                }
            }
            es_data = EsHelper().search(
                index='selection_amazonads_report_productad', body=body)
            es_data_dic = {data['key']: {
                "adId": str(data['top']['hits']['hits'][0]['_source']['adId']),
                "asin": data['key'],
                "impressions": str(int(data['impressions']['value'])),
                "orders": str(int(data['orders']['value'])),
                "sales": str("%.2f" % data['sales']['value']),
                "spend": str("%.2f" % data['spend']['value']),
                "clicks": str(int(data['clicks']['value'])),
                'acos': str("%.2f" % (data['spend']['value'] /
                                      data['sales']['value'] * 100))
                if data['sales']['value'] != 0 else "0.00",
                'cpc': str("%.2f" % (data['spend']['value'] /
                                     data['clicks']['value'] * 100))
                if data['clicks']['value'] != 0 else "0.00",
                'ctr': str("%.2f" % (data['clicks']['value'] /
                                     data['impressions']['value'] * 100))
                if data['impressions']['value'] != 0 else "0.00",
                'conversionRate': str("%.2f" % (data['orders']['value'] /
                                                data['clicks']['value'] * 100))
                if data['clicks']['value'] != 0 else "0.00"
            }
                for data in es_data['aggregations']['asin']['buckets']
            }

        # 根据asin从seller_product_info中查找对应的图片url，描述信息
        asin_dic = {hbase_data_dic[ad_id]['asin']: ad_id for ad_id in ad_id_list}
        body = {
                "query": {
                    "bool": {
                        "filter": [{
                            "terms": {
                                "product_current_asin": list(asin_dic.keys())
                            }
                        }]
                    }
                },
                "_source": {
                    "includes": ["product_current_asin",
                                 "product_description", "product_image"]
                }
            }
        es_data_asin = EsHelper().search(
            index='selection_amazon_seller_product_info', body=body)
        # 默认es_asin_dic的描述信息和图片url都为空
        es_asin_dic = {ad_id: {"productDescription": "", "productImage": ""}
                       for ad_id in ad_id_list}
        # 如果查找出部分asin信息，则替换asin信息字典
        if es_data_asin['hits']['hits']:
            for data in es_data_asin['hits']['hits']:
                es_asin_dic[asin_dic[
                    data['_source']['product_current_asin']]] = {
                    "productDescription": data['_source']['product_description'],
                    "productImage": data['_source']['product_image']}

        data_list = []
        # 以hbase的结果为主，遍历hbase的keys进行合并
        for ad_id in hbase_data_dic.keys():
            merged = dict(hbase_data_dic[ad_id],
                          **(es_data_dic[ad_id] if ad_id in es_data_dic else
                             {'impressions': '0', 'orders': '0',
                              'sales': '0.00', 'spend': '0.00', 'clicks': '0',
                              'acos': '0.00', 'cpc': '0.00', 'ctr': '0.00',
                              'conversionRate': '0.00'}))
            merged = dict(merged, **es_asin_dic[ad_id])
            data_list.append(merged)
        if download == "true":
            # Create the HttpResponse object with the appropriate CSV header.
            file_name = "%s-asinList.csv" % datetime.datetime.now()
            response = downloadData(data_list, file_name, ['adId', 'adGroupId'])
            return response

        order_type = True if order_type == 'desc' else False
        data_list.sort(key=lambda val: float(
            val['%s' % order_by]), reverse=order_type)
        return total, data_list

    @staticmethod
    def getKeywordList(
            profile_id, ad_group_id, targeting_type, min_date, max_date,
            size, current, order_by, order_type, filter_condition, download):
        if targeting_type == 'manual':
            fields = ['adGroupId', 'keywordId', 'matchType', 'bid', 'state']
            """根据ad_group_id从hbase的adGroup实体查出该条的默认竞价，
            如果hbase的Keyword实体没有bid字段，
            则Keyword的默认竞价为adGroup实体查出该条的默认竞价"""
            rowkey = AdGroup().generate_rowkey(profile_id, ad_group_id)
            defaultBid = AdGroup().get(rowkey, fields=["defaultBid"])
            ad_group_default_bid = \
                defaultBid['defaultBid'] if defaultBid else "0.00"
            if filter_condition:
                body = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [{
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "profile": {
                                                "query": profile_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "adGroupId": {
                                                "query": ad_group_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "range": {
                                    "date": {
                                        "gte": min_date,
                                        "lte": max_date
                                    }
                                }
                            }]
                        }
                    },
                    "aggs": {
                        "keywordId": {
                            "terms": {
                                "field": "keywordId",
                                "size": 2147483647
                            },
                            "aggs": {
                                "spend": {
                                    "sum": {
                                        "field": "cost"
                                    }
                                },
                                "sales": {
                                    "sum": {
                                        "field": "attributedSales7d"
                                    }
                                },
                                "orders": {
                                    "sum": {
                                        "field": "attributedConversions7d"
                                    }
                                },
                                "clicks": {
                                    "sum": {
                                        "field": "clicks"
                                    }
                                },
                                "impressions": {
                                    "sum": {
                                        "field": "impressions"
                                    }
                                },
                                "top": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
                es_data = EsHelper().search(
                    index='selection_amazonads_report_keyword', body=body)
                es_data_list = list(map(format_report, [{
                    "keywordId": data['key'],
                    "keywordText":
                        data['top']['hits']['hits'][0]['_source']['keywordText'],
                    "impressions": data['impressions']['value'],
                    "orders": data['orders']['value'],
                    "sales": data['sales']['value'],
                    "spend": data['spend']['value'],
                    "clicks": data['clicks']['value'],
                    'acos': data['spend']['value'] /
                            data['sales']['value'] * 100
                    if data['sales']['value'] != 0 else 0.00,
                    'cpc': data['spend']['value'] /
                           data['clicks']['value'] * 100
                    if data['clicks']['value'] != 0 else 0.00,
                    'ctr': data['clicks']['value'] /
                           data['impressions']['value'] * 100
                    if data['impressions']['value'] != 0 else 0.00,
                    'conversionRate': data['orders']['value'] /
                                      data['clicks']['value'] * 100
                    if data['clicks']['value'] != 0 else 0.00
                }
                    for data in es_data['aggregations']['keywordId']['buckets']
                ]))
                es_data_dic = filterData(
                    filter_condition, es_data_list, 'keywordId')
                keyword_id_list = list(es_data_dic.keys())
                total = len(list(es_data_dic.keys()))
                if download != "true":
                    '''先计算总数，再分页，再hbase查询'''
                    es_data_dic = {key: es_data_dic[key] for key in list(
                        es_data_dic.keys())[size * (current - 1):size * current]}
                    keyword_id_list = list(es_data_dic.keys())

                rows = []
                for keyword_id in keyword_id_list:
                    rowkey = KeyWord().generate_rowkey(profile_id, keyword_id)
                    rows.append(rowkey)
                columns = KeyWord().format_cloumn(fields)
                hbase_data_dic = KeyWord().rows_to_dict(
                    field_name='keywordId', rows=rows, columns=columns)

            elif not filter_condition:
                row_prefix = str(profile_id[::-1]).encode('utf-8')
                columns = KeyWord().format_cloumn(fields)
                # 根据profile前缀和adGroupId进行过滤查找keyword
                filter_str = "SingleColumnValueFilter(\
                'keyword','adGroupId',=,'substring:%s')" % ad_group_id
                hbase_data_dic = KeyWord().scan_to_dict(
                    field_name='keywordId', row_prefix=row_prefix,
                    columns=columns, filter=filter_str)
                keyword_id_list = list(hbase_data_dic.keys())
                total = len(list(hbase_data_dic.keys()))
                if download != "true":
                    """先统计总数，再分页，再进行es查询"""
                    hbase_data_dic = {key: hbase_data_dic[key] for key in
                                      list(hbase_data_dic.keys())
                                      [size * (current - 1):size * current]}
                    keyword_id_list = list(hbase_data_dic.keys())

                body = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [{
                                "terms": {
                                    "keywordId": keyword_id_list
                                }
                            }, {
                                "range": {
                                    "date": {
                                        "gte": min_date,
                                        "lte": max_date
                                    }
                                }
                            }]
                        }
                    },
                    "aggs": {
                        "keywordId": {
                            "terms": {
                                "field": "keywordId",
                                "size": 2147483647
                            },
                            "aggs": {
                                "spend": {
                                    "sum": {
                                        "field": "cost"
                                    }
                                },
                                "sales": {
                                    "sum": {
                                        "field": "attributedSales7d"
                                    }
                                },
                                "orders": {
                                    "sum": {
                                        "field": "attributedConversions7d"
                                    }
                                },
                                "clicks": {
                                    "sum": {
                                        "field": "clicks"
                                    }
                                },
                                "impressions": {
                                    "sum": {
                                        "field": "impressions"
                                    }
                                },
                                "top": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
                es_data = EsHelper().search(
                    index='selection_amazonads_report_keyword', body=body)
                es_data_dic = {data['key']: {
                    "keywordId": data['key'],
                    "keywordText":
                        data['top']['hits']['hits'][0]['_source']['keywordText'],
                    "impressions": str(int(data['impressions']['value'])),
                    "orders": str(int(data['orders']['value'])),
                    "sales": str("%.2f" % data['sales']['value']),
                    "spend": str("%.2f" % data['spend']['value']),
                    "clicks": str(int(data['clicks']['value'])),
                    'acos': str("%.2f" % (data['spend']['value'] /
                                          data['sales']['value'] * 100))
                    if data['sales']['value'] != 0 else "0.00",
                    'cpc': str("%.2f" % (data['spend']['value'] /
                                         data['clicks']['value'] * 100))
                    if data['clicks']['value'] != 0 else "0.00",
                    'ctr': str("%.2f" % (data['clicks']['value'] /
                                         data['impressions']['value'] * 100))
                    if data['impressions']['value'] != 0 else "0.00",
                    'conversionRate': str("%.2f" % (
                            data['orders']['value'] /
                            data['clicks']['value'] * 100))
                    if data['clicks']['value'] != 0 else "0.00"
                }
                    for data in es_data['aggregations']['keywordId']['buckets']
                }

            # 从hbase的KeyWordBidRec表中查询keyword的建议竞价
            rows = []
            for keyword_id in keyword_id_list:
                rowkey = KeyWordBidRec().generate_rowkey(profile_id, keyword_id)
                rows.append(rowkey)
            bid_rec_columns = KeyWordBidRec().format_cloumn(
                ['keywordId', 'rangeEnd', 'rangeStart', 'suggested'])
            keyword_bid_rec_dic = KeyWordBidRec().rows_to_dict(
                field_name='keywordId', rows=rows, columns=bid_rec_columns)

            data_list = []
            # 以hbase的结果为主，遍历hbase的keys进行合并
            for keyword_id in hbase_data_dic.keys():
                merged = dict(hbase_data_dic[keyword_id],
                              **(es_data_dic[keyword_id] if
                                 keyword_id in es_data_dic else
                                 {'impressions': '0', 'orders': '0',
                                  'sales': '0.00', 'spend': '0.00',
                                  'clicks': '0', 'acos': '0.00',
                                  'cpc': '0.00', 'ctr': '0.00',
                                  'conversionRate': '0.00'}))
                merged = dict(merged, **keyword_bid_rec_dic[keyword_id] if
                            keyword_id in keyword_bid_rec_dic else
                            {"suggested": "", "rangeStart": "", "rangeEnd": ""})
                # 判断是否有bid字段，如果没有就使用ad_group_default_bid作为默认竞价
                merged['defaultBid'] = ad_group_default_bid
                if 'bid' in merged:
                    merged['defaultBid'] = merged.pop('bid')
                data_list.append(merged)
            if download == "true":
                # Create the HttpResponse object with the appropriate CSV header
                file_name = "%s-keywordList.csv" % datetime.datetime.now()
                response = downloadData(
                    data_list, file_name, ['keywordId', 'adGroupId'])
                return response

            order_type = True if order_type == 'desc' else False
            if order_by == "suggested":
                pass
            else:
                data_list.sort(key=lambda val: float(
                    val['%s' % order_by]), reverse=order_type)
            return total, data_list

        elif targeting_type == 'auto':
            body = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [{
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "profile": {
                                                "query": profile_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "bool": {
                                    "must": [{
                                        "match_phrase": {
                                            "adGroupId": {
                                                "query": ad_group_id
                                            }
                                        }
                                    }]
                                }
                            }, {
                                "range": {
                                    "date": {
                                        "gte": min_date,
                                        "lte": max_date
                                    }
                                }
                            }]
                        }
                    },
                    "aggs": {
                        "keywordId": {
                            "terms": {
                                "field": "keywordId",
                                "size": 2147483647
                            },
                            "aggs": {
                                "spend": {
                                    "sum": {
                                        "field": "cost"
                                    }
                                },
                                "sales": {
                                    "sum": {
                                        "field": "attributedSales7d"
                                    }
                                },
                                "orders": {
                                    "sum": {
                                        "field": "attributedConversions7d"
                                    }
                                },
                                "clicks": {
                                    "sum": {
                                        "field": "clicks"
                                    }
                                },
                                "impressions": {
                                    "sum": {
                                        "field": "impressions"
                                    }
                                },
                                "top": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
            es_data = EsHelper().search(
                index='selection_amazonads_report_keyword', body=body)
            data_list = list(map(format_report, [{
                "keywordId": data['key'],
                "keywordText":
                    data['top']['hits']['hits'][0]['_source']['keywordText'],
                "matchType":
                    data['top']['hits']['hits'][0]['_source']['matchType'].lower(),
                "impressions": data['impressions']['value'],
                "orders": data['orders']['value'],
                "sales": data['sales']['value'],
                "spend": data['spend']['value'],
                "clicks": data['clicks']['value'],
                'acos': data['spend']['value'] / data['sales']['value'] * 100
                if data['sales']['value'] != 0 else 0.00,
                'cpc': data['spend']['value'] / data['clicks']['value'] * 100
                if data['clicks']['value'] != 0 else 0.00,
                'ctr': data['clicks']['value'] /
                       data['impressions']['value'] * 100
                if data['impressions']['value'] != 0 else 0.00,
                'conversionRate': data['orders']['value'] /
                                  data['clicks']['value'] * 100
                if data['clicks']['value'] != 0 else 0.00
            }
                for data in es_data['aggregations']['keywordId']['buckets']
            ]))
            if filter_condition:
                for k, v in filter_condition.items():
                    data_list = [
                        data for data in data_list if float(data[k]) >= v
                    ]
            if download == "true":
                # Create the HttpResponse object with the appropriate CSV header
                file_name = "%s-keywordList.csv" % datetime.datetime.now()
                response = downloadData(data_list, file_name, ['keywordId'])
                return response
            order_type = True if order_type == 'desc' else False
            data_list.sort(key=lambda val: float(
                val['%s' % order_by]), reverse=order_type)
            return len(data_list), data_list[size * (current - 1):size * current]

    @staticmethod
    def getSearchTermList(profile_id, ad_group_id, min_date, max_date,
                          size, current, order_by, order_type,
                          filter_condition, download):
        body = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [{
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "profile": {
                                            "query": profile_id
                                        }
                                    }
                                }]
                            }
                        }, {
                            "bool": {
                                "must": [{
                                    "match_phrase": {
                                        "adGroupId": {
                                            "query": ad_group_id
                                        }
                                    }
                                }]
                            }
                        }, {
                            "range": {
                                "date": {
                                    "gte": min_date,
                                    "lte": max_date
                                }
                            }
                        }]
                    }
                },
                "aggs": {
                    "query": {
                        "terms": {
                            "field": "query",
                            "size": 2147483647
                        },
                        "aggs": {
                            "spend": {
                                "sum": {
                                    "field": "cost"
                                }
                            },
                            "sales": {
                                "sum": {
                                    "field": "attributedSales7d"
                                }
                            },
                            "orders": {
                                "sum": {
                                    "field": "attributedConversions7d"
                                }
                            },
                            "clicks": {
                                "sum": {
                                    "field": "clicks"
                                }
                            },
                            "impressions": {
                                "sum": {
                                    "field": "impressions"
                                }
                            },
                            "top": {
                                "top_hits": {
                                    "size": 1
                                }
                            }
                        }
                    }
                }
            }
        es_data = EsHelper().search(
            index='selection_amazonads_report_keyword_query', body=body)
        data_list = list(map(format_report, [{
            "query": data['key'],
            "keywordText":
                data['top']['hits']['hits'][0]['_source']['keywordText'],
            "matchType":
                data['top']['hits']['hits'][0]['_source']['matchType'].lower(),
            "impressions": data['impressions']['value'],
            "orders": data['orders']['value'],
            "sales": data['sales']['value'],
            "spend": data['spend']['value'],
            "clicks": data['clicks']['value'],
            'acos': data['spend']['value'] / data['sales']['value'] * 100
            if data['sales']['value'] != 0 else 0.00,
            'cpc': data['spend']['value'] / data['clicks']['value'] * 100
            if data['clicks']['value'] != 0 else 0.00,
            'ctr': data['clicks']['value'] / data['impressions']['value'] * 100
            if data['impressions']['value'] != 0 else 0.00,
            'conversionRate': data['orders']['value'] /
                              data['clicks']['value'] * 100
            if data['clicks']['value'] != 0 else 0.00
        }
            for data in es_data['aggregations']['query']['buckets']
        ]))
        if filter_condition:
            for k, v in filter_condition.items():
                data_list = [
                    data for data in data_list if float(data[k]) >= v
                ]
        if download == "true":
            # Create the HttpResponse object with the appropriate CSV header.
            file_name = "%s-searchTermList.csv" % datetime.datetime.now()
            response = downloadData(data_list, file_name)
            return response
        order_type = True if order_type == 'desc' else False
        data_list.sort(
            key=lambda val: float(val['%s' % order_by]), reverse=order_type)
        return len(data_list), data_list[size * (current - 1):size * current]

    @staticmethod
    def hasCampaignPerfRecord(profile_id):
        body = {
            "query": {
                "bool": {
                    "filter": [{
                        "bool": {
                            "must": [{
                                "match_phrase": {
                                    "profile": {
                                        "query": profile_id
                                    }
                                }
                            }]
                        }
                    }]
                }
            },
            "_source": {
                "includes": ["campaignId"]
            }
        }
        res = EsHelper().search(
            index='selection_amazonads_report_campaign', body=body)
        if res['hits']['total'] == 0:
            return False
        return True


def getEveryDayData(begin_date, end_date, data_list):
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    data_time = []
    for data in data_list:
        data_time.append(data["date"])
    for time in date_list:
        default = {
            "spend": "",
            "sales": "",
            "orders": "",
            "clicks": "",
            "impressions": "",
            "acos": "",
            "cpc": "",
            "ctr": "",
            "conversionRate": "",
        }
        if time not in data_time:
            default['date'] = time
            data_list.append(default)
    return data_list


def filterData(filter_condition, data_list, key):
    for k, v in filter_condition.items():
        data_list = [
            data for data in data_list if float(data[k]) >= v
        ]
    data_dic = {data[key]: data for data in data_list}
    return data_dic


def downloadData(data_list, file_name, key=None):
    """
    :param data_list: data_list
    :param file_name: file_name
    :param key: csv文件删除ID字段内容，防止乱码
    :return: HttpResponse(content_type='text/csv')
    """
    if not data_list:
        return HttpResponse('')
    # Create the HttpResponse object with the appropriate CSV header.
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename=%s' % file_name
    response.write(codecs.BOM_UTF8)
    writer = csv.writer(response)
    if key:
        [[data.pop(k) for data in data_list] for k in key]
    writer.writerow(data_list[0])
    for data in data_list:
        writer.writerows([data.values()])
    return response
