import logging
from amazonads.celery import app
from api.api_manager import APIManager
from advertising.models import CustomerSeller, SellerProfile
from advertising.hbase_models import AdGroup, KeyWords, AdGroupBidRec, \
    KeywordBidRec
from advertising.actions import Actions

logger = logging.getLogger('tasks')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    pass
    # sender.add_periodic_task(7200.0, getKeywordAggPerf.s(today, today, 'today'), name='keyword_today')


@app.task
def retrieve_profiles():
    sellers = CustomerSeller.objects.all()
    for seller in sellers:
        APIManager(seller_uuid=seller.seller_uuid).retrieve_profile_dict()
        logger.info('celery retrieve_profiles of customer %s' % seller.customer_id)
    return 'ok'


@app.task
def retrieve_adgroup_bidrec_all():
    seller_profiles = SellerProfile.objects.all()
    for seller_profile in seller_profiles:
        retrieve_adgroup_bidrec.apply_async((seller_profile.profile_id,))


@app.task
def retrieve_kw_bidrec_all():
    seller_profiles = SellerProfile.objects.all()
    for seller_profile in seller_profiles:
        retrieve_kw_bidrec.apply_async((seller_profile.profile_id,))


@app.task
def retrieve_adgroup_bidrec(profile_id):
    # step1:get all adgroup_ids we need
    adgroupid_all_list = AdGroup().get_ad_group_all(profile_id)
    # step2:get exists adgroup_ids
    adgroupid_exists_id_list = AdGroupBidRec().get_ad_group_bidrec_all(
            profile_id)
    # step3:get adgroup_ids that we need to retrieve from amazon
    adgroupid_amazon_list = list(set(adgroupid_all_list).difference(
        set(adgroupid_exists_id_list)))
    # step4: get data one by one and save to hbase
    for adgroup_id in adgroupid_amazon_list:
        Actions().retrieve_adgroup_bidrec(profile_id, adgroup_id)
    logger.info('celery retrieve_adgroup_bidrec of profile_id %s, '
                'renewal data - %s ,nums ' % (str(profile_id),
                                              str(len(adgroupid_amazon_list)))
                )
    return 'ok'


@app.task
def retrieve_kw_bidrec(profile_id):
    # step1:从hbase实体信息表获取所有的profile_id下的关键词信息
    keyword_all_list = KeyWords().get_keyword_id_all(profile_id)
    # step2:从hbase读取已经记录过竞价信息的所有关键词信息
    keyword_exists_list = KeywordBidRec().get_raw_keyword_bidrec_all(
        profile_id)
    # step3:取差集得到新的需要去亚马逊获取数据的关键词信息
    # 最终需要获取[(kw_text,match_type),(kw_text,match_type)……]
    keyword_amazon_list = list(set(keyword_all_list).difference(
        set(keyword_exists_list)))
    # 转换需要从亚马逊获取数据的keyword_amazon_list格式
    keyword_amazon_format_list = get_keyword_batch(keyword_amazon_list)
    for keyword_amazon_format in keyword_amazon_format_list:
        adgroup_id = keyword_amazon_format['adgroup_id']
        keyword_matchtype_tuples = \
            keyword_amazon_format['keyword_matchtype_tuples']
        # 将keyword按照每批次100个进行处理
        for i in range(len(keyword_matchtype_tuples) // 100 + 1):
            keyword_matchtype_tuples_split = keyword_matchtype_tuples[
                                        0 + i * 100:100 + i * 100]
            Actions().retrieve_raw_keyword_bidrec(
                profile_id, adgroup_id, keyword_matchtype_tuples_split)
    logger.info('celery retrieve_keyword_bidrec of profile_id %s, '
                'renewal data: %s nums ' %
                (str(profile_id), str(len(keyword_amazon_list)))
                )
    return 'ok'


def get_keyword_batch(keyword_amazon_list):
    # keyword_amazon_list 格式转换
    # 取出所有的adgroup_id去重
    adgroup_id_set = set(
        adgroup_id for keyword, match_type, adgroup_id in keyword_amazon_list
    )
    # 将所有的关键词和匹配规则信息按照adgroup_id区分
    return [{
            "adgroup_id": adgroup_id,
            "keyword_matchtype_tuples":
                get_keyword_adgroup_batch(adgroup_id, keyword_amazon_list)
            } for adgroup_id in adgroup_id_set]


def get_keyword_adgroup_batch(adgroup_id, keyword_amazon_list):
    res = []
    for keyword, match_type, adgroupId in keyword_amazon_list:
        if adgroup_id == adgroupId:
            res.append((keyword, match_type))
    return res

    # return [(keyword, match_type) if adgroup_id == adgroupId else None
    #         for keyword, match_type, adgroupId in keyword_amazon_list]
