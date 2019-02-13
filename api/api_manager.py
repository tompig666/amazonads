import datetime
import logging
import uuid
from datetime import timedelta
from time import sleep
from rest_framework.exceptions import APIException
from api.api_client import APIClient
from requests.exceptions import RequestException, HTTPError
from advertising.models import CustomerSeller, SellerProfile, SellerAuth
from advertising.hbase_models import Campaign, AdGroup, ProductAds, \
    CampaignNegativeKeyword, AdgroupNegativeKeyword, KeyWord, AdGroupBidRec, \
    KeyWordBidRec


logger = logging.getLogger('amazonads')


class APIManager:
    """
    Managing full Amazon ads API functions with cached seller UUID.

    All methods are driven by databse items and output into databse.
    Some functions of APIClient are ignored as follows:
    retrieve_single_profile -- Less efficient than retrieve_profile.
    update_profile -- Not expected by seller.
    create_profile -- Only for sandbox.
    retrieve_single_entity -- Less efficient than retrieve_entity.
    archive_single_entity -- Dangerous and less efficient than update_entity.
    retrieve_keyword_bidrec -- Less efficient than retrieve_raw_keyword_bidrec.
    retrieve_single_asin_sugkey, retrieve_asin_sugkey -- Less efficient than
    retrieve_adgroup_sugkey in initialization.
    """
    _RETRY_COUNT = 3
    _RETRY_SECONDS = 20
    _auth_expiration_seconds = 3600
    _entity_retrieval_count = 5000

    def __init__(self, seller_uuid, retry=False):
        """Init API client."""
        self.__seller_uuid = seller_uuid
        seller_auth = SellerAuth.objects.get(seller=seller_uuid)
        profile_id_dict = dict(
            seller_auth.seller.sellerprofile_set
            .values_list('profile_id', 'country_code',)
        )
        self.__client = APIClient(seller_auth.access_token,
                                  seller_auth.refresh_token,
                                  profile_id_dict)
        self.__retry_count = self._RETRY_COUNT if retry else 0
        self.__retry_seconds = self._RETRY_SECONDS if retry else 0

    def refresh_auth(self):
        """
        Force refresh auth tokens.

        Race Condition -- Single atomic call thus atomic.
        """
        access_token, refresh_token = self.__client.refresh_auth()
        defaults_dict = {'access_token': access_token or '',
                         'refresh_token': refresh_token or ''}
        seconds = self._auth_expiration_seconds
        if access_token:
            defaults_dict['expire_after'] = \
                datetime.datetime.now() + timedelta(seconds=seconds)
        SellerAuth.objects.update_or_create(
            seller=self.__seller_uuid,
            defaults=defaults_dict,
        )
        return access_token, refresh_token

    def retrieve_profile_dict(self):
        """Retrieve profiles, update cached profile dict."""
        profile_dict = self.__call(
            self.__client.retrieve_profile_dict)
        for country, profile in profile_dict.items():
            model, created = SellerProfile.objects.update_or_create(
                profile_id=profile['profile_id'],
                seller=self.__seller_uuid,
                defaults={
                    "country_code": profile['country'],
                    "currency_code": profile['currency'],
                    "daily_budget": profile['daily_budget'],
                    "timezone": profile['timezone'],
                    "marketplace_string_id": profile['marketplace_str_id'],
                    "amazon_account_id": profile['seller_str_id'],
                    "amazon_account_type": profile['account_type'],
                }
            )
            if created:
                model.status = 'new'
                model.save()
        return True

    def operate_entity(self, profile_id, record_type, is_update,
                       entity_col=None):
        """
        Create entities on Amazon, return dict of executed entity fields.
        Record operation historys.

        More about arguments:
        profile_id -- seller profile id.
        record_type -- One of 'campaigns'/'adGroups'/'productAds'/'keywords'/
        'negativeKeywords'/'campaignNegativeKeywords'.
        is_update -- Otherwise creation.
        """
        # step 1 call api to create/update entity
        # create entitys
        results = self.__call(self.__client.update_entity if is_update
                              else self.__client.create_entity, profile_id,
                              record_type, entity_col)

        return results
        # TODO: step 2 record operations to entity_operations_history.

    def retrieve_entity(self, profile_id, record_type, campaign_id_filter=None,
                        adgroup_id_filter=None, keyword_id_filter=None,
                        only_existing=False):
        """Retrieve entities filtered by campaign ID.

        Race Condition -- Multiple atomic entity info updates/creations.
        Also record serving status history for campaigns, adgroups, productads.
        More about arguments:
        country -- In 2-char ISO-3166-1 code.
        record_type -- One of 'campaigns'/'adGroups'/'productAds'/'keywords'/
        'negativeKeywords'/'campaignNegativeKeywords'.
        campaign_id_filter -- Filter by corresponding campaign ID, None for
        no filtering.
        only_existing -- Ignore archived entities if set true.
        Note that campaigns/adGroups/productAds are retrieved with extended
        fields, and keywords/negativeKeywords/campaignNegativeKeywords not.
        """
        extended = record_type in ('campaigns', 'adGroups', 'productAds')
        accumulated_count = 0
        retrieved_count = 0
        count = self._entity_retrieval_count
        first_run = True
        while first_run or retrieved_count == count:
            entity_list = self.__call(
                self.__client.retrieve_entity,
                profile_id,
                record_type,
                extended=extended,
                start_index=accumulated_count,
                count=count,
                campaign_id_filter=campaign_id_filter,
                adgroup_id_filter=adgroup_id_filter,
                keyword_id_filter=keyword_id_filter,
                state_filter=('enabled', 'paused') if only_existing else None, \
            ) or []
            self.__save_retrieved_entity(profile_id, record_type, entity_list)
            retrieved_count = len(entity_list)
            accumulated_count += retrieved_count
            first_run = False

    @classmethod
    def __save_retrieved_entity(cls, profile_id, record_type, entity_list):
        model, key_field = cls.__map_entity_retrieval(record_type)
        for entity in entity_list:
            row_key = model.generate_rowkey(profile_id, entity[key_field])
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            model().put(row_key, {**entity,
                                  **{'profile': profile_id,
                                     'updatedAt': now}})

    @classmethod
    def __map_entity_retrieval(cls, record_type):
        return {
            'campaigns': (Campaign, 'campaignId'),
            'adGroups': (AdGroup, 'adGroupId'),
            'productAds': (ProductAds, 'adId'),
            'keywords': (KeyWord, 'keywordId'),
            'negativeKeywords': (AdgroupNegativeKeyword, 'keywordId'),
            'campaignNegativeKeywords': (CampaignNegativeKeyword,
                                         'keywordId'),
        }[record_type]

    def archive_single_entity(self, profile_id, record_type, entity_id):
        result = self.__class__(
                self.__client.archive_single_entity,
                profile_id,
                record_type,
                entity_id
            )
        return result

    def retrieve_adgroup_bidrec(self, profile_id, adgroup_id):
        bid_rec_dict = self.__call(
            self.__client.retrieve_adgroup_bidrec,
            profile_id,
            adgroup_id)
        rowkey = AdGroupBidRec.generate_rowkey(profile_id, adgroup_id)
        # save bid_rec_dict record into hbase
        if bid_rec_dict:
            AdGroupBidRec().put(rowkey,
                                {**bid_rec_dict,
                                 **{
                                    'profile': profile_id,
                                    'adGroupId': adgroup_id
                                 }})

    def retrieve_keyword_bidrec(self, profile_id, keyword_id):
        bid_rec_dict = self.__call(
            self.__client.retrieve_keyword_bidrec,
            profile_id,
            keyword_id)
        rowkey = KeyWordBidRec.generate_rowkey(profile_id, keyword_id)
        # save bid_rec_dict record into hbase
        if bid_rec_dict:
            KeyWordBidRec().put(rowkey,
                                {**bid_rec_dict,
                                 **{
                                     'profile': profile_id,
                                     'keywordId': keyword_id
                                 }})
        return bid_rec_dict

    def retrieve_raw_keyword_bidrec(self, profile_id, adgroup_id,
                                    raw_keyword_col):
        bid_rec_dict = self.__call(
            self.__client.retrieve_raw_keyword_bidrec, profile_id,
            adgroup_id, raw_keyword_col)
        # save bid_rec_dict record into hbase
        if bid_rec_dict:
            batch_data = KeyWordBidRec().format_kw_bidrec_batch(
                profile_id, bid_rec_dict)
            KeyWordBidRec().put_batch(batch_data)
        return bid_rec_dict

    # API call routine
    def __call(self, func, *args, **kwargs):
        """
        Use to call api functions.
        More about Details:
        It will refresh the access_token before calling a function,
        retry after the seconds set by global settings of the class.
        """
        for trial in range(0, 1 + self.__retry_count):
            try:
                seller_auth = SellerAuth.objects\
                    .get(seller=self.__seller_uuid)
                if seller_auth.deleted_at or not seller_auth.refresh_token:
                    return None
                self.__client.set_token(seller_auth.access_token,
                                        seller_auth.refresh_token)
                if not seller_auth.access_token or \
                        datetime.datetime.now() >= seller_auth.expire_after:
                    if not self.refresh_auth()[0]:
                        return None
                return func(*args, **kwargs)
            except RequestException as e:
                if isinstance(e, HTTPError) and e.response is not None and \
                        e.response.status_code == 401:
                    SellerAuth.objects.filter(seller=self.__seller_uuid)\
                        .update(access_token='',
                                expire_after=datetime.datetime.now())
                    return None
                elif trial < self.__retry_count:
                    error_name = e.__class__.__name__
                    source = 'preset'
                    sleeping_seconds = self._retry_seconds
                    if e.response is not None:
                        error_name += '(%s)' % e.response.status_code
                        retry_after_str = e.response.headers.get('Retry-After')
                        if retry_after_str is not None:
                            sleeping_seconds = int(retry_after_str)
                            source = 'header'
                    logger.warning(('%s in trial %s for %s,'
                                    ' retry after %s (%s) seconds.'),
                                   error_name,
                                   trial,
                                   func.__name__,
                                   sleeping_seconds,
                                   source)
                    sleep(sleeping_seconds)
                    continue
                else:
                    raise

    @staticmethod
    def assemble_authcode_url(state):
        return APIClient.assemble_authcode_url(state=state)

    @classmethod
    def auth(cls, email, code, client_id):
        seller_uuid = uuid.uuid4()
        access_token, refresh_token = APIClient.grant_auth(authcode=code)
        if not access_token:
            raise APIException('grant_auth failed')
        logger.info('%s start to retrieve_profile_dict' % client_id)
        profile_dict = APIClient(access_token, refresh_token).retrieve_profile_dict()
        logger.info('%s retrieve_profile_dict success' % client_id)
        sellers = CustomerSeller.objects.filter(customer_id=client_id).all()
        # 遍历查到的profiles 去验证用户是否授权过了
        for country, profile in profile_dict.items():
            if SellerProfile.objects.filter(
                    profile_id=profile['profile_id']) \
                    .filter(seller__in=sellers):
                # 如果profileId已经存在，表示已经授权过，返回false
                return False

        CustomerSeller.objects.create(
            customer_id=client_id,
            seller_uuid=seller_uuid,
            seller_email=email
        )
        seller = CustomerSeller.objects.filter(seller_uuid=seller_uuid).first()
        SellerAuth.objects.create(
            seller=seller,
            access_token=access_token,
            refresh_token=refresh_token,
            expire_after=datetime.datetime.now() + datetime.timedelta(hours=1)
        )

        for seller in sellers:
            for country, profile in profile_dict.items():
                model, created = SellerProfile.objects.update_or_create(
                    profile_id=profile['profile_id'],
                    seller=seller,
                    defaults={
                        "country_code": profile['country'],
                        "currency_code": profile['currency'],
                        "daily_budget": profile['daily_budget'],
                        "timezone": profile['timezone'],
                        "marketplace_string_id": profile['marketplace_str_id'],
                        "amazon_account_id": profile['seller_str_id'],
                        "amazon_account_type": profile['account_type'],
                    }
                )
                if created:
                    model.status = 'new'
                    model.save()

        # 没有授权过，返回true
        return True
