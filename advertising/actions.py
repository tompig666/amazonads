from .models import SellerProfile
from .hbase_models import CampaignNegativeKeyword, AdgroupNegativeKeyword
from api.api_manager import APIManager


class Actions:

    def __init__(self, retry=False):
        self.__retry = retry

    def add_campaign_negative_keywords(self, profile_id, data):
        seller_profile = SellerProfile.objects.get(profile_id=profile_id)
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        result, keyword_ids = [], []
        # add campaigns.
        for is_success, data in api_manager.operate_entity(
                profile_id, 'campaignNegativeKeywords', False, data):
            if is_success:
                keyword_ids.append(data)
            result.append({is_success: data})
        api_manager.retrieve_entity(profile_id, 'campaignNegativeKeywords',
                                    keyword_id_filter=keyword_ids)
        return result

    def delete_campaign_negative_keywords(self, profile_id, data):
        """
        About data format:
        [{keyword_id: 123,}]
        """
        seller_profile = SellerProfile.objects.get(profile_id=profile_id)
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        delete_data = [{**item, **{'state': 'deleted'}} for item in data]
        result = []
        for is_success, data in api_manager.operate_entity(
                profile_id, 'campaignNegativeKeywords', True, delete_data):
            if is_success:
                CampaignNegativeKeyword().put(
                    CampaignNegativeKeyword.generate_rowkey(profile_id, data),
                    {'state': 'deleted'}
                )
            result.append({is_success: data})
        return result

    def add_adgroup_negative_keywords(self, profile_id, data):
        seller_profile = SellerProfile.objects.get(profile_id=profile_id)
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        result, keyword_ids = [], []

        # add campaigns.
        for is_success, data in api_manager.operate_entity(
                profile_id, 'negativeKeywords', False, data):
            if is_success:
                keyword_ids.append(data)
            result.append({is_success: data})
        api_manager.retrieve_entity(profile_id, 'negativeKeywords',
                                    keyword_id_filter=keyword_ids)
        return result

    def delete_adgroup_negative_keywords(self, profile_id, data):
        """
        About data format:
        [{keyword_id: 123,}]
        """
        seller_profile = SellerProfile.objects.get(profile_id=profile_id)
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        delete_data = [{**item, **{'state': 'archived'}} for item in data]
        result = []
        for is_success, data in api_manager.operate_entity(
                profile_id, 'negativeKeywords', True, delete_data):
            if is_success:
                AdgroupNegativeKeyword().put(
                    AdgroupNegativeKeyword.generate_rowkey(profile_id, data),
                    {'state': 'deleted'}
                )
            result.append({is_success: data})
        return result

    def archive_campaign_negative_keyword(self, profile_id, keyword_id):
        seller_profile = SellerProfile.objects.get(profile_id=profile_id)
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        api_manager.archive_single_entity(profile_id,
                                          'campaignNegativeKeywords',
                                          keyword_id)
        APIManager.retrieve_entity(profile_id,
                                   'campaignNegativeKeywords',
                                    keyword_id_filter=keyword_id
                                )
        return True

    def update_campaign(self, profile_id, data):
        seller_profile = SellerProfile.objects \
            .filter(profile_id=profile_id).first()
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        result, campaign_ids = [], []
        for is_success, data in api_manager.operate_entity(
                profile_id, 'campaigns', True, data):
            if is_success:
                campaign_ids.append(data)
            result.append({is_success: data})
        api_manager.retrieve_entity(profile_id, 'campaigns',
                                    campaign_id_filter=campaign_ids)
        return result

    def add_campaign(self, profile_id, data):
        seller_profile = SellerProfile.objects \
            .filter(profile_id=profile_id).first()
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        result, campaign_ids = [], []
        for is_success, data in api_manager.operate_entity(
                profile_id, 'campaigns', False, data):
            if is_success:
                campaign_ids.append(data)
            result.append({is_success: data})
        api_manager.retrieve_entity(profile_id, 'campaigns',
                                    campaign_id_filter=campaign_ids)
        return result

    def update_adgroup(self, profile_id, data):
        seller_profile = SellerProfile.objects \
            .filter(profile_id=profile_id).first()
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        result, adgroup_ids = [], []
        for is_success, data in api_manager.operate_entity(
                profile_id, 'adGroups', True, data):
            if is_success:
                adgroup_ids.append(data)
            result.append({is_success: data})
        api_manager.retrieve_entity(profile_id, 'adGroups',
                                    adgroup_id_filter=adgroup_ids)
        return result

    def create_adgroup(self, profile_id, data):
        seller_profile = SellerProfile.objects \
            .filter(profile_id=profile_id).first()
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        result, adgroup_ids = [], []
        for is_success, data in api_manager.operate_entity(
                profile_id, 'adGroups', False, data):
            if is_success:
                adgroup_ids.append(data)
            result.append({is_success: data})
        api_manager.retrieve_entity(profile_id, 'adGroups',
                                    adgroup_id_filter=adgroup_ids)
        return result

    def retrieve_adgroup_bidrec(self, profile_id, adgroup_id):
        seller_profile = SellerProfile.objects\
            .filter(profile_id=profile_id).first()
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        return api_manager.retrieve_adgroup_bidrec(profile_id, adgroup_id)

    def retrieve_keyword_bidrec(self, profile_id, keyword_id):
        seller_profile = SellerProfile.objects\
            .filter(profile_id=profile_id).first()
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        return api_manager.retrieve_keyword_bidrec(profile_id, keyword_id)

    def retrieve_raw_keyword_bidrec(self, profile_id,
                                    adgroup_id, raw_keyword_col):
        seller_profile = SellerProfile.objects. \
            filter(profile_id=profile_id).first()
        api_manager = APIManager(seller_profile.seller_id, self.__retry)
        return api_manager.retrieve_raw_keyword_bidrec(
            profile_id, adgroup_id, raw_keyword_col)
