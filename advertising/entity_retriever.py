from .hbase_models import CampaignNegativeKeyword, AdgroupNegativeKeyword


class EntityRetriever:

    @staticmethod
    def get_campaign_negative_keyword_list(profile_id, campaign_id):
        """
        get campaign negative keywords from hbase
        """
        return CampaignNegativeKeyword().get_cp_negative_kw_all(
                profile_id, campaign_id)

    @staticmethod
    def get_adgroup_negative_keyword_list(profile_id, adgroup_id):
        """
        get adgroup negative keyword from hbase.
        """
        return AdgroupNegativeKeyword().get_adgroup_negative_kw_all(
                profile_id, adgroup_id)
