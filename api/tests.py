from django.test import TestCase

# Create your tests here.

from .api_manager import APIManager

seller_uuid = '78141b93-8c7c-42c8-99a5-a33814167f08'
profile_id = '245092591347578'
adgroup_id = '236505286172641'
campaign_entitys = [{
    'campaignId': 130263187355826,
    'name': 'test',
    'targetingType': 'manual',
    'state': 'enabled',
    'dailyBudget': 5.0,
    'startDate': '20181218'
}]

client = APIManager(seller_uuid)

# test update campaign
# client.operate_entity(profile_id, 'campaigns', True, campaign_entitys)

client = APIManager(seller_uuid)

client.retrieve_adgroup_bidrec(profile_id, adgroup_id)
