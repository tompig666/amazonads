import logging
import json
from rest_framework.views import APIView
from django.shortcuts import redirect
import requests
import base64
from advertising.models import CustomerSeller
from django.conf import settings
from rest_framework.exceptions import APIException
from django.http import JsonResponse
from .operation import Operation
from api.api_manager import APIManager
from .actions import Actions
from amazonads import verify_form_data
from .entity_retriever import EntityRetriever


logger = logging.getLogger('amazonads')


def check_data(obj):
    result = {'code': 1, 'msg': None}
    if obj and not obj.is_valid():
        error_str = obj.errors.as_json()
        result['msg'] = list(json.loads(error_str).values())[0][0]['message']
        return JsonResponse(result)


class AuthUrl(APIView):
    def get(self, request):
        email = request.GET.get('name')
        if not email:
            return JsonResponse({'code': 1, 'msg': 'name can not be null'})

        authorization = request.META['HTTP_AUTHORIZATION']
        logger.info('authUrl api start to get client_id')
        client_id = get_client_id(authorization)
        logger.info('authUrl api get client_id success')
        info = email + ',' + client_id
        state = base64.b64encode(info.encode('utf-8'))
        content = {
            "code": 0,
            "data": {
                "auth_url": APIManager.assemble_authcode_url(state=state)
            }
        }
        return JsonResponse(content)


class Auth(APIView):
    def get(self, request):
        code = request.GET.get('code')
        if not code:
            return JsonResponse({'code': 1, 'msg': 'code can not be null'})
        state = request.GET.get('state')
        if not state:
            return JsonResponse({'code': 1, 'msg': 'state can not be null'})
        info = base64.b64decode(state).decode().split(',')
        email = info[0]
        client_id = info[1]
        res = APIManager.auth(email, code, client_id)
        if res:
            # status=0字段，表示没有授权过
            return redirect(settings.FRONT_AUTH_SUCCESS_URL + '?status=0')
        else:
            # status = 1 字段表示已经授权过
            return redirect(settings.FRONT_AUTH_SUCCESS_URL + '?status=1')


class Profile(APIView):
    def get(self, request):
        authorization = request.META['HTTP_AUTHORIZATION']
        logger.info('getSellerProfiles api start to get client_id')
        client_id = get_client_id(authorization)
        logger.info('getSellerProfiles api get client_id success')
        # update profile status
        Operation.update_customer_profile_status(client_id)
        content = get_profile(client_id=client_id)
        return JsonResponse(content)


class CampaignNegativeKeywords(APIView):
    def post(self, request):
        # TODO: validate form data
        profile_id = request.data.get('profileId')
        data = request.data.get('negativeKeywords')

        result = Actions().add_campaign_negative_keywords(
                    profile_id, data)
        content = {
            'code': '0',
            'msg': result
        }
        
        return JsonResponse(content)

    def put(self, request):
        # TODO: validate form data.
        profile_id = request.data.get('profileId')
        data = request.data.get('negativeKeywords')
        result = Actions().delete_campaign_negative_keywords(
                profile_id, data)
        content = {
            'code': '0',
            'msg': result
        }
        return JsonResponse(content)


# class CampaignNegativeKeywordsList(APIView):
#     def delete(self, request, keyword_id):
#         profile_id = request.GET.get('profileId')
#         is_success = Actions().archive_campaign_negative_keyword(
#             profile_id,
#             [keyword_id]
#         )
#         content = {
#             'code': '0',
#             'msg': is_success
#         }
#         return JsonResponse(content)


class CampaignNegativeKeywordsSearch(APIView):
    def post(self, request):
        profile_id = request.data.get('profileId')
        campaign_id = request.data.get('campaignId')
        size = request.data.get('size')
        current = request.data.get('current')

        # TODO: validate form data
        data = EntityRetriever.get_campaign_negative_keyword_list(
            profile_id, campaign_id)
        content = {
            'code': 0,
            'msg': {
                'total': len(data),
                'size': size,
                'current': current,
                'list': data[size * (current - 1):size * current]
            }
        }
        return JsonResponse(content)


class Campaigns(APIView):
    def put(self, request):
        profile_id = request.data.get("profileId")
        data = request.data.get("campaign_entitys")
        # validate form data
        obj = verify_form_data.OperateAdForms.check_campaign(
            request.data, is_update=True)
        result = check_data(obj)
        if result:
            return result
        # update camapign data
        result = Actions().update_campaign(profile_id, data)
        content = {
            'code': '0',
            'msg': result
        }
        return JsonResponse(content)

    def post(self, request):
        profile_id = request.data.get("profileId")
        data = request.data.get("campaign_entitys")
        # validate form data
        obj = verify_form_data.OperateAdForms.check_campaign(
            request.data, is_update=False)
        result = check_data(obj)
        if result:
            return result
        # create camapign data
        result = Actions().add_campaign(profile_id, data)
        content = {
            'code': '0',
            'msg': result
        }
        return JsonResponse(content)


class AdGroups(APIView):
    def put(self, request):
        profile_id = request.data.get("profileId")
        data = request.data.get("adgroup_entitys")
        # validate form data
        obj = verify_form_data.OperateAdForms.check_adgroup(
            request.data, is_update=True)
        result = check_data(obj)
        if result:
            return result
        # update adgroup data
        result = Actions().update_adgroup(profile_id, data)
        content = {
            'code': '0',
            'msg': result
        }
        return JsonResponse(content)

    def post(self, request):
        profile_id = request.data.get("profileId")
        data = request.data.get("adgroup_entitys")
        # validate form data
        obj = verify_form_data.OperateAdForms.check_adgroup(
            request.data, is_update=False)
        result = check_data(obj)
        if result:
            return result
        # create adgroup data
        result = Actions().create_adgroup(profile_id, data)
        content = {
            'code': '0',
            'msg': result
        }
        return JsonResponse(content)


class AdGroupNegativeKeywords(APIView):
    def post(self, request):
        # TODO: validate form data
        profile_id = request.data.get('profileId')
        data = request.data.get('negativeKeywords')
        result = Actions().add_adgroup_negative_keywords(
                    profile_id, data)
        content = {
            'code': '0',
            'msg': result
        }
        return JsonResponse(content)

    def put(self, request):
        # TODO: validate form data.
        profile_id = request.data.get('profileId')
        data = request.data.get('negativeKeywords')

        result = Actions().delete_adgroup_negative_keywords(
                profile_id, data)
        content = {
            'code': '0',
            'msg': result
        }
        return JsonResponse(content)


class AdGroupNegativeKeywordsSearch(APIView):
    def post(self, request):
        profile_id = request.data.get('profileId')
        adgroup_id = request.data.get('adGroupId')
        size = request.data.get('size')
        current = request.data.get('current')

        # TODO: validate form data
        data = EntityRetriever.get_adgroup_negative_keyword_list(
            profile_id, adgroup_id)
        content = {
            'code': 0,
            'msg': {
                'total': len(data),
                'size': size,
                'current': current,
                'list': data[size * (current - 1):size * current]
            }
        }
        return JsonResponse(content)



class AdGroupNegativeKeywordsList(APIView):
    def post(self, request):
        profile_id = request.data.get('profileId')
        adgroup_id = request.data.get('adGroupId')
        size = request.data.get('size')
        current = request.data.get('current')
        # TODO: validate form data

        data = EntityRetriever.get_adgroup_negative_keyword_list(
            profile_id, adgroup_id, size, current)
        content = {
            'code': 0,
            'msg': data
        }
        return JsonResponse(content)


def get_client_id(authorization):
    headers = {'Authorization': authorization}
    url = settings.AUTH_CLIENT_INFO_URL
    response = requests.post(url, headers=headers)
    if response.status_code != 200:
        logger.error('get client_id failed, response is %s' % response)
        raise APIException
    client_id = response.json()['data']['clientId']
    return client_id


def get_profile(client_id):
    content = {
        "code": 0,
        "msg": "操作成功",
        "data": []
    }
    sellers = CustomerSeller.objects.filter(customer_id=client_id)
    for seller in sellers:
        profiles = seller.sellerprofile_set.all()
        profile_list = []
        for profile in profiles:
            profile_dict = dict()
            profile_dict['countryCode'] = profile.country_code
            profile_dict['profileId'] = str(profile.profile_id)
            profile_dict['currencyCode'] = profile.currency_code
            profile_dict['marketplaceStringId'] = profile.marketplace_string_id
            profile_dict['amazonAccountId'] = profile.amazon_account_id
            profile_dict['status'] = profile.status
            profile_list.append(profile_dict)
        content['data'].append({
            "name": seller.seller_email,
            "createdAt": seller.created_at,
            "profiles": profile_list
        })
    content['data'].sort(key=lambda value: value['createdAt'], reverse=True)
    return content
