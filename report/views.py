import time

import requests
from django.conf import settings
from rest_framework.views import APIView
import json

from advertising.models import CustomerSeller, SellerAuth, SellerProfile
from advertising.views import get_client_id
from amazonads import verify_form_data
from django.http import JsonResponse
from report.report_retriever import SellerReportRetriver
import logging

logger = logging.getLogger('amazonads')


def check_data(obj, min_date, max_date):
    result = {'code': 1, 'msg': None}
    is_valid = obj.is_valid()
    if not is_valid:
        error_str = obj.errors.as_json()
        result['msg'] = list(json.loads(error_str).values())[0][0]['message']
        return JsonResponse(result)
    if min_date > max_date:
        return JsonResponse({'code': 1, 'msg': 'minDate must less than maxDate'})


class InitAccount(APIView):
    def post(self, request):
        logger.info('InitAccount start')
        authorization = request.META['HTTP_AUTHORIZATION']
        client_id = get_client_id(authorization)
        sellers = CustomerSeller.objects.filter(customer_id=client_id)
        for seller in sellers:
            refresh_token = SellerAuth.objects \
                .filter(seller=seller) \
                .first().refresh_token
            profiles = SellerProfile.objects.filter(seller=seller)
            for profile in profiles:
                profile_id = profile.profile_id
                country_code = profile.country_code
                authorizationArguments = json.dumps({
                    "refreshToken": refresh_token,
                    "clientSecret": settings.AMZ_CLIENT_SECRET,
                    "countryCode": country_code,
                })
                headers = {"Content-Type": "application/json"}
                data = json.dumps({
                    "id": 1,
                    "channelType": "AMAZON_ADVERTISING",
                    # "channelType" 固定设置"AMAZON_ADVERTISING"
                    "clientId": settings.AMZ_CLIENT_ID,
                    # clientId这里指的是amazon的CLIENT_ID
                    "sellerId": profile_id,
                    # sellerId这里指的是profileId
                    "authorizationArguments": authorizationArguments,
                    "bizType": "AMAZON_ADVERTISING"
                })
                url = settings.DATA_SERVICE_BASE_URL + '/channel/add'
                # 下载报告的接口
                try:
                    re = requests.post(url, headers=headers, data=data, timeout=10)
                    if re.status_code != 200:
                        logger.error('InitAccount api request report failed %s' % re)
                        re.raise_for_status()
                except Exception as ex:
                    logger.exception(ex)
            profiles.update(status="analyse")
            logger.info('InitAccount success')
        return JsonResponse({"code": 0, "msg": "ok", "status": "analyse"})


class ReportUpdateTime(APIView):
    def get(self, request):
        content = {
                  "code": 0,
                  "msg": "操作成功",
                  "data": {"report_update_time": ""}
                }
        profile_id = request.GET.get('profileId')
        if not profile_id:
            return JsonResponse({"code": 1, "msg": "profileId can not be null"})
        update_time = SellerReportRetriver.getReportUpdateTime(profile_id)
        if not update_time:
            content['data']['report_update_time'] = ""
            return JsonResponse(content)
        update_time_sec = time.mktime(time.strptime(update_time, "%Y-%m-%d %H:%M:%S"))
        update_time_del = time.time() - update_time_sec
        h, m = divmod(int(update_time_del), 3600)
        content['data']['report_update_time'] = "%s小时%s分钟前" % (h, m//60)
        return JsonResponse(content)


class SellerCampaignSummaries(APIView):
    def get(self, request):
        obj = verify_form_data.SellerAdForms(request.GET)
        min_date = request.GET.get('minDate')
        max_date = request.GET.get('maxDate')
        profile_id = request.GET.get('profileId')
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                  "code": 0,
                  "msg": "操作成功",
                  "data": {}
                }
        res = SellerReportRetriver.getSellerCampaignSummaries(
            profile_id, min_date, max_date)
        content['data'] = res
        return JsonResponse(content)


class SellerCampaignTrend(APIView):
    def get(self, request):
        obj = verify_form_data.SellerAdForms(request.GET)
        min_date = request.GET.get('minDate')
        max_date = request.GET.get('maxDate')
        profile_id = request.GET.get('profileId')
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                    "code": 0,
                    "msg": "操作成功",
                    "data": []
                }
        res = SellerReportRetriver.getSellerCampaignTrend(
            profile_id, min_date, max_date)
        content['data'] = res
        return JsonResponse(content)


class CampaignList(APIView):
    def post(self, request):
        obj = verify_form_data.CampaignForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        filter_condition = request.data.get('filter')
        download = request.data.get('download')
        current = int(request.data.get('current'))
        size = int(request.data.get('size'))
        order_by = request.data.get('orderBy') or "spend"
        order_type = request.data.get('orderType') or "asc"
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                "code": 0,
                "msg": "操作成功",
                "data": {
                        "total": "",
                        "size": "",
                        "current": "",
                        "list": []
                }
            }
        if download == "true":
            response = SellerReportRetriver.getCampaignList(
                profile_id, min_date, max_date, order_by, order_type,
                size, current, filter_condition, download)
            return response
        total, res = SellerReportRetriver.getCampaignList(
            profile_id, min_date, max_date, order_by, order_type,
            size, current, filter_condition, download)
        content['data']['total'] = total
        content['data']['size'] = size
        content['data']['current'] = current
        content['data']['list'] = res
        return JsonResponse(content)


class AdGroupSummaries(APIView):
    def post(self, request):
        obj = verify_form_data.SellerAdForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        campaign_id = request.data.get('campaignId')
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                  "code": 0,
                  "msg": "操作成功",
                  "data": {}
                }
        res = SellerReportRetriver.getAdGroupSummaries(
            profile_id, campaign_id, min_date, max_date)
        content['data'] = res
        return JsonResponse(content)


class AdGroupTrend(APIView):
    def post(self, request):
        obj = verify_form_data.CampaignDetailForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        campaign_id = request.data.get('campaignId')
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                    "code": 0,
                    "msg": "操作成功",
                    "data": []
                }
        res = SellerReportRetriver.getAdGroupTrend(
            profile_id, campaign_id, min_date, max_date)
        content['data'] = res
        return JsonResponse(content)


class AdGroupList(APIView):
    def post(self, request):
        obj = verify_form_data.AdGroupForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        campaign_id = request.data.get('campaignId')
        filter_condition = request.data.get('filter')
        download = request.data.get('download')
        targeting_type = request.data.get('targetingType') or ""
        current = int(request.data.get('current'))
        size = int(request.data.get('size'))
        order_by = request.data.get('orderBy') or "spend"
        order_type = request.data.get('orderType') or "asc"
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                "code": 0,
                "msg": "操作成功",
                "data": {
                    "total": '',
                    "size": '',
                    "current": '',
                    "list": []
                }
            }
        if download == "true":
            response = SellerReportRetriver.getAdGroupList(
                profile_id, campaign_id, targeting_type, min_date, max_date,
                order_by, order_type, size, current, filter_condition, download)
            return response
        total, res = SellerReportRetriver.getAdGroupList(
            profile_id, campaign_id, targeting_type, min_date, max_date,
            order_by, order_type, size, current, filter_condition, download)
        content['data']['total'] = total
        content['data']['total'] = total
        content['data']['size'] = size
        content['data']['current'] = current
        content['data']['list'] = res
        return JsonResponse(content)


class ProductAdSummaries(APIView):
    def post(self, request):
        obj = verify_form_data.SellerAdForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        ad_group_id = request.data.get('adGroupId')
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                  "code": 0,
                  "msg": "操作成功",
                  "data": {}
                }
        res = SellerReportRetriver.getProductAdSummaries(
            profile_id, ad_group_id, min_date, max_date)
        content['data'] = res
        return JsonResponse(content)


class ProductAdTrend(APIView):
    def post(self, request):
        obj = verify_form_data.AdGroupDetailForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        ad_group_id = request.data.get('adGroupId')
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                "code": 0,
                "msg": "操作成功",
                "data": []
            }
        res = SellerReportRetriver.getProductAdTrend(
            profile_id, ad_group_id, min_date, max_date)
        content['data'] = res
        return JsonResponse(content)


class AsinList(APIView):
    def post(self, request):
        obj = verify_form_data.ProductAdForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        ad_group_id = request.data.get('adGroupId')
        filter_condition = request.data.get('filter')
        download = request.data.get('download')
        current = int(request.data.get('current'))
        size = int(request.data.get('size'))
        order_by = request.data.get('orderBy') or "spend"
        order_type = request.data.get('orderType') or "asc"
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                "code": 0,
                "msg": "操作成功",
                "data":
                {
                    "total": "",
                    "size": "",
                    "current": "",
                    "list": []
                }
            }
        if download == "true":
            response = SellerReportRetriver.getAsinList(
                profile_id, ad_group_id, min_date, max_date,
                size, current, order_by, order_type, filter_condition, download)
            return response
        total, res = SellerReportRetriver.getAsinList(
            profile_id, ad_group_id, min_date, max_date,
            size, current, order_by, order_type, filter_condition, download)
        content['data']['total'] = total
        content['data']['size'] = size
        content['data']['current'] = current
        content['data']['list'] = res
        return JsonResponse(content)


class KeywordList(APIView):
    def post(self, request):
        obj = verify_form_data.ProductAdForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        ad_group_id = request.data.get('adGroupId')
        filter_condition = request.data.get('filter')
        download = request.data.get('download')
        targeting_type = request.data.get('targetingType')
        current = int(request.data.get('current'))
        size = int(request.data.get('size'))
        order_by = request.data.get('orderBy') or "spend"
        order_type = request.data.get('orderType') or "asc"
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                "code": 0,
                "msg": "操作成功",
                "data": {
                    "total": "",
                    "size": "",
                    "current": "",
                    "list": []
                }
            }
        if download == "true":
            response = SellerReportRetriver.getKeywordList(
                profile_id, ad_group_id, targeting_type, min_date, max_date,
                size, current, order_by, order_type, filter_condition, download)
            return response
        total, res = SellerReportRetriver.getKeywordList(
            profile_id, ad_group_id, targeting_type, min_date, max_date,
            size, current, order_by, order_type, filter_condition, download)
        content['data']['total'] = total
        content['data']['size'] = size
        content['data']['current'] = current
        content['data']['list'] = res
        return JsonResponse(content)


class SearchTermList(APIView):
    def post(self, request):
        obj = verify_form_data.KeywordQueryForms(request.data)
        min_date = request.data.get('minDate')
        max_date = request.data.get('maxDate')
        profile_id = request.data.get('profileId')
        ad_group_id = request.data.get('adGroupId')
        filter_condition = request.data.get('filter')
        download = request.data.get('download')
        current = int(request.data.get('current'))
        size = int(request.data.get('size'))
        order_by = request.data.get('orderBy') or "spend"
        order_type = request.data.get('orderType') or "asc"
        result = check_data(obj, min_date, max_date)
        if result:
            return result
        min_date = "".join(min_date.split("-"))
        max_date = "".join(max_date.split("-"))
        content = {
                "code": 0,
                "msg": "操作成功",
                "data": {
                    "total": "",
                    "size": "",
                    "current": "",
                    "list": []
                }
            }
        if download == "true":
            response = SellerReportRetriver.getSearchTermList(
                profile_id, ad_group_id, min_date, max_date,
                size, current, order_by, order_type, filter_condition, download)
            return response
        total, res = SellerReportRetriver.getSearchTermList(
            profile_id, ad_group_id, min_date, max_date,
            size, current, order_by, order_type, filter_condition, download)
        content['data']['total'] = total
        content['data']['size'] = size
        content['data']['current'] = current
        content['data']['list'] = res
        return JsonResponse(content)
