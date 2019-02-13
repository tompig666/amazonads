"""Manage all Amazon ads API calls without explicit database support."""
from collections import defaultdict
from datetime import datetime
from gzip import GzipFile
from io import BytesIO
import json
from urllib.parse import urlencode
from urllib.parse import urlparse, ParseResult
import logging
import requests
from django.conf import settings


logger = logging.getLogger('amazonads')


class APIClient:
    """
    Managing Amazon ads API calls.

    All API calls implemented.
    """

    __COUNTRY_ENDPOINT_DICT = {
        'US': settings.AMZ_AD_ENDPOINT_NA,
        'CA': settings.AMZ_AD_ENDPOINT_NA,
        'UK': settings.AMZ_AD_ENDPOINT_EU,
        'FR': settings.AMZ_AD_ENDPOINT_EU,
        'DE': settings.AMZ_AD_ENDPOINT_EU,
        'IT': settings.AMZ_AD_ENDPOINT_EU,
        'ES': settings.AMZ_AD_ENDPOINT_EU,
    }

    __AUTHCODE_REDIRECT_URI = 'https://%s/%s' % \
        (settings.DOMAIN, settings.AMZ_AUTHCODE_REDIRECT_PATH)

    def __init__(self, access_token, refresh_token, profile_id_dict=None):
        """
        Init auth & profile ID dict.

        More about arguments:
        profile_id_dict -- 2-char ISO-3166-1 country code to profile ID.
        """
        self.__access_token = access_token
        self.__refresh_token = refresh_token
        self.__profile_id_dict = profile_id_dict or {}

    # Attribute operation
    def set_token(self, access_token, refresh_token):
        """Update cached tokens."""
        self.__access_token = access_token
        self.__refresh_token = refresh_token

    def get_country(self, profile_id):
        return self.__profile_id_dict.get(profile_id)

    # Auth related
    @classmethod
    def assemble_authcode_url(cls, state):
        """Get full authcode grant url with arguments."""
        parsed = urlparse(settings.AMZ_AUTHCODE_BASEURL)
        query = urlencode({
            'client_id': settings.AMZ_CLIENT_ID,
            'scope': 'cpc_advertising:campaign_management',
            'response_type': 'code',
            'redirect_uri': cls.__AUTHCODE_REDIRECT_URI,
            'state': state,
        })
        return ParseResult(parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment).geturl()

    @classmethod
    def grant_auth(cls, authcode):
        """Retrieve auth tokens and return."""
        logger.info('start to grant_auth %s' % authcode)
        response = requests.post(
            url=settings.AMZ_AUTH_URL,
            headers=cls.__build_auth_header(),
            data={
                'grant_type': 'authorization_code',
                'code': authcode,
                'redirect_uri': cls.__AUTHCODE_REDIRECT_URI,
                'client_id': settings.AMZ_CLIENT_ID,
                'client_secret': settings.AMZ_CLIENT_SECRET,
            },
        )

        if response.status_code == 400:
            logger.error('grant_auth failed,response is 400, reason is %s' % response.text)
            return (None, None)
        if response.status_code != 200:
            logger.error('grant_auth failed, response is %s,reason is %s'
                         % (response, response.text))
        response.raise_for_status()
        token_dict = response.json()
        logger.info('grant_auth success %s' % authcode)
        return token_dict.get('access_token'), token_dict.get('refresh_token')

    def refresh_auth(self):
        """Refresh auth tokens, update cached tokens."""
        response = requests.post(
            url=settings.AMZ_AUTH_URL,
            headers=self.__build_auth_header(),
            data={
                'grant_type': 'refresh_token',
                'client_id': settings.AMZ_CLIENT_ID,
                'client_secret': settings.AMZ_CLIENT_SECRET,
                'refresh_token': self.__refresh_token,
            },
        )
        if response.status_code == 400:
            self.__access_token = None
            self.__refresh_token = None
        else:
            response.raise_for_status()
            self.__access_token = response.json().get('access_token')
            self.__refresh_token = response.json().get('refresh_token')
        return self.__access_token, self.__refresh_token

    def retrieve_profile_dict(self):
        """Retrieve profiles, update cached profile dict."""
        profile_dict = {}
        for url in self.__build_all_urls('profiles'):
            response = requests.get(
                url=url,
                headers=self.__build_seller_header(),
            )
            if response.status_code != 200:
                logger.error('retrieve_profile_dict failed, response is %s,reason is %s'
                             % (response, response.text))
            response.raise_for_status()
            profile_dict.update((profile['country'], profile) for profile in
                                map(self.__parse_profile, response.json()))
        self.__profile_id_dict = {
            profile['profile_id']: country
            for country, profile in profile_dict.items()
        }
        return profile_dict

    def retrieve_single_profile(self, profile_id):
        """Retrieve single profile by ID, return the profile."""
        for url in self.__build_all_urls('profiles', profile_id):
            response = requests.get(
                url=url,
                headers=self.__build_seller_header(),
            )
            if response.status_code != 404:
                response.raise_for_status()
                return self.__parse_profile(response.json())
        return {}

    def update_profile(self, profile_col):
        """
        Update profiles, return successfulness/(ID or None) tuple list.

        More about arguments:
        profile_col -- Collection of dicts including country, profile_id &
        daily_budget. Update daily_budget.
        """
        endpoint_profile_dict = defaultdict(list)
        for profile in profile_col:
            url = self.__build_url(profile['country'], 'profiles')
            endpoint_profile_dict[url].add(profile)
        result_dict = {}
        for url, profile_list in endpoint_profile_dict:
            response = requests.put(
                url=url,
                headers=self.__build_seller_header(),
                json=map(self.__format_profile, profile_list),
            )
            response.raise_for_status()
            for result in response.json():
                result_dict[result['profileId']] = result['code'] == 'SUCCESS'
        return result_dict

    def create_profile(self, country):
        """
        Create a new profile, return profile ID. SANDBOX ENVIRONMENT ONLY.

        More about arguments:
        country -- US/CA/UK/DE/FR/IT/ES.
        """
        response = requests.put(
            url=self.__build_url(country, 'profiles', 'register'),
            headers=self.__build_seller_header(),
            json={'countryCode': country},
        )
        response.raise_for_status()
        return response.json()['profileId']

    # Entity operating
    def retrieve_entity(self, profile_id, record_type, extended=True, **kwargs):
        """
        Retrieve entities with filters.

        More about arguments:
        profile_id -- seller profile id.
        record_type -- One of 'campaigns'/'adGroups'/'productAds'/'keywords'/
        'negativeKeywords'/'campaignNegativeKeywords'.
        kwargs -- 'start_index', 'count', 'campaign_type', 'campaign_id_filter'
        and other record_type-related filters. Ignore 'adgroup_id'.

        More about kwargs, corresponding to record_type:
        'campaigns': state_filter, name.
        'adGroups': state_filter, adgroup_id_filter, name.
        'productAds': state_filter, adgroup_id_filter, productad_id_filter,
        sku, asin.
        'keywords': state_filter, adgroup_id_filter, keyword_id_filter,
        keyword_text, match_type_filter.
        'negativeKeywords': state_filter, adgroup_id_filter,
        keyword_id_filter, keyword_text, match_type_filter.
        'campaignNegativeKeywords': keyword_id_filter, keyword_text,
        match_type_filter.
        Note that 'name', 'sku', 'asin', 'keyword_text' receive only single
        values, whileas others receive comma-separated multiple values.
        IMPORTANT: keyword_id_filter for keywords / negativeKeywords /
        campaignNegativeKeywords works, but is ignored in documentation.
        state_filter for keywords / negativeKeywords works, but is mentioned as
        'state' in documentation. extended fields are not returned for
        campaignNegativeKeywords.
        """
        country = self.get_country(profile_id)
        response = requests.get(
            url=(self.__build_url(country, record_type, 'extended')
                 if extended else self.__build_url(country, record_type)),
            headers=self.__build_profile_header(profile_id),
            params=self.__format_entity_retrieval_param(kwargs),
        )
        response.raise_for_status()
        return [self.__parse_entity(raw_entity)
                for raw_entity in response.json()]

    def retrieve_single_entity(self, profile_id, record_type, entity_id,
                               extended=True):
        """Retrieve single entity, similar to retrieve_entity()."""
        country = self.get_country(profile_id)
        response = requests.get(
            url=(self.__build_url(country, record_type, 'extended', entity_id)
                 if extended
                 else self.__build_url(country, record_type, entity_id)),
            headers=self.__build_profile_header(profile_id),
        )
        if response.status_code == 404:
            return {}
        response.raise_for_status()
        return self.__parse_entity(response.json())

    def create_entity(self, profile_id, record_type, entity_col,
                      campaign_type='sponsoredProducts'):
        """
        Create entities, return successfulness/(ID or None) tuple list.

        More about arguments:
        profile_id -- seller profile_id retrive from amazon, int type.
        record_type -- One of 'campaigns'/'adGroups'/'productAds'/'keywords'/
        'negativeKeywords'/'campaignNegativeKeywords'.
        entity_col -- Collection of entity dicts, maximum 100 for 'campaigns' &
        'adGroups', 1000 for other record types.

        More about keys in entity, corresponding to record_type:
        'campaigns': name, campaign_type, targeting_type, state, daily_budget
        (>=1.00), start_date (>=today) required. end_date, bid_plus (only
        manual can set true) optional.
        'adGroups': campaign_id, name, default_bid (>=0.02), state required.
        'productAds': campaign_id, adgroup_id, sku, state required.
        'keywords': campaign_id, adgroup_id, keyword_text, match_type, state
        required. bid optional.
        'negativeKeywords': campaign_id, adgroup_id, keyword_text,
        match_type, state required.
        'campaignNegativeKeywords': campaign_id, keyword_text, match_type,
        state required.
        Common note: state in ('enabled', 'paused').
        """

        country = self.get_country(profile_id)
        response = requests.post(
            url=self.__build_url(country, 'sp', record_type),
            headers=self.__build_profile_header(profile_id),
            json=[self.__format_entity(
                {**entity, **{'campaignType': campaign_type}}
                if record_type == 'campaigns'
                else entity
            ) for entity in entity_col],
        )
        response.raise_for_status()
        self.__raise_server_is_busy_in_batch(response)
        return map(self.__parse_entity_operation, response.json())

    def update_entity(self, profile_id, record_type, entity_col):
        """
        Update entities, return successfulness/(ID or None) tuple list.

        More about arguments:
        profile_id -- seller profile_id retrive from amazon, int type.
        record_type -- One of 'campaigns'/'adGroups'/'productAds'/'keywords'/
        'negativeKeywords'/'campaignNegativeKeywords'.
        entity_col -- Collection of entity dicts, maximum 100 for 'campaigns' &
        'adGroups', 1000 for other record types.

        More about keys in entity, corresponding to record_type:
        'campaigns': campaign_id required. name, state, daily_budget,
        start_date, end_date, bid_plus (only manual can set true) optional.
        'adGroups': adgroup_id required. name, default_bid, state optional.
        'productAds': productad_id required. state optional.
        'keywords': keyword_id required. state, bid optional.
        'negativeKeywords': keyword_id required. state optional.
        'campaignNegativeKeywords': keyword_id required. state optional.
        Common note: duplicate entity IDs fail after the first one.
        Note for campaignNegativeKeywords that the only state is 'enabled',
        otherwise update it to 'deleted', and it will be deleted forever.
        """
        country = self.get_country(profile_id)
        response = requests.put(
            url=self.__build_url(country, 'sp', record_type),
            headers=self.__build_profile_header(profile_id),
            json=[self.__format_entity(entity) for entity in entity_col],
        )
        response.raise_for_status()
        self.__raise_server_is_busy_in_batch(response)
        return map(self.__parse_entity_operation, response.json())

    def archive_single_entity(self, profile_id, record_type, entity_id):
        """Archive single entity by updating state to 'archived'."""
        country = self.get_country(profile_id)
        response = requests.delete(
            url=self.__build_url(country, record_type, entity_id),
            headers=self.__build_profile_header(profile_id),
        )
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    # Snapshot
    def request_snapshot(self, profile_id, record_type, state_filter=None,
                         campaign_type='sponsoredProducts'):
        """
        Request a snapshot, return snapshot ID or empty string.

        More about arguments:
        profile_id -- seller profile id ,int type.
        record_type -- One of 'campaigns'/'adGroups'/'productAds'/'keywords'/
        'negativeKeywords'/'campaignNegativeKeywords'.
        IMPORTANT: Semantically, "success" should be 202, and it is 202 in
        documentation, but both sandbox & production return 200.
        """
        request_dict = {'campaignType': campaign_type}
        if state_filter:
            request_dict['stateFilter'] = self.__join_filter(state_filter)
        country = self.get_country(profile_id)
        response = requests.post(
            url=self.__build_url(country, record_type, 'snapshot'),
            headers=self.__build_profile_header(profile_id),
            json=request_dict,
        )
        response.raise_for_status()
        return response.json()['snapshotId']

    def retrieve_snapshot_download_uri(self, profile_id, snapshot_id):
        """Retrieve requested snapshot metadata, return a download URI/None."""
        country = self.get_country(profile_id)
        response = requests.get(
            url=self.__build_url(country, 'snapshots', snapshot_id),
            headers=self.__build_profile_header(profile_id),
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()['location']

    def download_snapshot(self, profile_id, download_uri):
        """Download and parse raw snapshot file, return snapshot data."""
        response = requests.get(
            url=download_uri,
            headers=self.__build_profile_download_header(profile_id),
        )
        if response.status_code == 403:
            return None
        response.raise_for_status()
        return [self.__parse_entity(raw_entity) for raw_entity
                in json.loads(response.content.decode('utf-8'))]

    # Bid recommendation
    def retrieve_adgroup_bidrec(self, profile_id, adgroup_id):
        """Retrieve a bid recommendation for an auto adgroup."""
        country = self.get_country(profile_id)
        response = requests.get(
            url=self.__build_url(country, 'adGroups', adgroup_id,
                                 'bidRecommendations'),
            headers=self.__build_profile_header(profile_id),
        )
        if response.status_code == 404:
            logger.error('retrieve adgroup bidrec error, message: %s'
                         % response.text)
            return None
        response.raise_for_status()
        return self.__parse_bidrec(response.json()['suggestedBid'])

    def retrieve_keyword_bidrec(self, profile_id, keyword_id):
        """Retrieve a bid recommendation for a keyword in a manual adgroup."""
        country = self.get_country(profile_id)
        response = requests.get(
            url=self.__build_url(country, 'keywords', keyword_id,
                                 'bidRecommendations'),
            headers=self.__build_profile_header(profile_id),
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return self.__parse_bidrec(response.json()['suggestedBid'])

    def retrieve_raw_keyword_bidrec(self, profile_id,
                                    adgroup_id, raw_keyword_col):
        """
        Retrieve bid recommendations for raw keywords referring to an
        adgroup, return bid recommendations or None in the same order of input.

        More about arguments:
        raw_keyword_list -- List pairs of text & match type, not keyword ID.
        """
        country = self.get_country(profile_id)
        response = requests.post(
            url=self.__build_url(country, 'keywords', 'bidRecommendations'),
            headers=self.__build_profile_header(profile_id),
            json={
                'adGroupId': adgroup_id,
                'keywords': [{'keyword': keyword, 'matchType': match_type}
                             for keyword, match_type in raw_keyword_col],
            },
        )
        response.raise_for_status()
        return map(self.__parse_raw_keyword_bidrec,
                   response.json()['recommendations'])

    # Keyword suggestion
    def retrieve_adgroup_sugkey(self, profile_id, adgroup_id,
                                extended=False, **kwargs):
        """
        Retrieve Amazon suggested keywords for manual adgroup.

        More about arguments:
        kwargs -- max_suggestion_num, ad_state_filter for both cases,
        and suggest_bid with value 'yes'/'no' for extended only.
        """
        country = self.get_country(profile_id)
        response = requests.get(
            url=(self.__build_url(country, 'adGroups', adgroup_id,
                                  'suggested', 'keywords', 'extended')
                 if extended
                 else self.__build_url(country, 'adGroups', adgroup_id,
                                       'suggested', 'keywords')),
            headers=self.__build_profile_header(profile_id),
            params=self.__format_sugkey_param(
                'adgroup_extended' if extended else 'adgroup', kwargs
            ),
        )
        response.raise_for_status()
        raw_sugkey_list = response.json() if extended \
            else response.json()['suggestedKeywords']
        return [self.__parse_sugkey(raw_sugkey)
                for raw_sugkey in raw_sugkey_list]

    def retrieve_single_asin_sugkey(self, profile_id, asin, **kwargs):
        """
        Retrieve Amazon suggested keywords for asin.

        More about arguments:
        kwargs -- max_suggestion_num.
        """
        response = requests.get(
            url=self.__build_url(profile_id, 'asins', asin, 'suggested',
                                 'keywords'),
            headers=self.__build_profile_header(profile_id),
            params=self.__format_sugkey_param('asin', kwargs),
        )
        response.raise_for_status()
        return [self.__parse_sugkey(raw_sugkey)
                for raw_sugkey in response.json()]

    def retrieve_asin_sugkey(self, profile_id, asin_col, **kwargs):
        """
        Retrieve Amazon suggested keywords for asin.

        More about arguments:
        kwargs -- max_suggestion_num.
        """
        param_dict = self.__format_sugkey_param('asin', kwargs)
        param_dict['asins'] = list(asin_col)
        country = self.get_country(profile_id)
        response = requests.post(
            url=self.__build_url(country, 'asins', 'suggested', 'keywords'),
            headers=self.__build_profile_header(profile_id),
            json=param_dict,
        )
        response.raise_for_status()
        return [self.__parse_sugkey(raw_sugkey)
                for raw_sugkey in response.json()]

    # Report
    def request_report(self, profile_id, date, record_type, segment=None,
                       campaign_type='sponsoredProducts'):
        """
        Request a report, return report ID or empty string.

        Only useful report metrics requested.
        More about arguments:
        profile_id -- seller profile id ,int type.
        date -- Date object.
        record_type -- One of 'campaigns'/'adGroups'/'productAds'/'keywords'.
        segment -- 'query' or None. 'query' works with 'keywords'. Ignore
        impressions field when segment is 'query', since data is incomplete.
        Note that asins report & placement segment for campaign report ignored.
        """
        country = self.get_country(profile_id)
        response = requests.post(
            url=self.__build_url(country, record_type, 'report'),
            headers=self.__build_profile_header(profile_id),
            json={
                'campaignType': campaign_type,
                'segment': segment,
                'reportDate': self.__format_date(date),
                'metrics': (('' if segment == 'query' else 'impressions,') +
                            ','.join(('clicks',
                                      'cost',
                                      'attributedConversions1dSameSKU',
                                      'attributedConversions1d',
                                      'attributedSales1dSameSKU',
                                      'attributedSales1d',
                                      'attributedConversions7dSameSKU',
                                      'attributedConversions7d',
                                      'attributedSales7dSameSKU',
                                      'attributedSales7d',
                                      'attributedConversions30dSameSKU',
                                      'attributedConversions30d',
                                      'attributedSales30dSameSKU',
                                      'attributedSales30d'))),
            },
        )
        if response.status_code == 406:
            return None
        response.raise_for_status()
        return response.json().get('reportId')

    def retrieve_report_download_uri(self, profile_id, report_id):
        """Retrieve requested report metadata, return a download URI/None."""
        country = self.get_country(profile_id)
        response = requests.get(
            url=self.__build_url(country, 'reports', report_id),
            headers=self.__build_profile_header(profile_id),
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json().get('location')

    def download_report(self, profile_id, download_uri):
        """Download and parse a gzipped report file, return report data."""
        response = requests.get(
            url=download_uri,
            headers=self.__build_profile_download_header(profile_id),
        )
        if response.status_code == 403:
            return None
        response.raise_for_status()
        return [self.__parse_report(raw_report) for raw_report
                in json.loads(GzipFile(fileobj=BytesIO(response.content))
                              .read().decode('utf-8'))]

    # HTTP operations
    @classmethod
    def __build_url(cls, country, *parts):
        return cls.__COUNTRY_ENDPOINT_DICT[country] + \
            '/'.join(str(e) for e in parts)

    @classmethod
    def __build_all_urls(cls, *parts):
        for endpoint in set(cls.__COUNTRY_ENDPOINT_DICT.values()):
            yield endpoint + '/'.join(str(e) for e in parts)

    @classmethod
    def __build_auth_header(cls):
        return {
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        }

    def __build_seller_header(self):
        return {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.__access_token,
            'Amazon-Advertising-API-ClientId': settings.AMZ_CLIENT_ID
        }

    def __build_profile_header(self, profile_id):
        return {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.__access_token,
            'Amazon-Advertising-API-Scope': str(profile_id),
            'Amazon-Advertising-API-ClientId': settings.AMZ_CLIENT_ID
        }

    def __build_profile_download_header(self, profile_id):
        return {
            'Authorization': 'Bearer ' + self.__access_token,
            'Amazon-Advertising-API-Scope': str(profile_id),
        }

    @classmethod
    def __raise_server_is_busy_in_batch(cls, response):
        if response.status_code == 207 and \
            any(result['code'] == 'SERVER_IS_BUSY'
                for result in response.json()):
            raise requests.exceptions.HTTPError('Server is busy in batch.',
                                                response=response)

    # API call data formatting
    @classmethod
    def __join_filter(cls, value_col):
        return ','.join(str(value) for value in set(value_col))

    @classmethod
    def __format_date(cls, date):
        return datetime.strftime(date, '%Y%m%d')

    @classmethod
    def __format_profile(cls, profile):
        return {'profileId': int(profile['profile_id']),
                'dailyBudget': float(profile['daily_budget'])}

    @classmethod
    def __format_entity_retrieval_param(cls, kwargs):
        return {
            param: func(kwargs[arg]) if func else kwargs[arg]
            for arg, param, func in (
                ('start_index', 'startIndex', int),
                ('count', 'count', int),
                ('campaign_type', 'campaignType', None),
                ('campaign_id_filter', 'campaignIdFilter', cls.__join_filter),
                ('adgroup_id_filter', 'adGroupIdFilter', cls.__join_filter),
                ('productad_id_filter', 'adIdFilter', cls.__join_filter),
                ('keyword_id_filter', 'keywordIdFilter', cls.__join_filter),
                ('state_filter', 'stateFilter', cls.__join_filter),
                ('name', 'name', None),
                ('sku', 'sku', None),
                ('asin', 'asin', None),
                ('keyword_text', 'keywordText', None),
                ('match_type_filter', 'matchTypeFilter', cls.__join_filter),
            ) if kwargs.get(arg)
        }

    @classmethod
    def __format_sugkey_param(cls, sugkey_type, kwargs):
        sugkey_param_tuple = (('max_suggestion_num', 'maxNumSuggestions',
                               int),)
        if sugkey_type in ('adgroup', 'adgroup_extended'):
            sugkey_param_tuple += (('ad_state_filter', 'adStateFilter',
                                    cls.__join_filter),)
        if sugkey_type == 'adgroup_extended':
            sugkey_param_tuple += (('suggest_bid', 'suggestBids', None),)
        return {
            param: (func(kwargs[arg]) if func else kwargs[arg])
            for arg, param, func in sugkey_param_tuple
            if kwargs.get(arg)
        }

    @classmethod
    def __format_entity(cls, entity):
        return {
            raw_field:
            (func(entity[field]) if func and entity[field] is not None
             else entity[field])
            for field, raw_field, func in (
                ('campaignType', 'campaignType', None),
                ('state', 'state', None),
                ('campaignId', 'campaignId', int),
                ('adGroupId', 'adGroupId', int),
                ('adId', 'adId', int),
                ('keywordId', 'keywordId', int),
                ('name', 'name', None),
                ('targetingType', 'targetingType', None),
                ('dailyBudget', 'dailyBudget', float),
                ('startDate', 'startDate', None),
                ('endDate', 'endDate', None),
                ('premiumBidAdjustment', 'premiumBidAdjustment', bool),
                ('defaultBid', 'defaultBid', float),
                ('sku', 'sku', None),
                ('keywordText', 'keywordText', None),
                ('matchType', 'matchType', None),
                ('bid', 'bid', float),
            ) if entity.get(field) is not None or
            (field in ('end_date', 'bid') and field in entity)
        }

    # API raw data parsing
    @classmethod
    def __parse_date(cls, date_str):
        return datetime.strptime(date_str, '%Y%m%d').date() \
            if date_str else None

    @classmethod
    def __parse_profile(cls, raw_profile):
        profile = {
            'profile_id': int(raw_profile['profileId']),
            'country': raw_profile['countryCode'],
            'currency': raw_profile['currencyCode'],
            'daily_budget': float(raw_profile['dailyBudget']),
            'timezone': raw_profile['timezone'],
            'account_type': raw_profile['accountInfo']['type'],
            'marketplace_str_id':
                raw_profile['accountInfo']['marketplaceStringId'],
            'seller_str_id':
                raw_profile['accountInfo']['id'],
        }
        if profile['timezone'] == 'BST':
            profile['timezone'] = 'Europe/London'
        return profile

    @classmethod
    def __parse_profile_operation(cls, result):
        return (True, result['profileId']) if result['code'] == 'SUCCESS' \
               else (False, json.dumps(result))

    @classmethod
    def __parse_entity_operation(cls, result):
        if result['code'] == 'SUCCESS':
            for key in ('campaignId', 'adGroupId', 'adId', 'keywordId'):
                if key in result:
                    return (True, result[key])
        return (False, json.dumps(result))

    @classmethod
    def __parse_entity(cls, raw_entity):
        return {
            field:
            (func(raw_entity[raw_field])
             if func and raw_entity[raw_field] is not None
             else raw_entity[raw_field])
            for raw_field, field, func in (
                ('state', 'state', None),
                ('servingStatus', 'servingStatus', None),
                ('campaignId', 'campaignId', int),
                ('adGroupId', 'adGroupId', int),
                ('adId', 'adId', int),
                ('keywordId', 'keywordId', int),
                ('name', 'name', None),
                ('targetingType', 'targetingType', None),
                ('dailyBudget', 'dailyBudget', float),
                ('startDate', 'startDate', cls.__parse_date),
                ('endDate', 'endDate', cls.__parse_date),
                ('premiumBidAdjustment', 'premiumBidAdjustment', bool),
                ('defaultBid', 'defaultBid', float),
                ('sku', 'sku', None),
                ('asin', 'asin', None),
                ('keywordText', 'keywordText', None),
                ('matchType', 'matchType', None),
                ('bid', 'bid', float),
            ) if raw_entity.get(raw_field) is not None or
            (raw_field in ('endDate', 'bid') and raw_field in raw_entity)
        }  # Ignore fields 'creationDate', lastUpdatedDate', 'campaignType'.

    @classmethod
    def __parse_bidrec(cls, raw_bidrec):
        return {
            field: float(raw_bidrec[raw_field])
            for raw_field, field in (
                ('suggested', 'suggested'),
                ('rangeStart', 'rangeStart'),
                ('rangeEnd', 'rangeEnd'),
            ) if raw_bidrec.get(raw_field) is not None
        }

    @classmethod
    def __parse_raw_keyword_bidrec(cls, raw_raw_keyword_bidrec):
        # return cls.__parse_bidrec(raw_raw_keyword_bidrec['suggestedBid']) \
        #     if raw_raw_keyword_bidrec['code'] == 'SUCCESS' \
        #     else None
        raw_keyword_bidrec_dic = \
            cls.__parse_bidrec(raw_raw_keyword_bidrec['suggestedBid'])
        raw_keyword_bidrec_dic["keyword"] = raw_raw_keyword_bidrec["keyword"]
        raw_keyword_bidrec_dic["matchType"] = \
            raw_raw_keyword_bidrec["matchType"]
        return raw_keyword_bidrec_dic \
            if raw_raw_keyword_bidrec['code'] == 'SUCCESS' else None

    @classmethod
    def __parse_sugkey(cls, raw_sugkey):
        return {
            field:
            (func(raw_sugkey[raw_field])
             if func and raw_sugkey[raw_field] is not None
             else raw_sugkey[raw_field])
            for raw_field, field, func in (
                ('keywordText', 'keyword_text', None),
                ('matchType', 'match_type', None),
                ('campaignId', 'campaign_id', int),
                ('adGroupId', 'adgroup_id', int),
                ('state', 'state', None),
                ('bid', 'bid', float),
            ) if raw_field in raw_sugkey
        }

    @classmethod
    def __parse_report(cls, raw_report):
        return {
            field:
            func(raw_report[raw_field]) if func else raw_report[raw_field]
            for raw_field, field, func in (
                ('campaignId', 'campaign_id', int),
                ('adGroupId', 'adgroup_id', int),
                ('adId', 'productad_id', int),
                ('keywordId', 'keyword_id', int),
                ('query', 'query', None),
                ('impressions', 'impressions', int),
                ('clicks', 'clicks', int),
                ('cost', 'cost', float),
                ('attributedConversions1dSameSKU', 'sku_convs_1d', int),
                ('attributedConversions1d', 'convs_1d', int),
                ('attributedSales1dSameSKU', 'sku_sales_1d', float),
                ('attributedSales1d', 'sales_1d', float),
                ('attributedConversions7dSameSKU', 'sku_convs_7d', int),
                ('attributedConversions7d', 'convs_7d', int),
                ('attributedSales7dSameSKU', 'sku_sales_7d', float),
                ('attributedSales7d', 'sales_7d', float),
                ('attributedConversions30dSameSKU', 'sku_convs_30d', int),
                ('attributedConversions30d', 'convs_30d', int),
                ('attributedSales30dSameSKU', 'sku_sales_30d', float),
                ('attributedSales30d', 'sales_30d', float),
            ) if raw_report.get(raw_field) is not None
        }
