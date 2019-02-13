from django.conf import settings
from django.http import JsonResponse, HttpResponse
import requests
import logging
import json

logger = logging.getLogger('amazonads')


class CustomerAuthMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        # One-time configuration and initialization.

    def __call__(self, request):
        logger.info('the request path of AuthMiddleware is %s' % request.path)
        if request.path in ('/auth/',):
            return self.get_response(request)
        if request.path in ('/favicon.ico/', '/favicon.ico'):
            return HttpResponse('')
        try:
            # authenticate client
            headers = {
                'BasicAuthorization': 'Basic ' + settings.CLIENT_SECRECT,
                'Content-Type': "application/json"
            }
            token = request.META['HTTP_AUTHORIZATION'].split(' ', 1)[1]
            re = requests.post(
                settings.TOKEN_CHECK_URL,
                headers=headers,
                data=json.dumps({'access_token': token})
            )
            if re.status_code != 200 or re.json()['code'] != '0':
                re.raise_for_status()
        except Exception as err:
            logger.exception('Fail to Auth! path: %s , msg: %s' % (
                request.path, err))
            return JsonResponse(
                {'msg': 'Authentication Failed', 'code': 1},
                status=401
            )
        response = self.get_response(request)
        return response
