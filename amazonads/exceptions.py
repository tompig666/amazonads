from rest_framework.exceptions import APIException
from rest_framework.views import exception_handler
import logging

logger = logging.getLogger('amazonads')


def custom_exception_handler(exc, context):
    # receive response from exception
    if isinstance(exc, APIException):
        logger.exception(exc)
        response = exception_handler(exc, context)
        # add a new status_code
        response.data['code'] = 1
        response.data['msg'] = 'request failed'
        # delete old detail
        del response.data['detail']
        return response
    raise exc
