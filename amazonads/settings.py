"""
Django settings for amazonads project.
"""

import os
import env

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/2.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '+y#5bek7$vu&5%x3jcslojm9p%nhu-emvf-t)a9!!2s5$^@#p+'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env.DEBUG

if DEBUG:
    ALLOWED_HOSTS = [
        '*'
    ]
else:
    ALLOWED_HOSTS = [
        'link.biseller.com',
        '.biseller.com'
    ]

# Application definition
INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'corsheaders',
    'rest_framework',
    'report',
    'advertising',
    'django_celery_results',
    'django_extensions'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'amazonads.customer_auth_middleware.CustomerAuthMiddleware'
]

# 跨域增加忽略
CORS_ALLOW_CREDENTIALS = True
CORS_ORIGIN_ALLOW_ALL = True
CORS_ORIGIN_WHITELIST = (
    '*'
)

APPEND_SLASH = True

ROOT_URLCONF = 'amazonads.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

REST_FRAMEWORK = {
    'EXCEPTION_HANDLER': 'amazonads.exceptions.custom_exception_handler',
    'UNAUTHENTICATED_USER': None,
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
    ),
    'DEFAULT_PARSER_CLASSES': (
        'rest_framework.parsers.JSONParser',
    )
}

WSGI_APPLICATION = 'amazonads.wsgi.application'


# Database
# https://docs.djangoproject.com/en/2.1/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': env.DB_NAME,
        'HOST': env.DB_HOST,
        'PORT': env.DB_PORT,
        'USER': env.DB_USER,
        'PASSWORD': env.DB_PASSWORD,
    }
}
# Habse setting
DATA_DB_HOST = env.DATA_DB_HOST
DATA_DB_PORT = env.DATA_DB_PORT

# ElasticSearch setting
DATA_DB_ES_USERNAME = env.DATA_DB_ES_USERNAME
DATA_DB_ES_PASSWORD = env.DATA_DB_ES_PASSWORD
DATA_DB_ES_HOST = env.DATA_DB_ES_HOST
DATA_DB_ES_PORT = env.DATA_DB_ES_PORT

# Internationalization
# https://docs.djangoproject.com/en/2.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'Asia/Shanghai'

USE_I18N = True

USE_L10N = True

USE_TZ = False


STATIC_URL = '/static/'

# Redis settings
REDIS_HOST = env.REDIS_HOST
REDIS_PORT = env.REDIS_PORT
REDIS_PWD = env.REDIS_PWD

# Celery 设置
CELERY_BROKER_URL = 'redis://:%s@%s:%s/4' % (REDIS_PWD, REDIS_HOST, REDIS_PORT)
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_BACKEND = 'django-db'
CELERY_TIMEZONE = 'Asia/Shanghai'
CELERYD_MAX_TASKS_PER_CHILD = 20

# User Auth Endpoint
AUTH_URL = env.AUTH_BASE_URL
TOKEN_CHECK_URL = AUTH_URL + '/token/check'
AUTH_CLIENT_INFO_URL = AUTH_URL + '/api/user/base/info'
CLIENT_SECRECT = env.CLIENT_SECRECT


# Amazon Api Settings
AMZ_AD_ENDPOINT_NA = env.AMZ_ENDPOINT_NA
AMZ_AD_ENDPOINT_EU = env.AMZ_ENDPOINT_EU
AMZ_CLIENT_ID = env.AMZ_CLIENT_ID
AMZ_CLIENT_SECRET = env.AMZ_CLIENT_SECRET
AMZ_AUTHCODE_REDIRECT_PATH = env.AMZ_AUTHCODE_REDIRECT_PATH + '/'
AMZ_AUTHCODE_BASEURL = env.AMZ_AUTHCODE_BASEURL
AMZ_AUTH_URL = env.AMZ_AUTH_URL
DOMAIN = env.DOMAIN

# Front Settings
FRONT_BASE_URL = env.FRONT_BASE_URL
FRONT_AUTH_SUCCESS_URL = FRONT_BASE_URL + "/advertising/dataAnalysis"
DATA_SERVICE_BASE_URL = env.DATA_SERVICE_BASE_URL

# 配置django的logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
           'standard': {
                'format': "%(asctime)s %(name)s %(module)s.%(funcName)s %(levelname)s %(message)s"}
    },
    'filters': {
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'propagate': True,
            'level': 'INFO'
        },
        'tasks': {
            'handlers': ['console'],
            'propagate': True,
            'level': 'INFO'
        },
        'celery': {
            'handlers': ['console'],
            'propagate': True,
            'level': 'INFO'
        }
    }
}
