from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import crontab

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'amazonads.settings')

app = Celery('amazonads')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')


# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


app.conf.beat_schedule = {
    'retrieve_profiles': {
        'task': 'report.tasks.retrieve_profiles',
        'schedule': crontab(hour='*/6', minute=0),
        'args': ()
    },
    'retrieve_adgroup_bidrec': {
        'task': 'report.tasks.retrieve_adgroup_bidrec',
        'schedule': crontab(day_of_month='1'),
        'args': ()
    },
    'retrieve_kw_bidrec': {
        'task': 'report.tasks.retrieve_kw_bidrec',
        'schedule': crontab(day_of_month='2'),
        'args': ()
    }
}


app.conf.timezone = 'UTC'
