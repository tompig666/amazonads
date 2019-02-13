import datetime


def handel_date(min_date, max_date):
    today = datetime.datetime.now()
    min_date = datetime.datetime.strptime(min_date, '%Y-%m-%d')
    max_date = datetime.datetime.strptime(max_date, '%Y-%m-%d')
    diff = (max_date - min_date).days + 1
    if diff == 1 and (today - max_date).days == 0:
        date_range = 'today'
    elif diff == 1 and (today - max_date).days == 1:
        date_range = 'yesterday'
    elif diff == 7:
        date_range = 'latest_7_days'
    elif diff == 15:
        date_range = 'latest_15_days'
    elif diff == 30:
        date_range = 'latest_30_days'
    elif diff == 60:
        date_range = 'latest_60_days'
    else:
        return 'yesterday'
    return date_range


def revert_str(input):
    return str(input[::-1])
