def format_report(row):
    """ format report data. """
    for key in ('orders', 'clicks', 'impressions'):
        row[key] = str(int(row[key]))
    for key in ('spend', 'sales'):
        row[key] = str(row[key])
    for key in ('acos', 'cpc'):
        row[key] = "%.2f" % row[key]
    row['ctr'] = "%.4f" % row['ctr']
    return row
