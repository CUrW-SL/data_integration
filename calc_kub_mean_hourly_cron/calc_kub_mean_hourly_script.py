import pandas as pd
from datetime import datetime, timedelta
from urllib import request


def datetime_lk_to_utc(timestamp_lk,  shift_mins=0):
    return timestamp_lk - timedelta(hours=5, minutes=30 + shift_mins)


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_TIME_FORMAT_HOURLY = '%Y-%m-%d %H:00:00'

# start_datetime = datetime.strptime('2018-09-24 00:00:00', DATE_TIME_FORMAT)
# end_datetime = datetime.strptime('2018-09-25 11:00:00', DATE_TIME_FORMAT)

start_datetime = datetime.strptime(datetime_utc_to_lk(datetime.now() - timedelta(hours=3))
                                   .strftime(DATE_TIME_FORMAT_HOURLY), DATE_TIME_FORMAT)
end_datetime = datetime.strptime(datetime_utc_to_lk(datetime.now()).strftime(DATE_TIME_FORMAT_HOURLY),
                                 DATE_TIME_FORMAT)

url = 'http://localhost:5005/trigger/update/kub_obs_mean?fall_back=true&force=true&from=%s&to=%s'


timedelta_index = pd.date_range(start=start_datetime, end=end_datetime, freq='H').to_series()
for index, value in timedelta_index.iteritems():
    start = index.to_pydatetime()
    end = start + timedelta(hours=1, minutes=30)
    end = end if end_datetime >= end else end_datetime

    if start > end:
        tmp_url = url % (start.strftime(DATE_TIME_FORMAT), end.strftime(DATE_TIME_FORMAT))
        print(tmp_url)
        print(request.urlopen(tmp_url).read())
        print()
