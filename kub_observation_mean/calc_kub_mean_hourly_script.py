import pandas as pd
from datetime import datetime, timedelta
from urllib import request


DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

start_datetime = datetime.strptime('2018-05-26 00:00:00', DATE_TIME_FORMAT)
end_datetime = datetime.strptime('2018-05-30 15:00:00', DATE_TIME_FORMAT)

url = 'http://104.198.0.87:5005/trigger/update/kub_obs_mean?fall_back=true&force=true&from=%s&to=%s'


timedelta_index = pd.date_range(start=start_datetime, end=end_datetime, freq='H').to_series()
for index, value in timedelta_index.iteritems():
    start = index.to_pydatetime()
    end = start + timedelta(hours=1, minutes=30)
    tmp_url = url % (start.strftime(DATE_TIME_FORMAT), end.strftime(DATE_TIME_FORMAT))
    print(tmp_url)
    print(request.urlopen(tmp_url).read())
    print()
