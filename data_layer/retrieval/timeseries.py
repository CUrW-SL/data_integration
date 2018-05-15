import pandas as pd

from datetime import datetime

from data_layer.models import Data, RunView
from data_layer.constants import COMMON_DATETIME_FORMAT


class Timeseries:
    def __init__(self, db):
        self.db = db

    def get_timeseries_id(self, run_name, station_name, variable, unit, event_type, source):
        result = RunView.query.filter_by(name=run_name,
                                         station=station_name,
                                         variable=variable,
                                         unit=unit,
                                         type=event_type,
                                         source=source
                                         ).all()
        return [run_view_obj.id for run_view_obj in result]

    def get_timeseries(self, timeseries_id, start_date, end_date):
        """
        Retrieves the timeseries corresponding to given id s.t.
        time is in between given start_date (inclusive) and end_date (exclusive).

        :param timeseries_id: string timeseries id
        :param start_date: datetime or string, should be of %s format
        :param end_date: datetime or string, should be of %s format
        :return: array of [id, time, value]
        """

        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            raise ValueError('start_date and/or end_date are not of datetime type.', start_date, end_date)

        start, end = start_date.strftime(COMMON_DATETIME_FORMAT), end_date.strftime(COMMON_DATETIME_FORMAT)
        result = Data.query.filter(Data.id == timeseries_id, Data.time >= start, Data.time < end).all()
        return [[data_obj.time, data_obj.value] for data_obj in result]

    def update_timeseries(self, timeseries_id, timeseries, should_overwrite):

        # timeseries should be a pnadas Dataframe, with 'time' as index, and 'value' as the column.
        if not isinstance(timeseries, pd.DataFrame):
            raise ValueError('The "timeseries" shoud be a pandas Dataframe containing (time, value) in a rows')

        session = self.db.session
        if should_overwrite:
            # update on conflict duplicate key.
            for index, row in timeseries.iterrows():
                session.merge(Data(id=timeseries_id, time=index.to_pydatetime(), value=float(row['value'])))
            session.commit()
            return True

        else:
            # raise IntegrityError on duplicate key.
            data_obj_list = []
            for index, row in timeseries.iterrows():
                data_obj_list.append(Data(id=timeseries_id, time=index.to_pydatetime(), value=float(row['value'])))

            session.bulk_save_objects(data_obj_list)
            session.commit()
            return True
