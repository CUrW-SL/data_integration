import copy
import numpy as np
import pandas as pd

from sqlalchemy import exc as SqlAlchemyExecptions

from curw.rainfall.wrf.extraction.spatial_utils import get_voronoi_polygons
from curw.rainfall.wrf.resources import manager as res_mgr
from data_layer.retrieval import Timeseries

from .config import timeseries_meta_data,  kub_mean_timeseries_meta


class KUBObservationMean:

    def __init__(self):
        self.shape_file = res_mgr.get_resource_path('extraction/shp/kub-wgs84/kub-wgs84.shp')
        self.output_dir = 'kub_observation_mean/output_shape/kub'
        self.percentage_factor = 100

    def calc_station_fraction(self, stations, precision_decimal_points=3):
        """
        Given station lat lon points must reside inside the KUB shape, otherwise could give incorrect results.
        :param stations: dict of station_name: [lon, lat] pairs
        :param precision_decimal_points: int
        :return: dict of station_id: area percentage
        """

        if stations is None:
            raise ValueError("'stations' cannot be null.")

        station_list = stations.keys()
        if len(station_list) <= 0:
            raise ValueError("'stations' cannot be empty.")

        station_fractions = {}
        if len(station_list) < 3:
            for station in station_list:
                station_fractions[station] = np.round(self.percentage_factor / len(station_list), precision_decimal_points)
            return station_fractions

        station_fractions = {}
        total_area = 0

        # calculate the voronoi/thesian polygons w.r.t given station points.
        voronoi_polygons = get_voronoi_polygons(points_dict=stations, shape_file=self.shape_file, add_total_area=True)

        for row in voronoi_polygons[['id', 'area']].itertuples(index=False, name=None):
            id = row[0]
            area = np.round(row[1], precision_decimal_points)
            station_fractions[id] = area
            # get_voronoi_polygons calculated total might not equal to sum of the rest, thus calculating total.
            if id != '__total_area__':
                total_area += area
        total_area = np.round(total_area, precision_decimal_points)

        for station in station_list:
            if station in station_fractions:
                station_fractions[station] = np.round(
                    (station_fractions[station] * self.percentage_factor) / total_area, precision_decimal_points)
            else:
                station_fractions[station] = np.round(0.0, precision_decimal_points)

        return station_fractions

    def calc_kub_mean(self, timerseries_dict, normalizing_factor='H', filler=0.0, precision_decimal_points=3):
        """
        :param timeseries: dict of (station_name: dict_inside) pairs. dict_inside should have
            ('lon_lat': [lon, lat]) and ('timeseries': pandas df with time(index), value columns)
        :param normalizing_factor: resampling factor, should be one of pandas resampling type
            (ref_link: http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases)
        :param filler value for missing values occur when normalizing
        :return: kub mean timeseries, [[time, value]...]
        """

        stations = {}
        timerseries_list = []
        for key in timerseries_dict.keys():
            stations[key] = timerseries_dict[key]['lon_lat']
            # Resample given set of timeseries.
            tms = timerseries_dict[key]['timeseries'].astype('float').resample(normalizing_factor).sum()
            # Rename coulmn_name 'value' to its own staion_name.
            tms = tms.rename(axis='columns', mapper={'value': key})
            timerseries_list.append(tms)

        if len(timerseries_list) <= 0:
            raise ValueError('Empty timeseries_dict given.')
        elif len(timerseries_list) == 1:
            matrix = timerseries_list[0]
        else:
            matrix = timerseries_list[0].join(other=timerseries_list[1:len(timerseries_list)], how='outer')

        # Note:
        # After joining resampling+sum does not work properly. Gives NaN andsum is not correct.
        # Therefore resamplig+sum is done for each timeseries. If this issue could be solved,
        # then resampling+sum could be carried out after joining.

        # Fill in missing values after joining into one timeseries matrix.
        matrix.fillna(value=np.round(filler, precision_decimal_points), inplace=True, axis='columns')

        station_fractions = self.calc_station_fraction(stations)

        # Make sure only the required station weights remain in the station_fractions, else raise ValueError.
        matrix_station_list = list(matrix.columns.values)
        weights_station_list = list(station_fractions.keys())
        invalid_stations = [key for key in weights_station_list if key not in matrix_station_list]
        for key in invalid_stations:
            station_fractions.pop(key, None)
        if not len(matrix_station_list) == len(station_fractions.keys()):
            raise ValueError('Problem in calculated station weights.', stations, station_fractions)

        # Prepare weights to calc the kub_mean.
        weights = pd.DataFrame.from_dict(data=station_fractions, orient='index', dtype='float')
        weights = weights.divide(self.percentage_factor, axis='columns')

        kub_mean = (matrix * weights[0]).sum(axis='columns')

        return kub_mean, station_fractions


class KUBObservationMeanUpdator:
    def __init__(self, db):
        self.db = db

    def update_kub_mean(self, start_datetime, end_datetime, should_fallback=True, is_forced=False):
        tms = Timeseries(self.db)
        timeseries_data = copy.deepcopy(timeseries_meta_data)
        failed_stations = []
        for station in timeseries_data.keys():
            timeseries_meta = timeseries_data[station]
            timeseries_id = tms.get_timeseries_id(run_name=timeseries_meta['run_name'],
                                                  station_name=station,
                                                  variable=timeseries_meta['variable'],
                                                  unit=timeseries_meta['unit'],
                                                  event_type=timeseries_meta['event_type'],
                                                  source=timeseries_meta['source'])
            # Checks consistency of obtained timeseries_ids.
            if len(timeseries_id) <= 0:
                if should_fallback:
                    failed_stations.append(station)
                    continue
                else:
                    return False, 'No timeseries_id found for station: %s' % station
            elif len(timeseries_id) > 1:
                if should_fallback:
                    failed_stations.append(station)
                    continue
                else:
                    return False, 'Database Inconsistency. %d timeseries_ids found for station: %s.' \
                           % (len(timeseries_id), station)

            timeseries = tms.get_timeseries(timeseries_id=timeseries_id[0], start_date=start_datetime, end_date=end_datetime)
            # Checks consistency of obtained timeseries.
            if len(timeseries) <= 0:
                if should_fallback:
                    failed_stations.append(station)
                    continue
                else:
                    return False, 'No timeseries found for the station: %s.' % station

            timeseries_df = pd.DataFrame(data=timeseries, columns=['time', 'value']).set_index(keys='time')
            timeseries_data[station]['timeseries'] = timeseries_df

        # Removes the failed stations. Then continue with the remaining.
        for station in failed_stations:
            timeseries_data.pop(station, None)

        # Validate remaining stations.
        if len(timeseries_data.keys()) <= 0:
            return False, 'No timseries can be found for any configed station of KUB.'

        # Calculate KUB Observation Mean.
        kub_obs_mean = KUBObservationMean()
        kub_mean_timeseries, station_fractions = kub_obs_mean.calc_kub_mean(timeseries_data)
        kub_mean_timeseries = kub_mean_timeseries.to_frame(name='value')

        # Store calculated KUB Observation Mean.
        try:
            kub_mean_timeseries_id = tms.get_timeseries_id(run_name=kub_mean_timeseries_meta['run_name'],
                                                           station_name=kub_mean_timeseries_meta['station_name'],
                                                           variable=kub_mean_timeseries_meta['variable'],
                                                           unit=kub_mean_timeseries_meta['unit'],
                                                           event_type=kub_mean_timeseries_meta['event_type'],
                                                           source=kub_mean_timeseries_meta['source'])
            tms.update_timeseries(kub_mean_timeseries_id[0], kub_mean_timeseries, is_forced)
        except SqlAlchemyExecptions.OperationalError:
            return False, 'DB connection error occurred during the operation. Please try again later.'
        except SqlAlchemyExecptions.IntegrityError:
            return False, 'To overwrite make force enabled by specifying force request param'

        # Prepare success response.
        st_frc = ""
        for st, frc in station_fractions.items():
            st_frc += " " + st + ":" + str(frc) + "%"
        response_content = 'Successfully updated KUB observation mean with station fractions of, { %s }' % st_frc
        return True, response_content
