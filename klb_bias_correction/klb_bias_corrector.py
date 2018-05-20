import pandas as pd

from data_layer.retrieval import Timeseries
from data_layer.exceptions import InconsistencyError

from .config import quadrant_frcst_tms_meta, quadrant_obs_tms_meta
from .config import add_error_tms_meta, mul_error_tms_meta, error_tms_meta_mapping


class KLBBiasCorrector:
    # method : {‘backfill’, ‘bfill’, ‘pad’, ‘ffill’, None}, default None
    # Method to use for filling holes in reindexed Series pad / ffill: propagate last valid observation forward to
    # next valid backfill / bfill: use NEXT valid observation to fill gap

    def __init__(self, db):
        self.db = db

    def _calc_error_scale(self, obs_mean, forecast_mean, freq):
        """
        :param obs_mean: pandas dataframe with 'time' 'value' where 'time' is the index
        :param forecast_mean: pandas dataframe with 'time' 'value' where 'time' is the index
        :param freq: pandas compatible resampling frequency
        :return: tuple of pandas Series with columns: 'time' 'error_scale' where 'time' is the index
        """
        forecast_mean = forecast_mean.resample(freq).sum()
        obs_mean = obs_mean.resample(freq).sum()

        diff_error = obs_mean.subtract(forecast_mean, fill_value=0.0, axis='columns')
        diff_error.index.name = 'time'

        div_error = obs_mean.apply(pd.to_numeric, errors='coerce').divide(
            forecast_mean.apply(pd.to_numeric, errors='coerce'), axis='columns')
        div_error.fillna(value=-1, inplace=True, axis='columns')
        div_error.index.name = 'time'

        return diff_error, div_error

    def calc_error_scale(self, model, quadrant, start_datetime, end_datetime, frequency='H', is_forced=False):
        """
        :param model: specifies the interested wrf model [wrf0, wrf1, wrf2, wrf3, wrf4, wrf5]
        :param quadrant: klb quadrant [met_col0, met_col1, met_col2, met_col3]
        :param start_datetime: python datetime
        :param end_datetime: python datetime
        :param frequency: pandas compatible resampling frequency
        :param is_forced: specifies the whether to overwrite or not the error scales in the db existing
        :return:
        """
        frcst_tms_meta = quadrant_frcst_tms_meta[model][quadrant]
        obs_tms_meta = quadrant_obs_tms_meta[quadrant]
        tms_retriever = Timeseries(self.db)

        frcst_tms_id = tms_retriever.get_timeseries_id(run_name=frcst_tms_meta['run_name'],
                                                       station_name=frcst_tms_meta['station_name'],
                                                       variable=frcst_tms_meta['variable'],
                                                       unit=frcst_tms_meta['unit'],
                                                       event_type=frcst_tms_meta['event_type'],
                                                       source=frcst_tms_meta['source'])
        obs_tms_id = tms_retriever.get_timeseries_id(run_name=obs_tms_meta['run_name'],
                                                     station_name=obs_tms_meta['station_name'],
                                                     variable=obs_tms_meta['variable'],
                                                     unit=obs_tms_meta['unit'],
                                                     event_type=obs_tms_meta['event_type'],
                                                     source=obs_tms_meta['source'])
        if not frcst_tms_id or not obs_tms_id:
            raise InconsistencyError('No timeseries exists for the given timeseries meta data.',
                                     frcst_tms_meta if not frcst_tms_id else obs_tms_meta)
        elif len(frcst_tms_id) > 1 or len(obs_tms_id) > 1:
            raise InconsistencyError('Cannot have multiple ids for the same timerseries meta data.',
                                     frcst_tms_meta if len(frcst_tms_id) > 1 else obs_tms_meta)
        frcst_tms_id = frcst_tms_id[0]
        obs_tms_id = obs_tms_id[0]

        frcst_tms = tms_retriever.get_timeseries(frcst_tms_id, start_datetime, end_datetime)
        frcst_tms_df = pd.DataFrame(data=frcst_tms, columns=['time', 'value']).set_index(keys='time')
        obs_tms = tms_retriever.get_timeseries(obs_tms_id, start_datetime, end_datetime)
        obs_tms_df = pd.DataFrame(data=obs_tms, columns=['time', 'value']).set_index(keys='time')

        # Calculate error scales.
        add_err_scl, mul_err_scl = self._calc_error_scale(obs_tms_df, frcst_tms_df, frequency)

        # Store error scales in the db.
        add_err_meta = add_error_tms_meta[model][quadrant]
        mul_err_meta = mul_error_tms_meta[model][quadrant]

        add_err_tms_id = tms_retriever.get_timeseries_id(run_name=add_err_meta['run_name'],
                                                         station_name=add_err_meta['station_name'],
                                                         variable=add_err_meta['variable'],
                                                         unit=add_err_meta['unit'],
                                                         event_type=add_err_meta['event_type'],
                                                         source=add_err_meta['source'])
        mul_err_tms_id = tms_retriever.get_timeseries_id(run_name=mul_err_meta['run_name'],
                                                         station_name=mul_err_meta['station_name'],
                                                         variable=mul_err_meta['variable'],
                                                         unit=mul_err_meta['unit'],
                                                         event_type=mul_err_meta['event_type'],
                                                         source=mul_err_meta['source'])
        if len(add_err_tms_id) <= 0:
            station = {'name': add_err_meta['station_name'],
                       'id': error_tms_meta_mapping['station_name'][add_err_meta['station_name']]}
            variable = {'name': add_err_meta['variable'],
                        'id': error_tms_meta_mapping['variable'][add_err_meta['variable']]}
            unit = {'name': add_err_meta['unit'],
                    'id': error_tms_meta_mapping['unit'][add_err_meta['unit']]}
            event_type = {'name': add_err_meta['event_type'],
                          'id': error_tms_meta_mapping['event_type'][add_err_meta['event_type']]}
            source = {'name': add_err_meta['source'],
                      'id': error_tms_meta_mapping['source'][add_err_meta['source']]}

            add_err_tms_id = tms_retriever.create_timeseries_id(run_name=add_err_meta['run_name'], station=station,
                                                                variable=variable, unit=unit, event_type=event_type,
                                                                source=source)
            del station, variable, unit, event_type, source

        if len(mul_err_tms_id) <= 0:
            station = {'name': mul_err_meta['station_name'],
                       'id': error_tms_meta_mapping['station_name'][mul_err_meta['station_name']]}
            variable = {'name': mul_err_meta['variable'],
                        'id': error_tms_meta_mapping['variable'][mul_err_meta['variable']]}
            unit = {'name': mul_err_meta['unit'],
                    'id': error_tms_meta_mapping['unit'][mul_err_meta['unit']]}
            event_type = {'name': mul_err_meta['event_type'],
                          'id': error_tms_meta_mapping['event_type'][mul_err_meta['event_type']]}
            source = {'name': mul_err_meta['source'],
                      'id': error_tms_meta_mapping['source'][mul_err_meta['source']]}

            mul_err_tms_id = tms_retriever.create_timeseries_id(run_name=mul_err_meta['run_name'], station=station,
                                                                variable=variable, unit=unit, event_type=event_type,
                                                                source=source)
            del station, variable, unit, event_type, source

        tms_retriever.update_timeseries(add_err_tms_id[0], add_err_scl, is_forced)
        tms_retriever.update_timeseries(mul_err_tms_id[0], mul_err_scl, is_forced)
        return True, 'Successfully updated error scales for model: %s, quadrant: %s' % (model, quadrant)

    def _bias_correct(self, error_scale, frcst_matrix, fill, freq):
        pass

    def bias_correct(self, model, quadrant, start_datetime, end_datetime, frequency='H', is_forced=False):
        """

        :param quadrant:
        :param start_datetime:
        :param end_datetime:
        :param fill_stratergy:
        :param frequency:
        :return:
        """
        # calc/retrieve error scale and retrieve forecast timeseries convert to pandas dataframe,
        # then join them into matrix
        # then pass the error_scale and pd matrix to _bias_correct
        # get the ouptut, decode and store back in the db
        pass
