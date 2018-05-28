import numpy as np
import pandas as pd

from data_layer.retrieval import Timeseries
from data_layer.exceptions import InconsistencyError

from .config import qaudrant_obs_tms_meta, get_frcst_mean_tms_meta, forecast_types
from .config import get_add_error_tms_meta, get_mul_error_tms_meta, tms_meta_mapping
from .config import qaudrant_frcst_tms_meta


class KLBBiasCorrector:
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
        forecast_mean = forecast_mean.apply(pd.to_numeric, errors='coerce')
        obs_mean = obs_mean.resample(freq).sum()
        obs_mean = obs_mean.apply(pd.to_numeric, errors='coerce')

        diff_error = obs_mean.subtract(forecast_mean, fill_value=0.0, axis='columns')
        diff_error.index.name = 'time'

        div_error = obs_mean.divide(forecast_mean, axis='columns')
        div_error.replace([np.inf, -np.inf], np.nan, inplace=True)
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
        frcst_tms_meta = get_frcst_mean_tms_meta(model, quadrant, forecast_types[0])
        obs_tms_meta = qaudrant_obs_tms_meta[quadrant]
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
        add_err_meta = get_add_error_tms_meta(model, quadrant, forecast_types[0])
        mul_err_meta = get_mul_error_tms_meta(model, quadrant, forecast_types[0])

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
                       'id': tms_meta_mapping['station_name'][add_err_meta['station_name']]}
            variable = {'name': add_err_meta['variable'],
                        'id': tms_meta_mapping['variable'][add_err_meta['variable']]}
            unit = {'name': add_err_meta['unit'],
                    'id': tms_meta_mapping['unit'][add_err_meta['unit']]}
            event_type = {'name': add_err_meta['event_type'],
                          'id': tms_meta_mapping['event_type'][add_err_meta['event_type']]}
            source = {'name': add_err_meta['source'],
                      'id': tms_meta_mapping['source'][add_err_meta['source']]}

            add_err_tms_id = tms_retriever.create_timeseries_id(run_name=add_err_meta['run_name'], station=station,
                                                                variable=variable, unit=unit, event_type=event_type,
                                                                source=source)
            del station, variable, unit, event_type, source

        if len(mul_err_tms_id) <= 0:
            station = {'name': mul_err_meta['station_name'],
                       'id': tms_meta_mapping['station_name'][mul_err_meta['station_name']]}
            variable = {'name': mul_err_meta['variable'],
                        'id': tms_meta_mapping['variable'][mul_err_meta['variable']]}
            unit = {'name': mul_err_meta['unit'],
                    'id': tms_meta_mapping['unit'][mul_err_meta['unit']]}
            event_type = {'name': mul_err_meta['event_type'],
                          'id': tms_meta_mapping['event_type'][mul_err_meta['event_type']]}
            source = {'name': mul_err_meta['source'],
                      'id': tms_meta_mapping['source'][mul_err_meta['source']]}

            mul_err_tms_id = tms_retriever.create_timeseries_id(run_name=mul_err_meta['run_name'], station=station,
                                                                variable=variable, unit=unit, event_type=event_type,
                                                                source=source)
            del station, variable, unit, event_type, source

        tms_retriever.update_timeseries(add_err_tms_id[0], add_err_scl, is_forced)
        tms_retriever.update_timeseries(mul_err_tms_id[0], mul_err_scl, is_forced)
        return True, 'Successfully updated error scales for model: %s, quadrant: %s' % (model, quadrant)

    def _bias_correct(self, add_error_scale, mul_error_scale, frcst_matrix, freq):
        frcst_matrix = frcst_matrix.resample(freq).sum()
        frcst_matrix = frcst_matrix.astype(np.float)

        # Checks the frequency of error scales are of the same as given freq.
        add_error_freq = pd.infer_freq(add_error_scale.index, warn=True)
        mul_error_freq = pd.infer_freq(mul_error_scale.index, warn=True)
        if add_error_freq != freq:
            raise ValueError("Error scale frequency is not same as the passed freq: %s" % freq, add_error_freq)
        if mul_error_freq != freq:
            raise ValueError("Error scale frequency is not same as the passed freq: %s" % freq, mul_error_freq)

        # blancket corrector function
        def blanket_corrector(series, err_scl):
            try:
                add_err = float(err_scl.at[series.name, 'value'])
            except Exception as ex:
                print(ex)
                return series

            bring_forward = 0.0
            corr_vals = []
            keys = []
            for index, value in series.iteritems():
                correction = value + (bring_forward + add_err)
                if correction >= 0:
                    corr_vals.append(correction)
                    keys.append(index)
                    bring_forward = 0.0
                else:
                    corr_vals.append(0.0)
                    keys.append(index)
                    bring_forward = correction
            return pd.Series(data=corr_vals, index=keys, dtype=np.float).rename(series.name)

        # multiplier corrector function
        def multiplier_corrector(series, err_scl):
            try:
                mul_err = float(err_scl.at[series.name, 'value'])
            except Exception as ex:
                print(ex)
                return series

            corr_vals = []
            keys = []
            for index, value in series.iteritems():
                correction = value * mul_err
                if correction >= 0:
                    corr_vals.append(correction)
                    keys.append(index)
                else:
                    corr_vals.append(value)
                    keys.append(index)
            return pd.Series(data=corr_vals, index=keys, dtype=np.float).rename(series.name)

        return frcst_matrix.apply(blanket_corrector,
                                  axis='columns',
                                  raw=False,
                                  err_scl=add_error_scale)

    def bias_correct(self, model, quadrant, start_datetime, end_datetime, frequency='H', is_forced=False):
        """
        :param model: specifies the interested wrf model [wrf0, wrf1, wrf2, wrf3, wrf4, wrf5]
        :param quadrant: klb quadrant [met_col0, met_col1, met_col2, met_col3]
        :param start_datetime: python datetime
        :param end_datetime: python datetime
        :param frequency: pandas compatible resampling frequency
        :param is_forced: specifies the whether to overwrite or not the error scales in the db existing
        :return:
        """
        # calc/retrieve error scale and retrieve forecast timeseries convert to pandas dataframe,
        # then join them into matrix
        # then pass the error_scale and pd matrix to _bias_correct
        # get the ouptut, decode and store back in the db

        tms_retriever = Timeseries(self.db)

        # Retrieve error scales.
        add_err_meta = get_add_error_tms_meta(model, quadrant, forecast_types[0])
        mul_err_meta = get_mul_error_tms_meta(model, quadrant, forecast_types[0])
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
        if len(add_err_tms_id) <= 0 or len(mul_err_tms_id) <= 0:
            return False, 'No Error Scales found for model: %s, quadrant: %s' % (model, quadrant)

        add_err_tms = tms_retriever.get_timeseries(add_err_tms_id, start_datetime, end_datetime)
        mul_err_tms = tms_retriever.get_timeseries(mul_err_tms_id, start_datetime, end_datetime)

        # Error scales should have at least one entry to bias correct the forecast.
        if len(add_err_tms) < 1 or len(mul_err_tms) < 1:
            return False, 'No Values found in the error scales for model: %s, quadrant: %s' % (model, quadrant)

        add_err_tms_df = pd.DataFrame(data=add_err_tms, columns=['time', 'value']).set_index(keys='time')
        mul_err_tms_df = pd.DataFrame(data=mul_err_tms, columns=['time', 'value']).set_index(keys='time')

        # Retrieve 'timeseries' and compose them into one pd dataframe matrix.
        tms_meta_matrix = qaudrant_frcst_tms_meta[quadrant](forecast_types[0], model)
        row_count, col_count = tms_meta_matrix.shape
        tms_list = []
        for row in range(0, row_count):
            for col in range(0, col_count):
                tms_meta = tms_meta_matrix[row, col]
                tms_id = tms_retriever.get_timeseries_id(run_name=tms_meta['run_name'],
                                                         station_name=tms_meta['station_name'],
                                                         variable=tms_meta['variable'],
                                                         unit=tms_meta['unit'],
                                                         event_type=tms_meta['event_type'],
                                                         source=tms_meta['source'])
                value_header = str(row) + '_' + str(col)
                df = pd.DataFrame(
                    data=tms_retriever.get_timeseries(tms_id, start_datetime, end_datetime),
                    columns=['time', value_header]).set_index(keys='time')
                tms_list.append(df)
        if len(tms_list) <= 0:
            return False, 'No forecast time series found for given model: %s, qaudrant: %s.' % (model, quadrant)
        elif len(tms_list) == 1:
            tms_df = tms_list[0]
        else:
            tms_df = tms_list[0].join(tms_list[1:])

        corrected_tms_df = self._bias_correct(add_err_tms_df, mul_err_tms_df, tms_df, frequency)

        for col_name in corrected_tms_df.columns.values:
            corrected_tms = corrected_tms_df[col_name].to_frame('value')
            meta_index = col_name.split('_')
            tms_meta = tms_meta_matrix[int(meta_index[0]), int(meta_index[1])]
            tms_meta['run_name'] = 'Mean Magnitude Corrected'

            # Save timeseries.
            tms_id = tms_retriever.get_timeseries_id(run_name=tms_meta['run_name'],
                                                     station_name=tms_meta['station_name'],
                                                     variable=tms_meta['variable'],
                                                     unit=tms_meta['unit'],
                                                     event_type=tms_meta['event_type'],
                                                     source=tms_meta['source'])
            if len(tms_id) > 1:
                return False, 'More than one timeseries exist for timeseries meta: %r' % tms_meta
            if len(tms_id) <= 0:
                station = {'name': tms_meta['station_name'],
                           'id': tms_meta_mapping['station_name'][tms_meta['station_name']]}
                variable = {'name': tms_meta['variable'],
                            'id': tms_meta_mapping['variable'][tms_meta['variable']]}
                unit = {'name': tms_meta['unit'],
                        'id': tms_meta_mapping['unit'][tms_meta['unit']]}
                source = {'name': tms_meta['source'],
                          'id': tms_meta_mapping['source'][tms_meta['source']]}
                event = {'name': tms_meta['event_type'],
                         'id': tms_meta_mapping['event_type'][tms_meta['event_type']]}
                tms_id = tms_retriever.create_timeseries_id(run_name=tms_meta['run_name'],
                                                            station=station,
                                                            variable=variable,
                                                            unit=unit,
                                                            source=source,
                                                            event_type=event)
                del station, variable, unit, source, event
            tms_retriever.update_timeseries(tms_id[0], corrected_tms, is_forced)

        return True, 'Successfully corrected forecast of model: %s, qaudrant: %s' % (model, quadrant)
