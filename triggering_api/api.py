import pytz
from datetime import datetime
from flask import request
from flask_api import status

from data_layer import models
from data_layer.constants import COMMON_DATETIME_FORMAT
from kub_observation_mean import KUBObservationMeanUpdator
from klb_bias_correction import KLBBiasCorrector, wrf_models, klb_quadrants

from .config import create_app


trig_api, db = create_app(models.db)


@trig_api.route('/')
def welcome():
    return 'Welcome to the data integration REST service!'


@trig_api.route('/trigger/update/kub_obs_mean')
def update_kub_obs_mean():
    """
    request params
    - from      date-time
    - to        date-time
    - fall_back boolean
    - force     boolean
    :return:
    """

    # "from" should not be None and should be in the valid format.
    start_datetime = request.args.get('from')
    try:
        start_datetime = datetime.strptime(start_datetime, COMMON_DATETIME_FORMAT)
    except (TypeError, ValueError):
        error_msg = 'Invalid start_date. Should be a valid date of "YYYY-mm-dd HH:MM:SS" format.'
        return error_msg, status.HTTP_400_BAD_REQUEST

    # "to" can be None, if None default to server current time in IST. If specified should be in the valid format.
    end_datetime = request.args.get('to')
    if end_datetime is None or end_datetime == '':
        end_datetime = datetime.now(tz=pytz.timezone('Asia/Colombo'))
    else:
        try:
            end_datetime = datetime.strptime(end_datetime, COMMON_DATETIME_FORMAT)
        except ValueError:
            error_msg = 'Invalid end_date. Should be a valid date of "YYYY-mm-dd HH:MM:SS" format.'
            return error_msg, status.HTTP_400_BAD_REQUEST

    # "from" should be < "to"
    if start_datetime >= end_datetime:
        error_msg = 'Invalid time period.'
        return error_msg, status.HTTP_400_BAD_REQUEST

    # "force" can be None. If specified even the KUB Mean Timeseries is already exists for the given time period,
    # will calculate and update the timeseries.
    force = request.args.get('force')
    force = type(force) is str and force.lower() == 'true'

    # "fall_back" can be None. If specified will fall back by removing from the set of considered stations
    # if a timeseries cannot be found for that particular station.
    fall_back = request.args.get('fall_back')
    fall_back = type(fall_back) is str and fall_back.lower() == 'true'

    kub_mean_updator = KUBObservationMeanUpdator(db)
    success, msg = kub_mean_updator.update_kub_mean(start_datetime, end_datetime, fall_back, force)

    return msg, status.HTTP_200_OK if success else status.HTTP_500_INTERNAL_SERVER_ERROR


@trig_api.route('/trigger/update/klb_error_scales')
def update_klb_error_scales():
    # "from" should not be None and should be in the valid format.
    start_datetime = request.args.get('from')
    try:
        start_datetime = datetime.strptime(start_datetime, COMMON_DATETIME_FORMAT)
    except (TypeError, ValueError):
        error_msg = 'Invalid start_date. Should be a valid date of "YYYY-mm-dd HH:MM:SS" format.'
        return error_msg, status.HTTP_400_BAD_REQUEST

    # "to" can be None, if None default to server current time in IST. If specified should be in the valid format.
    end_datetime = request.args.get('to')
    if end_datetime is None or end_datetime == '':
        end_datetime = datetime.now(tz=pytz.timezone('Asia/Colombo'))
    else:
        try:
            end_datetime = datetime.strptime(end_datetime, COMMON_DATETIME_FORMAT)
        except ValueError:
            error_msg = 'Invalid end_date. Should be a valid date of "YYYY-mm-dd HH:MM:SS" format.'
            return error_msg, status.HTTP_400_BAD_REQUEST

    # "from" should be < "to"
    if start_datetime >= end_datetime:
        error_msg = 'Invalid time period.'
        return error_msg, status.HTTP_400_BAD_REQUEST

    # "force" can be None. If specified even if the KLB error scales timeseries are already exists
    # for the given time period, will calculate and update the timeseries.
    force = request.args.get('force')
    force = type(force) is str and force.lower() == 'true'

    model = request.args.get('model')
    quadrant = request.args.get('quadrant')
    if model not in wrf_models:
        error_msg = 'Given model name is not a valid wrf model.'
        return error_msg, status.HTTP_400_BAD_REQUEST
    if quadrant not in klb_quadrants:
        error_msg = 'Given quadrant name is not a valid KLB quadrant'
        return error_msg, status.HTTP_400_BAD_REQUEST

    klb_bias_corrector = KLBBiasCorrector(db)
    success, msg = klb_bias_corrector.calc_error_scale(model, quadrant, start_datetime, end_datetime, 'H', force)
    return msg, status.HTTP_200_OK if success else status.HTTP_500_INTERNAL_SERVER_ERROR
