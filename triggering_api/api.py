import pytz
from datetime import datetime
from flask import request
from flask_api import status

from data_layer import models
from data_layer.constants import COMMON_DATETIME_FORMAT
from kub_observation_mean import KUBObservationMeanUpdator

from .config import create_app


app, db = create_app(models.db)


@app.route('/')
def welcome():
    return 'Welcome to the data integration REST service!'


@app.route('/trigger/update/kub_obs_mean')
def update_kub_obs_mean():

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

    kub_mean_updator = KUBObservationMeanUpdator()
    success, msg = kub_mean_updator.update_kub_mean(start_datetime, end_datetime, force)

    return msg, status.HTTP_200_OK if success else status.HTTP_500_INTERNAL_SERVER_ERROR
