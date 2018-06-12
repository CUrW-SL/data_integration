from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_layer.base import get_sessionmaker, get_engine
from data_layer.puller import Extractor
from data_layer.pusher import Pusher

from data_integration.config import iot_db, curw_db

tms_meta = {
    'station_name': 'Kohuwala',
    'run_name': 'CUrW Station',
    'variable': 'Precipitation',
    'event_type': 'Observed',
    'unit': 'mm',
    'source': 'WeatherStation'
}

curw_db_engine = get_engine(host=curw_db['host'], port=curw_db['port'], user=curw_db['user'],
                            password=curw_db['password'], db=curw_db['db'])
curw_db_session_maker = get_sessionmaker(curw_db_engine)

iot_db_engine = get_engine(host=iot_db['host'], port=iot_db['port'], user=iot_db['user'],
                           password=iot_db['password'], db=iot_db['db'])
iot_db_session_maker = get_sessionmaker(iot_db_engine)


def iot_to_curw_precipitation(meta_data_iot, meta_data_curw, start_dt, end_dt):
    extractor = Extractor(iot_db_session_maker)
    tms = extractor.pull_timeseries(meta_data_iot, start_dt, end_dt)
    tms_processed = process_precipitation(tms)
    if tms_processed is None or tms_processed.empty:
        return True
    pusher = Pusher(curw_db_session_maker)
    pusher.push_timeseries(meta_data_curw, tms_processed, True)
    return True


def process_precipitation(tms_df):
    if tms_df is None or tms_df.empty:
        return None
    tms_df = tms_df.resample('5T').max()
    tms_df.dropna(axis='index', how='any', inplace=True)
    tms_df = tms_df.diff(axis='index', periods=1)
    tms_df.dropna(axis='index', how='any', inplace=True)
    return tms_df


def main(meta_data_iot, meta_data_curw, **kwargs):
    end_dt = kwargs['execution_date']
    start_dt = end_dt - timedelta(minutes=20)
    success = iot_to_curw_precipitation(meta_data_iot, meta_data_curw, start_dt, end_dt)
    return success


default_args = {
    'owner': 'flolas',
    'depends_on_past': True,
    'start_date': datetime(year=2018, month=6, day=11, hour=10),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2018, 6, 12),
    }

dag = DAG('iot_to_curw', default_args=default_args, schedule_interval=timedelta(minutes=5))

t1 = PythonOperator(
        task_id='precipitation_5min_instantaneous',
        python_callable=main,
        provide_context=True,
        op_kwargs={'meta_data_iot': tms_meta,
                   'meta_data_curw': tms_meta},
        dag=dag)

