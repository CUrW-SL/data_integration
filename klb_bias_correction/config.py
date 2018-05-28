import numpy as np

wrf_models = ['wrf0', 'wrf1', 'wrf2', 'wrf3', 'wrf4', 'wrf5']
klb_qaudrants = ['met_col0', 'met_col1', 'met_col2', 'met_col3']
forecast_types = ['Forecast-0-d', 'Forecast-1-d-after', 'Forecast-2-d-after']


def get_frcst_mean_tms_meta(model, qaudrant, event_type):
    if model not in wrf_models:
        raise ValueError("Invalid model. Given model should be one of %r" % wrf_models)
    if qaudrant not in klb_qaudrants:
        raise ValueError("Invalid quadrant. Given qaudrant should be one of %r" % klb_qaudrants)
    if event_type not in forecast_types:
        raise ValueError("Invalid event_type. Given event_type should be one of %r" % event_type)
    return {
        'station_name': qaudrant,
        'run_name': 'Cloud-1',
        'variable': 'Precipitation',
        'unit': 'mm',
        'event_type': event_type,
        'source': model
    }


qaudrant_obs_tms_meta = {
    klb_qaudrants[0]: {'station_name': 'Kohuwela',
                       'run_name': 'A&T Labs',
                       'variable': 'Precipitation',
                       'unit': 'mm',
                       'event_type': 'Observed',
                       'source': 'WeatherStation'
                       },
    klb_qaudrants[1]: {'station_name': 'Malabe',
                       'run_name': 'A&T Labs',
                       'variable': 'Precipitation',
                       'unit': 'mm',
                       'event_type': 'Observed',
                       'source': 'WeatherStation'
                       },
    klb_qaudrants[2]: {'station_name': 'Mutwal',
                       'run_name': 'A&T Labs',
                       'variable': 'Precipitation',
                       'unit': 'mm',
                       'event_type': 'Observed',
                       'source': 'WeatherStation'
                       },
    klb_qaudrants[3]: {'station_name': 'Mulleriyawa',
                       'run_name': 'A&T Labs',
                       'variable': 'Precipitation',
                       'unit': 'mm',
                       'event_type': 'Observed',
                       'source': 'WeatherStation'
                       }
}


def get_add_error_tms_meta(model, qaudrant, event_type):
    if model not in wrf_models:
        raise ValueError("Invalid model. Given model should be one of %r" % wrf_models)
    if qaudrant not in klb_qaudrants:
        raise ValueError("Invalid quadrant. Given qaudrant should be one of %r" % klb_qaudrants)
    if event_type not in forecast_types:
        raise ValueError("Invalid event_type. Given event_type should be one of %r" % event_type)
    return {
        'station_name': qaudrant,
        'run_name': 'Mean Error Scale',
        'variable': 'Error',
        'unit': 'add',
        'event_type': event_type,
        'source': model
    }


def get_mul_error_tms_meta(model, qaudrant, event_type):
    if model not in wrf_models:
        raise ValueError("Invalid model. Given model should be one of %r" % wrf_models)
    if qaudrant not in klb_qaudrants:
        raise ValueError("Invalid quadrant. Given qaudrant should be one of %r" % klb_qaudrants)
    if event_type not in forecast_types:
        raise ValueError("Invalid event_type. Given event_type should be one of %r" % event_type)
    return {
        'station_name': klb_qaudrants[0],
        'run_name': 'Mean Error Scale',
        'variable': 'Error',
        'unit': 'multiply',
        'event_type': event_type,
        'source': wrf_models[0]
    }


tms_meta_mapping = {
    'station_name': {klb_qaudrants[0]: 100029,
                     klb_qaudrants[1]: 100030,
                     klb_qaudrants[2]: 100031,
                     klb_qaudrants[3]: 100032,
                     'wrf_79.848206_6.832657': '1103220',
                     'wrf_79.875435_6.832657': '1103221',
                     'wrf_79.902664_6.832657': '1103222',
                     'wrf_79.929893_6.832657': '1103223',
                     'wrf_79.957123_6.832657': '1103224',
                     'wrf_79.984352_6.832657': '1103225',
                     'wrf_79.848206_6.859688': '1103255',
                     'wrf_79.875435_6.859688': '1103256',
                     'wrf_79.902664_6.859688': '1103257',
                     'wrf_79.929893_6.859688': '1103258',
                     'wrf_79.957123_6.859688': '1103259',
                     'wrf_79.984352_6.859688': '1103260',
                     'wrf_79.848206_6.886719': '1103290',
                     'wrf_79.875435_6.886719': '1103291',
                     'wrf_79.902664_6.886719': '1103292',
                     'wrf_79.929893_6.886719': '1103293',
                     'wrf_79.957123_6.886719': '1103294',
                     'wrf_79.984352_6.886719': '1103295',
                     'wrf_79.848206_6.913757': '1103325',
                     'wrf_79.875435_6.913757': '1103326',
                     'wrf_79.902664_6.913757': '1103327',
                     'wrf_79.929893_6.913757': '1103328',
                     'wrf_79.957123_6.913757': '1103329',
                     'wrf_79.984352_6.913757': '1103330',
                     'wrf_79.848206_6.940788': '1103360',
                     'wrf_79.875435_6.940788': '1103361',
                     'wrf_79.902664_6.940788': '1103362',
                     'wrf_79.929893_6.940788': '1103363',
                     'wrf_79.957123_6.940788': '1103364',
                     'wrf_79.984352_6.940788': '1103365',
                     'wrf_79.848206_6.967812': '1103395',
                     'wrf_79.875435_6.967812': '1103396',
                     'wrf_79.902664_6.967812': '1103397',
                     'wrf_79.929893_6.967812': '1103398',
                     'wrf_79.957123_6.967812': '1103399',
                     'wrf_79.984352_6.967812': '1103400',
                     'wrf_79.848206_6.994843': '1103430',
                     'wrf_79.875435_6.994843': '1103431',
                     'wrf_79.902664_6.994843': '1103432',
                     'wrf_79.929893_6.994843': '1103433',
                     'wrf_79.957123_6.994843': '1103434',
                     'wrf_79.984352_6.994843': '1103435'},
    'variable': {'Error': 15,
                 'Precipitation': 1},
    'unit': {'mm': 1,
             'add': 16,
             'multiply': 17},
    'event_type': {'Observed': 1,
                   forecast_types[0]: 16,
                   forecast_types[1]: 17,
                   forecast_types[2]: 18},
    'source': {wrf_models[0]: 8,
               wrf_models[1]: 9,
               wrf_models[2]: 10,
               wrf_models[3]: 11,
               wrf_models[4]: 12,
               wrf_models[5]: 13}
}

klb_lats = [6.832657, 6.859688, 6.886719, 6.913757, 6.940788, 6.967812, 6.994843]
klb_lons = [79.848206, 79.875435, 79.902664, 79.929893, 79.957123, 79.984352]


def get_frcst_tms_meta(event_type, model, lon, lat):
    return {
        'station_name': 'wrf' + '_' + str(lon) + '_' + str(lat),
        'run_name': 'Cloud-1',
        'variable': 'Precipitation',
        'unit': 'mm',
        'event_type': event_type,
        'source': model
    }


def get_klb_frcst_tms_meta(event_type, model):
    matrix = []
    for lon in klb_lons:
        column = []
        for lat in klb_lats:
            column.append(get_frcst_tms_meta(event_type, model, lon, lat))
        matrix.append(column)
    return np.matrix(matrix)


qaudrant_frcst_tms_meta = {
    klb_qaudrants[0]: lambda event_type, model: get_klb_frcst_tms_meta(event_type, model)[0:4, 0:3],
    klb_qaudrants[1]: lambda event_type, model: get_klb_frcst_tms_meta(event_type, model)[0:4, 4:7],
    klb_qaudrants[2]: lambda event_type, model: get_klb_frcst_tms_meta(event_type, model)[4:8, 0:3],
    klb_qaudrants[3]: lambda event_type, model: get_klb_frcst_tms_meta(event_type, model)[4:8, 4:7]
}
