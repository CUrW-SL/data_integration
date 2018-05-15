### Dependencies:
- pip install uwsgi
- pip install Flask
- pip install Flask-API
- pip install flask-sqlalchemy
- pip install pytz

#### How to run server
- python -m flask run
or
- uwsgi --ini triggering_api.ini


#### Note
should install data_integration in editable mode to make server works.
pip install -e <abs_path_to data_integration>
Find the a better way for this.