from flask import Flask


db_config = {
    'connector': 'pymysql',
    'host': "104.198.0.87",
    'port': 3306,
    'user': "curw_user",
    'password': "curw",
    'db': "curw"
}

sqlalchemy_config = {
    'db_url': "mysql+%s://%s:%s@%s:%d/%s" % (db_config['connector'], db_config['user'], db_config['password'],
                                             db_config['host'], db_config['port'], db_config['db'])
}


def create_app(db):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = sqlalchemy_config['db_url']
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    return app, db
