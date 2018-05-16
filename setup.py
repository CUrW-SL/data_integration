from setuptools import setup, find_packages

setup(
    name='data_integration',
    version='0.0.1',
    packages=find_packages(exclude=['triggering_api']),
    url='http://www.curwsl.org',
    license='MIT',
    author='thilinamad',
    author_email='madumalt@gamil.com',
    description='Data integration system for CUrW project managed under Mega Polis Ministry, Sri Lanka',
    include_package_data=True,
    install_requires=['uwsgi >= 2.0.17, < 3.0.0',
                      'Flask >= 1.0.2, < 2.0.0',
                      'Flask-API >= 1.0, < 2.0',
                      'pymysql >= 0.8.1, < 1.0.0',
                      'flask-sqlalchemy >= 2.3.2, < 3.0.0',
                      'pytz',
                      'pandas >= 0.22.0, < 1.0.0',
                      'numpy >= 1.14.3, < 2.0.0',
                      'geopandas >= 0.3.0, < 1.0.0',
                      'scipy >= 1.1.0, <2.0.0',
                      'shapely >= 1.6.4.post1, < 2.0.0',
                      'curw'],
    dependency_links=['https://github.com/nirandaperera/models/tarball/v2.0.0-snapshot-dev#egg=curw-2.0.0-snapshot'],
    zip_safe=False
)
