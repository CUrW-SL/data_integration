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
    requires=['numpy', 'geopandas', 'scipy', 'shapely', 'curw'],
    install_requires=['curw'],
    dependency_links=['https://github.com/nirandaperera/models/tarball/v2.0.0-snapshot-dev#egg=curw-2.0.0-snapshot'],
    zip_safe=False
)
