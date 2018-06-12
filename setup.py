from setuptools import setup, find_packages

setup(
    name='data_integration',
    version='1.0.0',
    packages=find_packages(),
    url='http://www.curwsl.org',
    license='MIT',
    author='thilinamad',
    author_email='madumalt@gamil.com',
    description='Weather Data integration Framework for CUrW project managed under Mega Polis Ministry, Sri Lanka',
    include_package_data=True,
    install_requires=['pandas',
                      'numpy',
                      'data_layer'],
    dependency_links=['https://github.com/CUrW-SL/data_layer/tarball/master#egg=data_layer-0.0.1'],
    zip_safe=False
)
