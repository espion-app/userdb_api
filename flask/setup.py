"""
Espion DB API Example
---------------------

A working example of Espion's external database API.
"""
from setuptools import setup
import os

version = '0.1.7'

setup(
    name='Espion_DB_API',
    version=version,
    url='http://espion.io/',
    license='MIT',
    author='Stratalis, Julien Demoor',
    author_email='jdemoor@stratalis.net',
    description='A working example of Espion\'s external database API.',
    long_description=__doc__,
    packages=['espion_db_api'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'Flask==0.10.1',
        'SQLAlchemy',
        'psycopg2',
        'python-dateutil==2.4.2',
        'Flask_Suite==0.0.3',
        'raven<6',
    ],
    setup_requires = [
        'setuptools'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

