# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import io
import os.path
from setuptools import setup, find_packages


# Helpers
def read(*segments):
    path = os.path.join(*segments)
    with io.open(path, encoding='utf-8') as f:
        return f.read().strip()


# Prepare
PACKAGE = 'jsontableschema_pandas'
NAME = PACKAGE.replace('_', '-')
INSTALL_REQUIRES = [
    'six',
    'pandas',
    'tabulator',
    'jsontableschema>=0.6',
]
TESTS_REQUIRE = [
    'pylama',
    'tox',
]
README = read('README.md')
VERSION = read(PACKAGE, 'VERSION')
PACKAGES = find_packages(exclude=['tests'])


# Run
setup(
    name=NAME,
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    extras_require={'develop': TESTS_REQUIRE},
    zip_safe=False,
    long_description=README,
    description='Generate Pandas data frames, load and extract data, based on JSON Table Schema descriptors.',
    author='Open Knowledge Foundation',
    author_email='info@okfn.org',
    url='https://github.com/frictionlessdata/jsontableschema-pandas-py',
    license='LGPLv3+',
    keywords=['frictionless data', 'datapackage', 'pandas'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
