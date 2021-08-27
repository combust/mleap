#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
from mleap.version import version
import sys
from setuptools import setup, find_packages

if sys.version_info < (2, 7):
    print("Python versions prior to 2.7 are not supported for pip installed MLeap.",
          file=sys.stderr)
    exit(-1)

try:
    exec(open('mleap/version.py').read())
except IOError:
    print("Failed to load MLeap version file for packaging. You must be in MLeap's python directory.",
          file=sys.stderr)
    sys.exit(-1)

VERSION = version

numpy_version = "1.8.2"

REQUIRED_PACKAGES = [
      'numpy >= %s' % numpy_version,
      'six >= 1.10.0',
      'scipy>=0.13.0b1',
      'pandas>=0.18.1',
      'scikit-learn>=0.22.0,<0.23.0',
]

TESTS_REQUIRED_PACKAGES = [
      'nose-exclude>=0.5.0'
]

setup(name='mleap',
      version=VERSION,
      description='MLeap Python API',
      author='MLeap Developers',
      author_email='combust@combust.ml',
      url='https://github.com/combust/mleap/tree/master/python',
      packages=find_packages(),
      zip_safe=False,
      install_requires=REQUIRED_PACKAGES,
      tests_require=TESTS_REQUIRED_PACKAGES,
      classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Communications :: Chat',
        'Topic :: Internet',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
     )
