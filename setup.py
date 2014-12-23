#!/usr/bin/env python

# To upload a version to PyPI, run:
#
#    python setup.py sdist upload
#
# If the package is not registered with PyPI yet, do so with:
#
# python setup.py register

from distutils.core import setup
import sys
import os

VERSION = '0.1.0'

# Auto generate a __version__ package for the package to import
with open(os.path.join('h5scripting', '__version__.py'), 'w') as f:
    f.write("__version__ = '%s'\n"%VERSION)

setup(name='h5scripting',
      version=VERSION,
      description='An interface for saving python functions to an h5 file and allowing these to be executed in a nice way',
      author='Ian Spielman',
      author_email='spielman@umd.edu',
      url='https://bitbucket.org/cbillington/h5scripting',
      license="BSD",
      packages=['h5scripting']
     )
