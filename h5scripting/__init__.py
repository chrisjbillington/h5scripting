# -*- coding: utf-8 -*-
"""
Created on Wed Nov 12 12:02:51 2014

@author: ispielma
"""

try:
    from .__version__ import __version__
except ImportError:
    __version__ = None

from .h5scripting import *
