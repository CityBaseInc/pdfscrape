#!/usr/bin/env python

import sys
from os.path import exists
from setuptools import setup, find_packages


setup(name='pdfscrape',
      version='0.0.1',
      description='This package scrapes pdf documents.',
      author='Vidal Anguiano Jr.',
      url='ssh://github.com/CityBaseInc/pdfscrape',
      maintainer_email='vanguiano@thecitybase.com',
      packages=['pdfscrape'],
      long_description=open('README.md').read() if exists('README.md') else ''
      )
