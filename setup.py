#!/usr/bin/env python

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(name='pysparkutil',
      version='1.0',
      description='Pyspark Utilities for schema',
      author='Sreeram',
      author_email='sreeram@accenturefederal.com',
      url='',
      packages=find_packages(include=['pysparkutil', 'pysparkutil.*']),
      install_requires=[
          'avro==1.11.0'
      ],
     )
