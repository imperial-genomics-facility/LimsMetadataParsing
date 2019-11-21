import os
import sys
from setuptools import setup

tests_require = ["unittest2"]

base_dir = os.path.dirname(os.path.abspath(__file__))

version = "0.0.1"

setup(
    name = "igfLims",
    version = version,
    description = "A pyspark based codebase for fetching and formatting metadata from a LIMS db for IGF",
    url = "https://github.com/imperial-genomics-facility/LimsMetadataParsing",
    author = "Avik Datta",
    author_email = "reach4avik@yahoo.com",
    maintainer = "Avik Datta",
    maintainer_email = "reach4avik@yahoo.com",
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Testing',
    ],
    packages = ["igfLims"],
    zip_safe = False,
    tests_require = tests_require,
    test_suite = "test.get_tests",
    license='Apache License 2.0',
    install_requires=[
        'setuptools',
        'pyarrow',
        'pyspark',
        'pyodbc',
        'JPype1',
        'JayDeBeApi',
        'pandas',
        'xlrd',
      ],
)