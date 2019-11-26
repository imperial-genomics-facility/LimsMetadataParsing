import os
import sys
from setuptools import setup

tests_require = ["unittest2"]

base_dir = os.path.dirname(os.path.abspath(__file__))

version = "0.0.1"

setup(
    name = "igfLimsParsing",
    version = version,
    description = "A pyspark based codebase for fetching and formatting metadata from a LIMS db for IGF",
    url = "https://github.com/imperial-genomics-facility/LimsMetadataParsing",
    author = "IGF",
    author_email = "igf[at]imperial.ac.uk",
    maintainer = "IGF",
    maintainer_email = "igf[at]imperial.ac.uk",
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Testing',
    ],
    packages = ["igfLimsParsing"],
    zip_safe = True,
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