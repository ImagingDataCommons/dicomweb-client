#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
import re

import setuptools

with io.open('src/dicomweb_client/__init__.py', 'rt', encoding='utf8') as f:
    version = re.search(r'__version__ = \'(.*?)\'', f.read()).group(1)


setuptools.setup(
    name='dicomweb-client',
    version=version,
    description='Client for DICOMweb RESTful services.',
    author='Markus D. Herrmann',
    maintainer='Markus D. Herrmann',
    url='https://github.com/mghcomputationalpathology/dicomweb-client',
    license='MIT',
    platforms=['Linux', 'MacOS', 'Windows'],
    classifiers=[
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Intended Audience :: Science/Research',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Multimedia :: Graphics',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 4 - Beta',
    ],
    entry_points={
        'console_scripts': ['dicomweb_client = dicomweb_client.cli:_main'],
    },
    include_package_data=True,
    packages=setuptools.find_packages('src'),
    package_dir={'': 'src'},
    extras_require={
        'gcp': [
            'google-auth>=1.6',
            'google-oauth>=1.0',
        ],
    },
    python_requires='>=3.6',
    install_requires=[
        'pydicom>=2.0',
        'requests>=2.18',
        'retrying>=1.3.3',
    ]
)
