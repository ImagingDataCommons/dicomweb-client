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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Development Status :: 4 - Beta',
    ],
    entry_points={
        'console_scripts': ['dicomweb_client = dicomweb_client.cli:_main'],
    },
    include_package_data=True,
    packages=setuptools.find_packages('src'),
    package_dir={'': 'src'},
    setup_requires=[
        'pytest-runner>=3.0',
    ],
    extras_require={
        'docs': [
            'sphinx>=1.7.1',
            'sphinx-pyreverse>=0.0.12',
            'sphinxcontrib-autoprogram>=0.1.4',
            'sphinx_rtd_theme>=0.2.4'
        ],
        'gcp': [
            'google-auth>=1.6',
            'google-oauth>=1.0',
        ],
    },
    tests_require=[
        'mypy>=0.7',
        'pytest>=5.0',
        'pytest-localserver>=0.5',
        'pytest-flake8>=1.0',
    ],
    install_requires=[
        'pydicom>=1.0',
        'requests>=2.18',
    ]
)
