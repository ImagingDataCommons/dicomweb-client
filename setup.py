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
    url='https://github.com/clindatsci/dicomweb-client',
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
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Development Status :: 4 - Beta',
    ],
    entry_points={
        'console_scripts': ['dicomweb_client = dicomweb_client.cli:main'],
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
        ]
    },
    tests_require=[
        'pytest>=3.3',
        'pytest-localserver>=0.4',
        'pytest-flake8>=0.9',
        'tox>=2.9'
    ],
    install_requires=[
        'numpy>=1.13',
        'pillow>=5.0',
        'pydicom>=1.0',
        'requests>=2.18',
        'six>=1.11'
    ]
)
