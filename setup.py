#!/usr/bin/env python

from setuptools import setup, find_packages

# Description
with open('README.md') as fd:
    long_description = fd.read()
#end with

# Requirements
with open('requirements.txt') as fr:
    set_parsed = fr.read().splitlines()
#end with

# Set requires
install_requires = [req.strip() for req in set_parsed]
tests_requires = [
    'pytest'
]

setup(
    name='wfl_utils',
    version=open('_version.py').readlines()[-1].split()[-1].strip("\"'"),
    description = 'Workflow utilities',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    author = 'Michele Berselli',
    author_email = 'berselli.michele@gmail.com',
    url='https://github.com/dbmi-bgm/cgap-wfl_utils',
    include_package_data=True,
    packages=find_packages(),
    classifiers=[
            'License :: OSI Approved :: MIT License',
            'Operating System :: POSIX :: Linux',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3'
            ],
    install_requires=install_requires,
    setup_requires=install_requires,
    tests_require=tests_requires,
    python_requires = '>=3.6',
    license = 'MIT'
)
