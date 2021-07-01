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
    name='magma-suite',
    version=open('_version.py').readlines()[-1].split()[-1].strip("\"'"),
    description = 'Collection of tools to manage meta-workflows automation',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    author = 'Michele Berselli, Soo Lee',
    author_email = 'berselli.michele@gmail.com',
    url='https://github.com/dbmi-bgm/magma',
    include_package_data=True,
    packages=['magma', 'magma_ff'],
    classifiers=[
            'License :: OSI Approved :: MIT License',
            'Operating System :: POSIX :: Linux',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3'
            ],
    install_requires=install_requires,
    setup_requires=install_requires,
    tests_require=tests_requires,
    python_requires = '>=3.6, <3.8',
    license = 'MIT'
)
