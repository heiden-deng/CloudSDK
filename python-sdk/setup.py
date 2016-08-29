from distutils.core import setup
import sys

packages = [
    'speedycloud',
    'speedycloud.products',
    'speedycloud.object_storage',
]

if sys.version_info < (2, 6):
    error = 'ERROR: speedycloud-sdk requires Python Version 2.6 or above.'
    print >> sys.stderr, error
    sys.exit(1)

setup(
    name='speedycloud-sdk',
    version='1.0',
    packages=packages
)
