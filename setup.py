from setuptools import setup, find_packages
import os


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='nycu_tdx_py',
    version='0.1.52',
    description='Python library for connecting to TDX',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT',
    author="Chia-Jung (Robert) Yeh",
    author_email='chia-jung.yeh@sydney.edu.au',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/ChiaJung-Yeh/nycu_tdx_py',
    keywords='tdx transport',
    install_requires=[
          'pandas',
          'geopandas',
          'numpy',
          'shapely',
          'requests',
          'tqdm'
      ],
)
