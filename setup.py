from setuptools import setup, find_packages
import os


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='nycu_tdx_py',
    version='0.1.30',
    description='Python library for connecting to TDX',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT',
    author="Robert Yeh",
    author_email='robert1328.mg10@nycu.edu.tw',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/ChiaJung-Yeh/nycu_tdx_py',
    keywords='tdx transport',
    install_requires=[
          'pandas>=2.0.1',
          'geopandas>=0.12.2',
          'numpy>=1.23.5',
          'shapely>=2.0.1',
          'requests>=2.29.0',
          'tqdm>=4.64.1'
      ],
)
