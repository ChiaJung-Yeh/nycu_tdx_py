from setuptools import setup, find_packages


setup(
    name='nycu_tdx_py',
    version='0.1.11',
    license='NYCU',
    author="Robert Yeh",
    author_email='robert1328.mg10@nycu.edu.tw',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/ChiaJung-Yeh/nycu_tdx_py',
    keywords='example project',
    install_requires=[
          'pandas',
          'geopandas',
          'numpy',
          'shapely',
          'requests'
      ],

)
