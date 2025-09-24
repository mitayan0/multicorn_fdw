from setuptools import setup, find_packages

setup(
    name='multicorn_fdw',
    version='0.0.1',
    author='Mitayan Chakma',
    description='A PostgreSQL Foreign Data Wrapper built with Multicorn.',
    license='Postgresql',
    packages=find_packages(),
    install_requires=[
        'multicorn',
        'requests',
        'python-dateutil'
    ],
    python_requires=">=3.10"
)