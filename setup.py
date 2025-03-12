# setup.py for de_lib
from setuptools import setup, find_packages

setup(
    name='de_lib',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'scikit-learn',
        'minio',
        'kaggle',
        'python-dotenv'
    ]
)