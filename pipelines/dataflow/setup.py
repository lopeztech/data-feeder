"""Setup for the Dataflow Flex Template pipeline package."""

from setuptools import setup, find_packages

setup(
    name='data-feeder-loader',
    version='1.0.0',
    description='Silver-to-Gold BigQuery loader Dataflow pipeline',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]>=2.61.0',
        'google-cloud-bigquery>=3.25.0',
        'google-cloud-firestore>=2.19.0',
        'google-cloud-pubsub>=2.23.0',
        'google-cloud-storage>=2.18.0',
        'pyarrow>=17.0.0',
    ],
)
