from setuptools import setup, find_packages

setup(
    name="endpoints",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "polygon-api-client>=1.12.4",
        "clickhouse-connect>=0.7.0",
        "python-dotenv>=1.0.0",
        "asyncio>=3.4.3"
    ]
) 