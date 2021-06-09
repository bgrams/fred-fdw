from setuptools import setup, find_packages


with open("requirements.txt", "r") as f:
    install_requires = f.read()

setup(
    name="fred-fdw",
    version="0.1.0",
    description="FRED® Foreign Data Wrapper",
    long_description_content_type="text/markdown",
    url="https://github.com/bgrams/fred-fdw",
    author="Brandon Grams",
    license="MIT",
    python_requires=">=3.6",
    packages=find_packages(include=["fred_fdw"]),
    install_requires=install_requires,
)