#!/usr/bin/env python3
"""
Setup script for PySpark Coding Challenge
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="pyspark-coding-challenge",
    version="1.0.0",
    author="Berkant Mangir",
    description="PySpark pipeline for transformer model training data preparation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/berkantmangir/pyspark-coding-challenge",
    project_urls={
        "Bug Tracker": "https://github.com/berkantmangir/pyspark-coding-challenge/issues",
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Data Scientists",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.5.0,<4.0.0",
        "numpy>=1.20.0",
        "pandas>=1.3.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0"
        ],
        "test": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "pyspark-training-pipeline=src.pipeline:main",
        ],
    },
    include_package_data=True,
    package_data={
        "src": ["data/*.json"],
    },
    zip_safe=False,
)

