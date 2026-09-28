from setuptools import setup, find_packages

setup(
    name="aetherdb",
    version="0.1.0",
    description="Unified Python Client SDK for AetherDB (KV + Vector + Atomic State)",
    author="AetherDB Team",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
