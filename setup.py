from setuptools import find_packages, setup

setup(
    name="weather_package",
    version="0.1.0",
    description="Utilities for the Australia weather dataset.",
    packages=find_packages(),
    install_requires=["pandas"],
    python_requires=">=3.8",
)
