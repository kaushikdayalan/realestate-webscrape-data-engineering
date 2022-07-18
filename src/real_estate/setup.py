import setuptools

setuptools.setup(
    name="real_estate",
    packages=setuptools.find_packages(exclude=["real_estate_tests"]),
    install_requires=[
        "dagster==0.15.6",
        "dagit==0.15.6",
        "pytest",
    ],
)
