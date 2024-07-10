from setuptools import find_packages, setup

setup(
    name="project_green",
    packages=find_packages(exclude=["project_green_tests"]),
    install_requires=[
        "dagster",
        "pandas",
        "psycopg2",
        "datetime"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
