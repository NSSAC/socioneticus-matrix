from setuptools import setup

setup(
    name="Matrix",
    version="0.0.7",

    packages=["matrix"],

    install_requires=[
        "Click",
        "logbook",
        "attrdict",
        "pyyaml"
    ],

    entry_points="""
        [console_scripts]
        matrix=matrix:cli
    """,

    description="An modeling framework for social agent simuation.",
)
