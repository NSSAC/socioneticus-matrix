from setuptools import setup

setup(
    name="Matrix",
    version="0.0.5",

    packages=["matrix"],

    install_requires=[
        "Click",
        "logbook",
        "gevent",
        "json-rpc"
    ],

    entry_points="""
        [console_scripts]
        matrix=matrix:cli
    """,

    description="An modeling framework for social agent simuation.",
)
