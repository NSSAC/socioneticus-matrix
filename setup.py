from setuptools import setup

setup(
    name="SocialSim",
    version="0.0.1",

    packages=["socialsim"],

    install_requires=[
        "Click",
        "logbook",
        "gevent",
        "json-rpc"
    ],

    entry_points="""
        [console_scripts]
        socialsim=socialsim:cli
    """,

    description="An modeling framework for social agent simuation.",
)
