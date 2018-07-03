from setuptools import setup

setup(
    name="Matrix",

    packages=["matrix"],

    use_scm_version=True,
    setup_requires=['setuptools_scm'],

    install_requires=[
        "Click",
        "logbook",
        "attrdict",
        "pyyaml",
        "aioamqp",
    ],

    entry_points="""
        [console_scripts]
        matrix=matrix:cli
    """,

    description="An modeling framework for social agent simuation.",
)
