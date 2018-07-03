from setuptools import setup

setup(
    name="Matrix",

    packages=["matrix"],
    scripts=["bin/matrix"],

    use_scm_version=True,
    setup_requires=['setuptools_scm'],

    install_requires=[
        "Click",
        "logbook",
        "attrdict",
        "pyyaml",
        "aioamqp",
        "blessings"
    ],

    description="An modeling framework for social agent simuation.",
)
