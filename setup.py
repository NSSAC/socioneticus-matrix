from setuptools import setup

setup(
    name="Matrix",
    maintainer="Parantapa Bhattacharya",
    maintainer_email="paran@vt.edu",

    packages=["matrix", "matrix.client"],
    scripts=["bin/matrix", "bin/bluepill"],

    use_scm_version=True,
    setup_requires=['setuptools_scm'],

    install_requires=[
        "Click",
        "logbook",
        "attrdict",
        "pyyaml",
        "aioamqp",
        "blessings",
        "more-itertools",
        "qz7-logbook",
    ],

    description="An modeling framework for social agent simuation.",
    url="https://ndsslgit.vbi.vt.edu/social-sim-darpa/matrix",
)
