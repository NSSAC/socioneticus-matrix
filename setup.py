from setuptools import setup

setup(
    name="socioneticus-matrix",
    description="An modeling framework for social agent simuation.",

    author="Parantapa Bhattacharya",
    author_email="parantapa@virginia.edu",


    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",

    packages=["matrix", "matrix.client"],
    scripts=["bin/matrix", "bin/bluepill"],

    use_scm_version=True,
    setup_requires=['setuptools_scm'],

    install_requires=[
        "click",
        "logbook",
        "attrdict",
        "pyyaml",
        "aioamqp",
        "more-itertools",
        "sortedcontainers"
    ],

    url="http://github.com/NSSAC/socioneticus-matrix",
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
