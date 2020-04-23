import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="glueetl",
    version="0.0.1",
    scripts=['glueetl'],
    author="Son Nguyen",
    license="MIT",
    author_email="ngocson2vn@gmail.com",
    description="A command line tool to help deploy AWS Glue Jobs at ease :)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ngocson2vn/glueetl",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "boto3"
    ]
)