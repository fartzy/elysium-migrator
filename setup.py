import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="elysium-migration",
    version="0.1.0",
    author="Mike Artz",
    author_email="michael.artz@financesecurities.com",
    description="Elysium Migration CLI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    license="",
    packages=setuptools.find_packages(exclude=["tests", "tests.*",]),
    install_requires=["click>=6.7",],
    package_data={"": ["config.yaml"]},
    entry_points={
        "console_scripts": ["elysium-migrate-cli=elysium_migration.cmds.cli:cli",],
    },
    scripts=[
        "scripts/getenv.sh",
        "scripts/install.sh",
        "scripts/yb_checksum.sh",
        "scripts/run.sh",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
