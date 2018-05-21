# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

# project structure https://github.com/pypa/sampleproject/blob/master/setup.py
# packaging         https://packaging.python.org/tutorials/distributing-packages/#choosing-a-versioning-scheme
# package testing   https://packaging.python.org/guides/using-testpypi/#using-test-pypi


here = path.abspath(path.dirname(__file__))
print(here)
print(find_packages(exclude=['tests', 'sample']))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# To create a dist from a wheel run:  $ python setup.py bdist_wheel
# To create a dist from source  run:  $ python setup.py sdist
# To upload it to PyPi run: $ twine upload dist/*

# For the test PyPi
# twine upload --repository-url https://test.pypi.org/legacy/ dist/*
# pip install --index-url https://test.pypi.org/simple/ condu
#
setup(
    # ==============================================================
    # This is the name of this project. It will determine how
    # users can install this project, e.g.: $ pip install condu
    # https://packaging.python.org/specifications/core-metadata/#name
    name='condu',  # Required
    # ==============================================================
    # Versions should comply with PEP 440:
    # https://www.python.org/dev/peps/pep-0440/
    #
    # For a discussion on single-sourcing the version across setup.py and the
    # project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='1.0.0b12',  # Required
    # ==============================================================
    # This is a one-line description or tagline of what your project does. This
    # corresponds to the "Summary" metadata field:
    # https://packaging.python.org/specifications/core-metadata/#summary
    description='Conductor client',  # Required
    # ==============================================================
    # This is a longer description of the project that represents
    # the body of text which users will see when they visit PyPI.
    #
    # Often, this is the same as README, so we can just read it from README.md
    # (as we have already done above)
    # This field corresponds to the "Description" metadata field:
    # https://packaging.python.org/specifications/core-metadata/#description-optional
    long_description=long_description,  # Optional
    #  ==============================================================
    # This should be a valid link the project's main homepage.
    # This field corresponds to the "Home-Page" metadata field:
    # https://packaging.python.org/specifications/core-metadata/#home-page-optional
    url='https://github.com/bioslikk/condu/',  # Optional
    #  ==============================================================
    # https://packaging.python.org/specifications/core-metadata/#author-optional
    author='Bruno Lopes',  # Optional
    #  ==============================================================
    # https://packaging.python.org/specifications/core-metadata/#author-email-optional
    author_email='brunoleonelopes@gmail.com',  # Optional
    #  ==============================================================
    # https://packaging.python.org/specifications/core-metadata/#maintainer-optional
    #not maintainer='Netflix OSS',  # Optional
    #  ==============================================================
    # https://packaging.python.org/specifications/core-metadata/#maintainer-email-optional
    #not maintainer_email='Netflix OSS',  # Optional
    #  ==============================================================
    # https://packaging.python.org/specifications/core-metadata/#classifier-multiple-use
    classifiers=[
        # List of possible values for the project's maturity
        # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        # Development Status :: 1 - Planning
        # Development Status :: 2 - Pre-Alpha
        # Development Status :: 3 - Alpha
        # Development Status :: 4 - Beta
        # Development Status :: 5 - Production/Stable
        # Development Status :: 6 - Mature
        # Development Status :: 7 - Inactive
        'Development Status :: 4 - Beta',
        # ==============================================================
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking'
    ],  # Optional
    #  ==============================================================
    # This field adds keywords for your project which will appear on the
    # project page.
    # https://packaging.python.org/specifications/core-metadata/#keywords-optional
    keywords='client for conductor',  # Optional
    #  ==============================================================
    # It is required to list the packages to be included in the project.
    # Although they can be listed manually, setuptools.find_packages finds them automatically.
    packages=find_packages(exclude=['tests', 'sample', 'sample.samples']),  # Required
    #  ==============================================================
    # This field lists other packages that the project depends on to run.
    # Any package you put here will be installed by pip when the project is
    # installed, reference - https://packaging.python.org/en/latest/requirements.html
    install_requires=['requests'],  # Optional,
    #  ==============================================================
    # https://packaging.python.org/specifications/core-metadata/#license-optional
    license='Apache 2.0',  # Optional
    #  ==============================================================
    # A string containing the URL from which this version of the distribution can be downloaded
    # https://packaging.python.org/specifications/core-metadata/#download-url
    # download_url='https://github.com/Netflix/conductor/releases',
    #  ==============================================================
    # This says that condu only runs on python 3.5 or greater
    # https://packaging.python.org/tutorials/distributing-packages/#python-requires
    python_requires='>=3.4',
    #  ==============================================================

)
