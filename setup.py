import argparse
import os
import shutil
import subprocess
import sys

from setuptools import setup, Extension


LIBRARY_NAME = 'pyaodbc'
VERSION = '0.2.0'


def uninstall_pyaodbc():
    print('***** start uninstall pyaodbc *****')
    subprocess.check_call([sys.executable, '-m', 'pip', 'uninstall', '-y', LIBRARY_NAME])
    print('***** finish uninstall pyaodbc *****')


def remove_tmp_directories():
    print('***** start remove tmp directories *****')
    directories = [
        os.path.realpath('.pytest_cache'),
        os.path.realpath('build'),
        os.path.realpath('dist'),
        os.path.realpath('pyaodbc.egg-info'),
    ]

    for directory in directories:
        shutil.rmtree(directory, True)

    print('***** finish remove tmp directories *****')


def main():
    if sys.version_info < (3, 7):
        sys.exit("Python < 3.7 isn't supported")

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--dev', help='dev mode: uninstall pyaodbc and remove temp directories', action='store_true')
    ext_args, args = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + args
    dev = True if ext_args.dev else False

    platform = sys.platform
    libraries = []

    if platform == 'win32':
        libraries.append('odbc32')
    elif platform == 'linux':
        libraries.append('odbc')

    sources = [os.path.realpath(os.path.join('src', file)) for file in os.listdir('src') if file.endswith('.c')]

    if not libraries:
        raise SystemError('Unsupported platform!')

    modules = [Extension(
        name=LIBRARY_NAME,
        sources=sources,
        libraries=libraries
    )]

    data_files = [('', [
        os.path.realpath(os.path.join('src', file))
        for file in os.listdir('src')
        if file.endswith('.pyi')
    ])]

    if dev:
        uninstall_pyaodbc()
        remove_tmp_directories()

    with open('README.md') as readme_file:
        long_description = readme_file.read()

    classifiers = [
        'Framework :: AsyncIO',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Database'
    ]

    platforms = [
        'Windows',
        'Linux'
    ]

    setup(
        name=LIBRARY_NAME,
        version=VERSION,
        author='Sergio Morello',
        author_email='mail.Krik@gmail.com',
        maintainer='Sergio Morello',
        maintainer_email='mail.Krik@gmail.com',
        url='https://github.com/SergioMorelo/pyaodbc',
        description='Library for asynchronous connection and execution of queries to the database via ODBC driver',
        long_description=long_description,
        long_description_content_type='text/markdown',
        download_url='https://pypi.org/project/pyaodbc/',
        classifiers=classifiers,
        platforms=platforms,
        license='Apache license 2.0',
        ext_modules=modules,
        data_files=data_files
    )


if __name__ == '__main__':
    main()
