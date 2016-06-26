""" Click commands for running unit/style/static tests """
import os

import click

from dockci.server import cli
from dockci.util import bin_root, project_root


def call_seq(*commands):
    """ Call commands in sequence, returning on first failure """
    for cmd in commands:
        result = cmd()
        if result is not None and result != 0:
            raise click.Abort()

    return 0


@cli.command()
def unittest():
    """ Run unit tests """
    return unittest_()


def unittest_():
    """ Run unit tests """
    import pytest

    tests_dir = project_root().join('tests')
    if not pytest.main(['--doctest-modules', '-vvrxs', tests_dir.strpath]):
        raise click.Abort()


@cli.command()
def doctest():
    """ Run doc tests """
    return doctest_()


def doctest_():
    """ Run doc tests """
    import pytest

    tests_dir = project_root().join('dockci')
    if not pytest.main(['--doctest-modules', '-vvrxs', tests_dir.strpath]):
        raise click.Abort()


@cli.command()
def pep8():
    """ Style tests with PEP8 """
    return pep8_()


def pep8_():
    """ Style tests with PEP8 """
    from pep8 import StyleGuide

    code_dir = project_root().join('dockci')
    pep8style = StyleGuide(parse_argv=False)

    report = pep8style.check_files((code_dir.strpath,))

    if report.total_errors:
        raise click.Abort()


@cli.command()
def pylint():
    """ Style tests with pylint """
    return pylint_()


def pylint_():
    """ Style tests with pylint """
    root_path = project_root()
    code_dir = root_path.join('dockci')
    rc_file = root_path.join('pylint.conf')

    pylint_path = bin_root().join('pylint').strpath
    os.execvp(pylint_path, [
        pylint_path,
        '--rcfile', rc_file.strpath,
        code_dir.strpath,
    ])


def pylint_forked():
    """ Fork, and execute the pylint command """
    pid = os.fork()
    if not pid:
        pylint_()
    else:
        _, returncode = os.waitpid(pid, 0)

        if returncode is not 0:
            raise click.Abort()


@cli.command()
def styletest():
    """ Run style tests """
    return styletest_()


def styletest_():
    """ Run style tests """
    return call_seq(pep8_, pylint_forked)


@cli.command()
def ci():  # pylint:disable=invalid-name
    """ Run all tests """
    return call_seq(styletest_, unittest_, doctest_)
