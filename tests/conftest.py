import random
import subprocess

from contextlib import contextmanager
from urllib.parse import urlparse, urlunparse

import alembic
import pytest

from flask_migrate import migrate

from dockci.models.auth import Role
from dockci.models.job import Job, JobStageTmp
from dockci.models.project import Project
from dockci.server import APP, app_init, DB, MIGRATE


@pytest.yield_fixture
def tmpgitdir(tmpdir):
    """ Get a new ``tmpdir``, make it the cwd, and set git config """
    with tmpdir.as_cwd():
        subprocess.check_call(['git', 'init'])
        for name, val in (
            ('user.name', 'DockCI Test'),
            ('user.email', 'test@example.com'),
        ):
            subprocess.check_call(['git', 'config', name, val])

        yield tmpdir
