"""
DockCI - CI, but with that all important Docker twist
"""

import logging
import re

from urllib.parse import quote_plus, urlparse, urlunparse
from uuid import uuid4

import py.error  # pylint:disable=import-error
import requests

from marshmallow import Schema, fields, post_load

from .auth import AuthenticatedRegistry
from .base import RestModel
from dockci.server import CONFIG
from dockci.util import (ext_url_for,
                         is_git_ancestor,
                         is_git_hash,
                         )


DOCKER_REPO_RE = re.compile(r'[a-z0-9-_.]+')


class ProjectSchema(Schema):
    slug = fields.Str(default=None, allow_none=True)
    name = fields.Str(default=None, allow_none=True)
    utility = fields.Bool(default=None, allow_none=True)
    status = fields.Str(default=None, allow_none=True)
    #display_repo = fields.Str(default=None, allow_none=True)
    branch_pattern = fields.Str(default=None, allow_none=True)
    github_repo_id = fields.Str(default=None, allow_none=True)
    github_repo_hook = fields.Str(default=None, allow_none=True)
    gitlab_repo_id = fields.Str(default=None, allow_none=True)
    registry_detail = fields.Str(default=None, allow_none=True)
    target_registry_detail = fields.Str(default=None,
                                        allow_none=True,
                                        load_from='target_registry')


class Project(RestModel):  # pylint:disable=no-init
    """
    A project, representing a container to be built
    """
    SCHEMA = ProjectSchema()

    def __str__(self):
        return '<{klass}: {project_slug}>'.format(
            klass=self.__class__.__name__,
            project_slug=self.slug,
        )

    def is_type(self, service):
        """ Check if the project is of a given service type """
        return getattr(self, '%s_repo_id' % service) is not None

    @property
    def gitlab_api_repo_endpoint(self):
        """ Repo endpoint for GitLab API """
        if self.gitlab_repo_id is None:
            raise ValueError("Not a GitLab repository")

        return 'v3/projects/%s' % quote_plus(self.gitlab_repo_id)

    @property
    def github_api_repo_endpoint(self):
        """ Repo endpoint for GitHub API """
        if self.github_repo_id is None:
            raise ValueError("Not a GitHub repository")

        return '/repos/%s' % self.github_repo_id

    @property
    def github_api_hook_endpoint(self):
        """ Hook endpoint for GitHub API """
        if self.github_hook_id is None:
            raise ValueError("GitHub hook not tracked")

        return '%s/hooks/%s' % (self.github_api_repo_endpoint,
                                self.github_hook_id)

    @property
    def url(self):
        """ URL for this project """
        return self.url_for(self.slug)

    @classmethod
    def url_for(_, project_slug):
        return '{dockci_url}/api/v1/projects/{project_slug}'.format(
            dockci_url=CONFIG.dockci_url,
            project_slug=project_slug,
        )

    def latest_job(self,
                   passed=None,
                   versioned=None,
                   tag=None,
                   ):
        from .job import Job
        response = requests.get(
            '%s/jobs' % self.url,
            params=dict(
                per_page=1,
                versioned=versioned,
                passed=passed,
                tag=tag,
            )
        )
        assert response.status_code == 200
        try:
            return Job.load_url(response.json()['items'][0]['detail'])
        except IndexError:
            return None

    _target_registry = None

    @property
    def target_registry(self):
        if self._target_registry is None:
            try:
                self._target_registry = AuthenticatedRegistry.load_url(
                    self.target_registry_detail
                )
            except AttributeError:
                return None

        return self._target_registry

    @target_registry.setter
    def target_registry(self, value):
        self._target_registry = value
