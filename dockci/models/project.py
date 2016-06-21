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

from .base import RepoFsMixin, RestModel
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


class Project(RestModel, RepoFsMixin):  # pylint:disable=no-init
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
        return (
            getattr(self, '%s_repo_id' % service) and
            self.external_auth_token and
            self.external_auth_token.service == service
        )

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

    @property
    def repo_fs(self):
        """ Format string for the repo """
        if self.is_type('gitlab'):
            gitlab_parts = list(urlparse(CONFIG.gitlab_base_url))
            gitlab_parts[1] = 'oauth2:{token_key}@%s' % gitlab_parts[1]
            gitlab_parts[2] = '%s.git' % self.gitlab_repo_id
            return urlunparse(gitlab_parts)

        elif self.is_type('github'):
            return 'https://oauth2:{token_key}@github.com/%s.git' % (
                self.github_repo_id
            )

        return self.repo
