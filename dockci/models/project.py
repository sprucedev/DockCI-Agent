""" Project data handling """

import re

from urllib.parse import quote_plus

from marshmallow import Schema, fields

from dockci.server import CONFIG
from .auth import AuthenticatedRegistry
from .base import RegexField, request, RestModel


DOCKER_REPO_RE = re.compile(r'[a-z0-9-_.]+')


class ProjectSchema(Schema):
    """ Schema for loading and saving ``Project`` models """
    slug = fields.Str(default=None, allow_none=True)
    name = fields.Str(default=None, allow_none=True)
    utility = fields.Bool(default=None, allow_none=True)
    status = fields.Str(default=None, allow_none=True)
    branch_pattern = RegexField(default=None, allow_none=True)
    github_repo_id = fields.Str(default=None, allow_none=True)
    github_hook_id = fields.Str(default=None, allow_none=True)
    gitlab_repo_id = fields.Str(default=None, allow_none=True)
    registry_detail = fields.Str(default=None, allow_none=True)
    target_registry_detail = fields.Str(default=None,
                                        allow_none=True,
                                        load_from='target_registry')


class Project(RestModel):
    """ A project, representing a container to be built """
    SCHEMA = ProjectSchema()

    slug = None
    name = None
    utility = None
    status = None
    branch_pattern = None
    github_repo_id = None
    github_hook_id = None
    gitlab_repo_id = None
    registry_detail = None
    target_registry_detail = None

    def __str__(self):
        return '<{klass}: {project_slug}>'.format(
            klass=self.__class__.__name__,
            project_slug=self.slug,
        )

    def is_type(self, service):
        """ Check if the project is of a given service type

        :param service: Service name to check
        :type service: str

        :return bool:

        Examples:

          >>> Project().is_type('github')
          False

          >>> Project(github_repo_id='testr').is_type('github')
          True

          >>> Project().is_type('gitlab')
          False

          >>> Project(gitlab_repo_id='testr').is_type('gitlab')
          True
        """
        return getattr(self, '%s_repo_id' % service, None) is not None

    @property
    def gitlab_api_repo_endpoint(self):
        """ Repo endpoint for GitLab API

        :return str:

        :raise ValueError: Project isn't a GitLab project

        Examples:

          >>> proj = Project(gitlab_repo_id='testr')
          >>> proj.gitlab_api_repo_endpoint
          'v3/projects/testr'

          >>> Project().gitlab_api_repo_endpoint
          Traceback (most recent call last):
          ...
          ValueError: Not a GitLab repository
        """
        if self.gitlab_repo_id is None:
            raise ValueError("Not a GitLab repository")

        return 'v3/projects/%s' % quote_plus(self.gitlab_repo_id)

    @property
    def github_api_repo_endpoint(self):
        """ Repo endpoint for GitHub API

        :return str:

        :raise ValueError: Project isn't a GitHub project

        Examples:

          >>> proj = Project(github_repo_id='testr')
          >>> proj.github_api_repo_endpoint
          '/repos/testr'

          >>> Project().github_api_repo_endpoint
          Traceback (most recent call last):
          ...
          ValueError: Not a GitHub repository
        """
        if self.github_repo_id is None:
            raise ValueError("Not a GitHub repository")

        return '/repos/%s' % self.github_repo_id

    @property
    def github_api_hook_endpoint(self):
        """ Hook endpoint for GitHub API

        :return str:

        :raise ValueError: Project isn't a GitHub project
        :raise ValueError: Project doesn't have a tracked hook ID

        Examples:

          >>> proj = Project(github_repo_id='testr', github_hook_id='testh')
          >>> proj.github_api_hook_endpoint
          '/repos/testr/hooks/testh'

          >>> Project(github_repo_id='testr').github_api_hook_endpoint
          Traceback (most recent call last):
          ...
          ValueError: GitHub hook not tracked

          >>> Project(github_hook_id='testr').github_api_hook_endpoint
          Traceback (most recent call last):
          ...
          ValueError: Not a GitHub repository
        """
        if self.github_hook_id is None:
            raise ValueError("GitHub hook not tracked")

        return '%s/hooks/%s' % (self.github_api_repo_endpoint,
                                self.github_hook_id)

    @property
    def url(self):
        """ URL for this project

        :return str:

        Examples:

          >>> CONFIG.dockci_url = 'http://dockcitest'
          >>> proj = Project(slug='testproj')
          >>> proj.url
          'http://dockcitest/api/v1/projects/testproj'
        """
        return self.url_for(self.slug)

    @classmethod
    def url_for(cls, project_slug):  # pylint:disable=arguments-differ
        """ Generate the absolute URL to load a project from

        :param project_slug: Slug for the project to generate for
        :type project_slug: str

        :return str: Absolute URL for project with slug

        Examples:

          >>> CONFIG.dockci_url = 'http://dockcitest'
          >>> Project.url_for('testproj')
          'http://dockcitest/api/v1/projects/testproj'
        """
        return '{dockci_url}/api/v1/projects/{project_slug}'.format(
            dockci_url=CONFIG.dockci_url,
            project_slug=project_slug,
        )

    def latest_job(self,
                   passed=None,
                   versioned=None,
                   tag=None,
                   ):
        """ Retrieve the latest job that matches the given conditions

        :param passed: Has to have completed successfully
        :type passed: bool
        :param versioned: Has to have a tag
        :type versioned: bool
        :param tag: Has to match the given tag
        :type tag: bool

        :return dockci.models.job.Job: Job that matches all filters
        :return None: No job matches all filters

        :raise AssertionError: Response code unexpected
        """
        from .job import Job
        response = request(
            'GET', '%s/jobs' % self.url,
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

    def filtered_commit_refs(self, filters, repo):
        """ Get commits hash list for this project

        :param filters: Job filters to apply to the query
        :type filters: dict
        :param repo: Git repo to resolve the commits with
        :type repo: pygit2.Repository

        :return list: List of pygit2 ref objects

        :raise AssertionError: Response code unexpected
        """
        return [ref for ref in (
            repo.get(ref_hash)
            for ref_hash
            in self.filtered_commits(filters)
        ) if ref is not None]

    def filtered_commits(self, filters):
        """ Get commits hash list for this project

        :param filters: Job filters to apply to the query
        :type filters: dict

        :return list: List of commit hashes

        :raise AssertionError: Response code unexpected
        """
        response = request(
            'GET', '%s/jobs/commits' % self.url,
            params=filters,
        )
        assert response.status_code == 200
        return response.json()['items']

    _target_registry = None

    @property
    def target_registry(self):
        """ Docker registry this project pushes to. Loads from
        ``target_registry_detail`` URL, or cached

        :return AuthenticatedRegistry: Loaded from detail URL
        :return None: No registry to push to

        :raise AssertionError: Response code unexpected
        """

        if (
            self._target_registry is None and
            self.target_registry_detail is not None
        ):
            self._target_registry = AuthenticatedRegistry.load_url(
                self.target_registry_detail
            )

        return self._target_registry

    @target_registry.setter
    def target_registry(self, value):
        """ Set the ``target_registry`` cache

        :param value: Docker registry this project pushes to
        :type value: AuthenticatedRegistry
        """
        self._target_registry = value
