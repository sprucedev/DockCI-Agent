"""
DockCI - CI, but with that all important Docker twist
"""

# TODO fewer lines somehow
# pylint:disable=too-many-lines

import logging
import sys
import tempfile

from collections import OrderedDict
from enum import Enum
from itertools import chain

import docker
import py.path  # pylint:disable=import-error
import requests
import semver

from marshmallow import Schema, fields

from .base import DateTimeOrNow, RestModel, ServiceBase
from .project import Project
from .job_meta.config import JobConfig
from .job_meta.stages import JobStage
from .job_meta.stages_main import (BuildStage,
                                   TestStage,
                                   )
from .job_meta.stages_post import (PushStage,
                                   FetchStage,
                                   CleanupStage,
                                   )
from .job_meta.stages_prepare import (GitInfoStage,
                                      GitMtimeStage,
                                      TagVersionStage,
                                      WorkdirStage,
                                      )
from .job_meta.stages_prepare_docker import (DockerLoginStage,
                                             ProvisionStage,
                                             PushPrepStage,
                                             UtilStage,
                                             )


STATE_MAP = {
    'github': {
        'queued': 'pending',
        'running': 'pending',
        'success': 'success',
        'fail': 'failure',
        'broken': 'error',
        None: 'error',
    },
    'gitlab': {
        'queued': 'pending',
        'running': 'running',
        'success': 'success',
        'fail': 'failed',
        'broken': 'canceled',
        None: 'canceled',
    },
}


class JobResult(Enum):
    """ Possible results for Job models """
    success = 'success'
    fail = 'fail'
    broken = 'broken'


PushableReasons = Enum(  # pylint:disable=invalid-name
    'PushableReasons',
    """
    result_success
    good_exit
    tag_push
    branch_push
    """,
)


UnpushableReasons = Enum(  # pylint:disable=invalid-name
    'UnpushableReasons',
    """
    not_good_state
    bad_state
    no_tag
    no_project
    no_target_registry
    no_branch
    no_branch_pattern
    no_branch_match
    """,
)


PUSH_REASON_MESSAGES = {
    PushableReasons.result_success: "successful job result",
    PushableReasons.good_exit: "exit code was 0",
    PushableReasons.tag_push: "commit is tagged",
    PushableReasons.branch_push: "branch matches rules",

    UnpushableReasons.not_good_state: "job is not in a successful state",
    UnpushableReasons.bad_state: "job is in a bad state",
    UnpushableReasons.no_tag: "commit not tagged",
    UnpushableReasons.no_project: "job doesn't have a project",
    UnpushableReasons.no_target_registry: "project has no registry target",
    UnpushableReasons.no_branch: "commit not on a branch",
    UnpushableReasons.no_branch_pattern: "project has no branch pattern",
    UnpushableReasons.no_branch_match:
        "branch name doesn't match branch pattern",
}


class JobStageTmpSchema(Schema):
    """ Schema for loading and saving ``JobStageTmp`` models """
    success = fields.Bool(default=None, allow_none=True)
    job_detail = fields.Str(default=None, allow_none=True, load_only=True)


class JobStageTmp(RestModel):
    """ Quick and dirty list of job stages for the time being """
    SCHEMA = JobStageTmpSchema()

    slug = None
    success = None
    job_detail = None

    @classmethod
    def url_for(cls, project_slug, job_slug, stage_slug):  # noqa,pylint:disable=arguments-differ
        """ Generate the absolute URL to load a stage from

        :param project_slug: Slug for the project to generate for
        :type project_slug: str
        :param job_slug: Slug for the job to generate for
        :type job_slug: str
        :param stage_slug: Slug for the stage to generate for
        :type stage_slug: str

        :return str: Absolute URL for job with slug

        Examples:

          >>> from dockci.server import CONFIG

          >>> CONFIG.dockci_url = 'http://dockcitest'
          >>> JobStageTmp.url_for('testproj', 'testjob', 'teststage')
          'http://dockcitest/.../testproj/jobs/testjob/stages/teststage'
        """
        return '{job_url}/stages/{stage_slug}'.format(
            job_url=Job.url_for(project_slug, job_slug),
            stage_slug=stage_slug,
        )

    @property
    def url(self):
        """ URL for this stage """
        return JobStageTmp.url_for(
            self.job.project.slug, self.job.slug, self.slug,
        )

    _job = None

    @property
    def job(self):
        """ Job associated with this stage

        :return Job: Loaded from detail URL
        :return None: No job associated

        :raise AssertionError: Response code unexpected
        """

        if (
            self._job is None and
            self.job_detail is not None
        ):
            self._job = Job.load_url(
                self.job_detail
            )

        return self._job

    @job.setter
    def job(self, value):
        """ Set the ``job`` cache

        :param value: Job associated with this stage
        :type value: Job
        """
        self._job = value


class JobSchema(Schema):
    """ Schema for loading and saving ``Job`` models """
    slug = fields.Str(default=None, allow_none=True, load_only=True)
    state = fields.Str(default=None, allow_none=True, load_only=True)
    result = fields.Str(default=None, allow_none=True)
    commit = fields.Str(default=None, allow_none=True)
    create_ts = fields.DateTime(default=None, allow_none=True, load_only=True)
    start_ts = DateTimeOrNow(default=None, allow_none=True)
    complete_ts = DateTimeOrNow(default=None, allow_none=True)
    tag = fields.Str(default=None, allow_none=True)
    git_branch = fields.Str(default=None, allow_none=True)
    project_detail = fields.Str(default=None, allow_none=True, load_only=True)
    project_slug = fields.Str(default=None, allow_none=True, load_only=True)
    display_repo = fields.Str(default=None, allow_none=True, load_only=True)
    command_repo = fields.Str(default=None, allow_none=True, load_only=True)
    image_id = fields.Str(default=None, allow_none=True)
    container_id = fields.Str(default=None, allow_none=True)
    exit_code = fields.Int(default=None, allow_none=True)
    git_author_name = fields.Str(default=None, allow_none=True)
    git_author_email = fields.Str(default=None, allow_none=True)
    git_committer_name = fields.Str(default=None, allow_none=True)
    git_committer_email = fields.Str(default=None, allow_none=True)


class Job(RestModel):  # noqa,pylint:disable=too-many-public-methods,too-many-instance-attributes
    """ An individual project job, and result """
    SCHEMA = JobSchema()

    slug = None
    state = None
    result = None
    commit = None
    create_ts = None
    start_ts = None
    complete_ts = None
    tag = None
    git_branch = None
    project_detail = None
    project_slug = None
    display_repo = None
    command_repo = None
    image_id = None
    container_id = None
    exit_code = None
    git_author_name = None
    git_author_email = None
    git_committer_name = None
    git_committer_email = None

    @classmethod
    def url_for(cls, project_slug, job_slug):  # noqa,pylint:disable=arguments-differ
        """ Generate the absolute URL to load a job from

        :param project_slug: Slug for the project to generate for
        :type project_slug: str
        :param job_slug: Slug for the job to generate for
        :type job_slug: str

        :return str: Absolute URL for job with slug

        Examples:

          >>> from dockci.server import CONFIG

          >>> CONFIG.dockci_url = 'http://dockcitest'
          >>> Job.url_for('testproj', 'testjob')
          'http://dockcitest/api/v1/projects/testproj/jobs/testjob'
        """
        return '{project_url}/jobs/{job_slug}'.format(
            project_url=Project.url_for(project_slug),
            job_slug=job_slug,
        )

    @property
    def url(self):
        """ URL for this job """
        return Job.url_for(self.project.slug, self.slug)

    def __init__(self, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        self._provisioned_containers = []
        self._old_image_ids = []
        self._stage_objects = {}

    def __str__(self):
        try:
            slug = self.slug
        except AttributeError:
            slug = '<unknown>'

        return '<{klass}: {project_slug}/{job_slug}>'.format(
            klass=self.__class__.__name__,
            project_slug=self.project.slug,
            job_slug=slug,
        )

    _project = None

    @property
    def project(self):
        """ Project associated with this job

        :return Project: Loaded from detail URL
        :return None: No project associated

        :raise AssertionError: Response code unexpected
        """

        if (
            self._project is None and
            self.project_detail is not None
        ):
            self._project = Project.load_url(
                self.project_detail
            )

        return self._project

    @project.setter
    def project(self, value):
        """ Set the ``project`` cache

        :param value: Project associated with this job
        :type value: Project
        """
        self._project = value

    _ancestor_job = None

    @property
    def ancestor_job(self):
        """ Ancestor associated with this job

        :return Job: Loaded from detail URL
        :return None: No ancestor associated

        :raise AssertionError: Response code unexpected
        """

        if (
            self._ancestor_job is None and
            self.ancestor_detail is not None
        ):
            self._ancestor_job = Job.load_url(
                self.ancestor_detail
            )

        return self._ancestor_job

    @ancestor_job.setter
    def ancestor_job(self, value):
        """ Set the ``ancestor_job`` cache

        :param value: Ancestor associated with this job
        :type value: Job
        """
        self._ancestor_job = value

    def resolve_job_ancestor(self, repo):
        closest = self.closest_job_ref(repo)
        if closest is None:
            return None

        response = requests.get(
            '%s/jobs' % self.project.url,
            params={'commit': closest.hex, 'per_page': 2},
        )
        assert response.status_code == 200
            i = response.json()['items']
            if i[0]['slug'] == self.slug:
                try:
                    d = i[1]['detail']
                except IndexError:
                    pass  # XXX next closest
            else:
                d = i[0]['detail']
            self.ancestor_job = Job.load_url(d)

    def closest_job_ref(self, repo):
        closest = None
        if self.git_branch is not None and self.git_branch != 'master':
            closest = self.closest_ref(repo, self.project.filtered_commit_refs(
                {'completed': True, 'branch': self.git_branch},
                repo
            ))
        if closest is None:
            closest = self.closest_ref(repo, self.project.filtered_commit_refs(
                {'completed': True, 'branch': master},
                repo
            ))

        return closest

    def closest_ref(self, repo, refs):
        local = repo[self.commit]
        ab = {
            ref: repo.ahead_behind(local.hex, ref.hex)
            for ref in refs
        }
        print({r.hex: a for r, a in ab.items()})
        ab = {
            key: ahead
            for key, (ahead, behind) in ab.items()
            if behind == 0
        }

        min_ahead = None
        closest = None
        for ref, ahead in ab.items():
            if (
                min_ahead is None or
                ahead < min_ahead or
                (ahead == min_ahead and closest.commit_time < ref.commit_time)
            ):
                min_ahead = ahead
                closest = ref
        print(closest.hex)

        return closest

    _job_config = None

    @property
    def job_config(self):
        """ JobConfig for this Job """
        if self._job_config is None:
            self._job_config = JobConfig(job=self)
        return self._job_config

    def changed_result(self, workdir=None):
        """
        Check if this job changed the result from it's ancestor. None if
        there's no result yet
        """
        if self.result is None:
            return None

        ancestor_job = self.ancestor_job
        if not ancestor_job:
            return True

        if ancestor_job.result is None:
            if workdir is None:  # Can't get a better ancestor
                return True

            ancestor_job = self.project.latest_job_ancestor(
                workdir, self.commit, complete=True,
            )

        if not ancestor_job:
            return True

        return ancestor_job.result != self.result

    _docker_client = None

    @property
    def docker_client(self):
        """
        Get the cached (or new) Docker Client object being used for this job

        CACHED VALUES NOT AVAILABLE OUTSIDE FORK
        """
        if self._docker_client is None:
            self._docker_client = docker.Client()
            # if self.docker_client_host is not None:
            #     for host_str in CONFIG.docker_hosts:
            #         if host_str.startswith(self.docker_client_host):
            #             docker_client_args = client_kwargs_from_config(
            #                 host_str,
            #             )
            #
            # elif CONFIG.docker_use_env_vars:
            #     docker_client_args = kwargs_from_env()
            #
            # else:
            #     docker_client_args = client_kwargs_from_config(
            #         # TODO real load balancing, queueing
            #         random.choice(CONFIG.docker_hosts),
            #     )
            #
            # self.docker_client_host = docker_client_args['base_url']
            # # self.save()
            #
            # self._docker_client = docker.Client(**docker_client_args)

        return self._docker_client

    @property
    def tag_semver(self):
        """
        Job tag, parsed as semver (or None if no match). Allows a 'v' prefix
        """
        if self.tag is None:
            return None

        try:
            return semver.parse(self._tag_without_v)
        except ValueError:
            pass

    @property
    def tag_semver_str_v(self):
        """ Job commit's tag with v prefix added or None if not semver """
        without = self.tag_semver_str
        if without:
            return "v%s" % without

    @property
    def tag_semver_str(self):
        """
        Job commit's tag with any v prefix dropped or None if not semver
        """
        if self.tag_semver:
            return self._tag_without_v

    @property
    def _tag_without_v(self):
        """ Job commit's tag with any v prefix dropped """
        if self.tag[0] == 'v':
            return self.tag[1:]
        else:
            return self.tag

    @property
    def branch_tag(self):
        """ Docker tag for the git branch """
        if self.git_branch:
            return 'latest-%s' % self.git_branch

    @property
    def docker_tag(self):
        """ Tag for the docker image """
        if not self.push_candidate:
            return None

        if self.tag_push_candidate:
            return self.tag

        return self.branch_tag

    @property
    def tags_set(self):
        """ Set of all tags this job should be known by """
        return {
            tag
            for tag in (
                self.tag,
                self.branch_tag,
            )
            if tag is not None
        }

    @property
    def tag_tags_set(self):
        """ Set of all tags from the git tag this job may be known by """
        return {
            tag
            for tag in (
                self.tag_semver_str,
                self.tag_semver_str_v,
                self.tag,
            )
            if tag is not None
        }

    @property
    def possible_tags_set(self):
        """ Set of all tags this job may be known by """
        branch_tag = self.branch_tag
        tags_set = self.tag_tags_set
        if branch_tag:
            tags_set.add(branch_tag)

        return tags_set

    @property
    def service(self):
        """ ``ServiceBase`` represented by this job """
        return ServiceBase(
            name=self.project.name,
            repo=self.job_config.repo_name,
            tag=self.tag,
            project=self.project,
            job=self,
        )

    @property
    def utilities(self):
        """ Dictionary of utility slug suffixes and their configuration """
        utility_suffixes = UtilStage.slug_suffixes_gen([
            config['name']  # TODO handle KeyError gracefully
            # pylint:disable=no-member
            for config in self.job_config.utilities
        ])
        utilities = zip(
            # pylint:disable=no-member
            utility_suffixes, self.job_config.utilities
        )
        return OrderedDict(utilities)

    @property
    def is_good_state(self):
        """
        Is the job completed, and in a good state (success)

        Examples:

        >>> Job(result=JobResult.success.value).is_good_state
        True

        >>> Job().is_good_state
        False
        """
        return self._is_good_state_full[0]

    @property
    def _is_good_state_full(self):
        """
        Return the ``bool`` for ``is_good_state``, and a reason tokens

        Examples:

        >>> Job(result=JobResult.success.value)._is_good_state_full
        (True, {<PushableReasons.result_success...>})

        >>> Job(exit_code=0)._is_good_state_full
        (True, {<PushableReasons.good_exit...>})

        >>> Job()._is_good_state_full
        (False, {<UnpushableReasons.not_good_state...>})
        """
        if self.result == JobResult.success.value:
            return True, {PushableReasons.result_success}
        if self.result is None and self.exit_code == 0:
            return True, {PushableReasons.good_exit}

        return False, {UnpushableReasons.not_good_state}

    @property
    def is_bad_state(self):
        """
        Is the job completed, and in a bad state (failed, broken)

        Examples:

        >>> Job(result=JobResult.fail.value).is_bad_state
        True

        >>> Job().is_bad_state
        False
        """
        return self._is_bad_state_full[0]

    @property
    def _is_bad_state_full(self):
        """
        Return the ``bool`` for ``is_bad_state``, and a reason tokens

        Examples:

        >>> Job(result=JobResult.fail.value)._is_bad_state_full
        (True, {<UnpushableReasons.bad_state...>})

        >>> Job(result=JobResult.broken.value)._is_bad_state_full
        (True, {<UnpushableReasons.bad_state...>})

        >>> Job(result=JobResult.success.value)._is_bad_state_full
        (False, set())

        >>> Job()._is_bad_state_full
        (False, set())
        """
        bad = self.result in (JobResult.fail.value, JobResult.broken.value)
        if bad:
            return bad, {UnpushableReasons.bad_state}

        return bad, set()

    @property
    def tag_push_candidate(self):
        """
        Determines if this job has a tag, and target registry

        Examples:

        >>> Job().tag_push_candidate
        False
        """
        return self._tag_push_candidate_full[0]

    @property
    def _tag_push_candidate_full(self):
        """
        Return the ``bool`` for ``tag_push_candidate``, and a reason tokens

        Note: more tests in DB test suite

        Examples:

        >>> Job()._tag_push_candidate_full
        (False, {<UnpushableReasons.no_project...>})
        """
        if not self.project:
            return False, {UnpushableReasons.no_project}
        if not self.project.target_registry:
            return False, {UnpushableReasons.no_target_registry}
        if not self.tag:
            return False, {UnpushableReasons.no_tag}

        return True, {PushableReasons.tag_push}

    @property
    def branch_push_candidate(self):
        """
        Determines if this job has a branch, target registry and the project
        branch pattern matches
        """
        return self._branch_push_candidate_full[0]

    @property
    def _branch_push_candidate_full(self):
        """
        Return the ``bool`` for ``branch_push_candidate``, and the reason
        tokens

        Note: more tests in DB test suite

        Examples:

        >>> Job()._branch_push_candidate_full
        (False, {<UnpushableReasons.no_project...>})
        """
        if not self.project:
            return False, {UnpushableReasons.no_project}
        if not self.project.target_registry:
            return False, {UnpushableReasons.no_target_registry}
        if not self.git_branch:
            return False, {UnpushableReasons.no_branch}
        if not self.project.branch_pattern:
            return False, {UnpushableReasons.no_branch_pattern}
        if not self.project.branch_pattern.match(self.git_branch):
            return False, {UnpushableReasons.no_branch_match}

        return True, {PushableReasons.branch_push}

    @property
    def push_candidate(self):
        """ Is the job a push candidate for either tag or branch push """
        return self._push_candidate_full[0]

    @property
    def _push_candidate_full(self):
        """
        Return the ``bool`` for ``branch_push_candidate``, and the reason
        """
        tag_push, tag_push_reasons = self._tag_push_candidate_full
        if tag_push:
            return True, tag_push_reasons

        branch_push, branch_push_reasons = self._branch_push_candidate_full
        if branch_push:
            return True, branch_push_reasons

        tag_push_reasons.update(branch_push_reasons)
        return False, tag_push_reasons

    @property
    def pushable(self):
        """ Is the job a push candidate, and in a good state """
        return self._pushable_full[0]

    @property
    def _pushable_full(self):
        """
        Return the ``bool`` for ``pushable``, and the reason tokens
        """
        push, push_reasons = self._push_candidate_full
        if not push:
            return False, push_reasons

        is_good, is_good_reasons = self._is_good_state_full
        if not is_good:
            return False, is_good_reasons

        push_reasons.update(is_good_reasons)
        return True, push_reasons

    @property
    def pushable_message(self):
        """ Messages describing why the job is, or isn't pushable """
        return Job._pushable_message_for(self._pushable_full[1])

    @classmethod
    def _pushable_message_for(cls, reasons):
        """
        Generate messages for a list of reason tokens

        Examples:

        >>> Job._pushable_message_for({PushableReasons.good_exit})
        'Pushable, because exit code was 0'

        >>> Job._pushable_message_for({UnpushableReasons.not_good_state})
        'Not pushable, because job is not in a successful state'

        >>> Job._pushable_message_for({
        ...     PushableReasons.good_exit,
        ...     PushableReasons.tag_push,
        ... })
        'Pushable, because commit is tagged, and exit code was 0'

        >>> Job._pushable_message_for({
        ...     PushableReasons.good_exit,
        ...     PushableReasons.tag_push,
        ...     UnpushableReasons.no_branch_match,
        ... })  # doctest: +NORMALIZE_WHITESPACE
        "Not pushable, because branch name doesn't match branch pattern, even
        though commit is tagged, and exit code was 0"

        >>> Job._pushable_message_for({})
        'Unknown'

        >>> Job._pushable_message_for({'something dumb'})
        'Unknown'
        """
        p_reasons = sorted([PUSH_REASON_MESSAGES[rea]
                            for rea in reasons
                            if rea in PushableReasons])
        u_reasons = sorted([PUSH_REASON_MESSAGES[rea]
                            for rea in reasons
                            if rea in UnpushableReasons])

        if p_reasons and not u_reasons:
            return "Pushable, because %s" % (
                cls._pushable_messages_join(p_reasons))
        if u_reasons and not p_reasons:
            return "Not pushable, because %s" % (
                cls._pushable_messages_join(u_reasons))
        elif p_reasons and u_reasons:
            return "Not pushable, because %s, even though %s" % (
                cls._pushable_messages_join(u_reasons),
                cls._pushable_messages_join(p_reasons))

        return "Unknown"

    @classmethod
    def _pushable_messages_join(cls, messages):
        """
        Join messages with commas, 'and', or nothing if just 1

        Examples:

        >>> Job._pushable_messages_join(['pikachu'])
        'pikachu'

        >>> Job._pushable_messages_join(['pikachu', 'charmander'])
        'pikachu, and charmander'

        >>> Job._pushable_messages_join(['pikachu', 'charmander', 'squirtle'])
        'pikachu, charmander, and squirtle'

        >>> Job._pushable_messages_join([
        ...     'pikachu', 'charmander', 'squirtle', 'bulbasaur'
        ... ])
        'pikachu, charmander, squirtle, and bulbasaur'
        """
        if len(messages) > 1:
            return "%s, and %s" % (
                ", ".join(messages[0:-1]),
                messages[-1]
            )
        else:
            return messages[0]

    def run(self, workdir=None):
        """
        Worker func that performs the job
        """
        if workdir is None:
            with tempfile.TemporaryDirectory() as workdir:
                return self.run(py.path.local(workdir))

        self.start_ts = 'now'
        self.save()

        self._stage_objects = {
            stage.slug: stage
            for stage in [
                WorkdirStage(self, workdir),
                GitInfoStage(self, workdir),
                # GitChangesStage(self, workdir),
                GitMtimeStage(self, workdir),
                TagVersionStage(self, workdir),
                PushPrepStage(self),
                DockerLoginStage(self, workdir),
                ProvisionStage(self),
                BuildStage(self, workdir),
                TestStage(self),
                PushStage(self),
                FetchStage(self),
                CleanupStage(self),
            ]
        }

        try:
            git_info = (stage() for stage in (
                lambda: self._stage_objects['git_prepare'].run(True),
                lambda: self._stage_objects['git_info'].run(True),
            ))

            if not all(git_info):
                self.result = 'broken'
                return False

            self._stage_objects.update({
                stage.slug: stage
                for stage in [
                    UtilStage(self, workdir, util_suffix, util_config)
                    for util_suffix, util_config
                    in self.utilities.items()
                ]
            })

            def tag_stage():
                """ Runner for ``TagVersionStage`` """
                if self.tag:
                    return True  # Don't override tags
                else:
                    return self._stage_objects['git_tag'].run(None)

            def push_prep_stage():
                """ Runner for ``PushPrepStage`` """
                if self.push_candidate:
                    return self._stage_objects['docker_push_prep'].run(None)
                else:
                    return True  # No prep to do for unpushable

            def util_stage_wrapper(suffix):
                """ Wrap a util stage for running """
                stage = self._stage_objects['utility_%s' % suffix]
                return lambda: stage.run(0)

            prepare = (stage() for stage in chain(
                (
                    # lambda: self._stage_objects['git_changes'].run(0),
                    lambda: self._stage_objects['git_mtime'].run(None),
                    tag_stage,
                    push_prep_stage,
                    lambda: self._stage_objects['docker_login'].run(0),
                ), (
                    util_stage_wrapper(util_suffix)
                    for util_suffix
                    in self.utilities.keys()
                ), (
                    lambda: self._stage_objects['docker_provision'].run(0),
                    lambda: self._stage_objects['docker_build'].run(0),
                )
            ))

            if not all(prepare):
                self.result = 'broken'
                self.save()
                return False

            if not self._stage_objects['docker_test'].run(0):
                self.result = 'fail'
                self.save()
                return False

            # We should fail the job here because if this is a tagged
            # job, we can't rebuild it
            if not self._stage_objects['docker_push'].run(0):
                self.result = 'broken'
                self.save()
                return False

            self.result = 'success'
            self.save()

            # Failing this doesn't indicate job failure
            # TODO what kind of a failure would this not working be?
            self._stage_objects['docker_fetch'].run(None)

            return True
        except Exception:  # pylint:disable=broad-except
            self._error_stage('error')
            self.result = 'broken'
            self.save()

            return False

        finally:
            try:
                self._stage_objects['cleanup'].run(None)

            except Exception:  # pylint:disable=broad-except
                self._error_stage('post_error')

            self.complete_ts = 'now'
            self.save()

    def _error_stage(self, stage_slug):
        """
        Create an error stage and add stack trace for it
        """
        message = None
        try:
            _, ex, _ = sys.exc_info()
            if ex.human_str:
                message = str(ex)

        except AttributeError:
            pass

        if message is None:
            import traceback
            message = traceback.format_exc()

        try:
            JobStage(
                self,
                stage_slug,
                lambda handle: handle.write(
                    message.encode()
                )
            ).run()
        except Exception:  # pylint:disable=broad-except
            logging.exception("Error adding error stage")
