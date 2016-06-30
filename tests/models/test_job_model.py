import re

from unittest.mock import PropertyMock

import pytest

from dockci.models.auth import AuthenticatedRegistry
from dockci.models.job import (Job,
                               JobResult,
                               PUSH_REASON_MESSAGES,
                               PushableReasons,
                               UnpushableReasons,
                               )
from dockci.models.project import Project


CHANGED_RESULT_PARAMS = [
    (JobResult.success, JobResult.success, False),
    (JobResult.success, JobResult.fail, True),
    (JobResult.success, JobResult.broken, True),
    (JobResult.fail, JobResult.broken, True),
]


class TestChangedResult(object):
    """ Test ``Job.changed_result`` """
    @pytest.mark.parametrize(
        'prev_result,new_result,changed',
        CHANGED_RESULT_PARAMS + [(JobResult.success, None, None)]
    )
    def test_ancestor_complete(self,
                               mocker,
                               prev_result,
                               new_result,
                               changed):
        """ Test when ancestor job has a result """
        job_current = Job()
        job_ancestor = Job()

        mocker.patch.object(job_current, 'ancestor_job', new=job_ancestor)
        mocker.patch.object(job_current, 'result', new=new_result)
        mocker.patch.object(job_ancestor, 'result', new=prev_result)

        assert job_current.changed_result() == changed


    @pytest.mark.parametrize(
        'prev_result,new_result,changed',
        CHANGED_RESULT_PARAMS
    )
    def test_ancestor_incomplete(self,
                                 mocker,
                                 prev_result,
                                 new_result,
                                 changed):
        job_current = Job()
        job_ancestor_incomplete = Job()
        job_ancestor = Job()
        project = Project()

        mocker.patch.object(job_ancestor_incomplete, 'result', new=None)
        mocker.patch.object(
            job_current, 'ancestor_job', new=job_ancestor_incomplete,
        )

        mocker.patch.object(job_current, 'project', new=project)
        mocker.patch.object(job_current, 'commit', new='fake commit')
        mocker.patch.object(job_current, 'result', new=new_result)
        mocker.patch.object(job_ancestor, 'result', new=prev_result)

        ancestor_mock = mocker.patch.object(
            project, 'latest_job_ancestor', return_value=job_ancestor,
        )

        assert job_current.changed_result(workdir='fake workdir') == changed

        ancestor_mock.assert_called_once_with(
            'fake workdir', 'fake commit', complete=True,
        )


    def test_ancestor_incomplete_no_workdir(self, mocker):
        job_current = Job()
        job_ancestor_incomplete = Job()

        mocker.patch.object(job_ancestor_incomplete, 'result', new=None)
        mocker.patch.object(
            job_current, 'ancestor_job', new=job_ancestor_incomplete,
        )

        mocker.patch.object(job_current, 'result', new=JobResult.success)

        assert job_current.changed_result() == True


    @pytest.mark.parametrize('new_result,changed', [
        (JobResult.success, True),
        (JobResult.fail, True),
        (JobResult.broken, True),
        (None, None),
    ])
    def test_no_ancestors(self,
                          mocker,
                          new_result,
                          changed):
        """ Test when ancestor job has a result """
        job_current = Job()

        mocker.patch.object(job_current, 'ancestor_job', new=None)
        mocker.patch.object(job_current, 'result', new=new_result)

        assert job_current.changed_result() == changed


class TestStateBools(object):
    """ Test ``Job.is_good_state`` and ``Job.is_bad_state`` """
    @pytest.mark.parametrize('result,exp', [
        (JobResult.success.value, True),
        (JobResult.fail.value, False),
    ])
    def test_good_state_result(self, result, exp):
        """ Test ``Job.is_good_state`` from job result """
        job = Job(result=result)
        assert job.is_good_state == exp

    @pytest.mark.parametrize('result,code,exp', [
        (JobResult.fail.value, 0, False),
        (None, 1, False),
        (None, 0, True),
    ])
    def test_good_state_exit_code(self, result, code, exp):
        """ Test ``Job.is_good_state`` from exit code """
        job = Job(result=result, exit_code=code)
        assert job.is_good_state == exp

    @pytest.mark.parametrize('result,exp', [
        (JobResult.success.value, False),
        (JobResult.fail.value, True),
        (JobResult.broken.value, True),
    ])
    def test_bad_state(self, result, exp):
        """ Test ``Job.is_bad_state`` """
        job = Job(result=result)
        assert job.is_bad_state == exp


class TestPushable(object):
    """ Test the various job push properties """
    @pytest.mark.parametrize('tag,reg,exp', [
        (None, None, False),
        ('abc', None, False),
        (None, AuthenticatedRegistry(), False),
        ('abc', AuthenticatedRegistry(), True),
    ])
    def test_tag_push_candidate(self, tag, reg, exp):
        """ Test ``Job.tag_push_candidate`` """
        project = Project(target_registry=reg)
        job = Job(project=project, tag=tag)

        assert job.tag_push_candidate == exp

    @pytest.mark.parametrize('branch,pattern,reg,exp', [
        (None, None, None, False),
        ('abc', None, None, False),
        (None, None, AuthenticatedRegistry(), False),
        ('abc', None, AuthenticatedRegistry(), False),
        (None, re.compile('abc'), None, False),
        ('abc', re.compile('abc'), None, False),
        (None, re.compile('abc'), AuthenticatedRegistry(), False),
        ('abc', re.compile('abc'), AuthenticatedRegistry(), True),
        ('abc', re.compile('nomatch'), AuthenticatedRegistry(), False),
        ('abc', re.compile('(a|b)bc'), AuthenticatedRegistry(), True),
        ('acd', re.compile('(a|b)cd'), AuthenticatedRegistry(), True),
        ('bcd', re.compile('(a|b)cd'), AuthenticatedRegistry(), True),
        ('abcd', re.compile('bcd'), AuthenticatedRegistry(), False),
    ])
    def test_branch_push_candidate(self, branch, pattern, reg, exp):
        """ Test ``Job.branch_push_candidate`` """
        project = Project(branch_pattern=pattern, target_registry=reg)
        job = Job(project=project, git_branch=branch)

        assert job.branch_push_candidate == exp

    @pytest.mark.parametrize('tag_pc,branch_pc,good_state,exp', [
        (False, False, False, False),
        (True, False, True, True),
        (False, True, True, True),
        (True, False, False, False),
        (False, True, False, False),
        (True, True, True, True),
    ])
    def test_pushable(self, mocker, tag_pc, branch_pc, good_state, exp):
        """ Test ``Job.pushable`` """
        mocker.patch('dockci.models.job.Job._is_good_state_full',
                     new_callable=PropertyMock(
                        return_value=(good_state, set())))
        mocker.patch('dockci.models.job.Job._tag_push_candidate_full',
                     new_callable=PropertyMock(
                        return_value=(tag_pc, set())))
        mocker.patch('dockci.models.job.Job._branch_push_candidate_full',
                     new_callable=PropertyMock(
                        return_value=(branch_pc, set())))
        job = Job()

        assert job.pushable == exp


class TestJobBase(object):
    """ Base for name, semver tests """
    def setup_method(self, method):
        self.registry = AuthenticatedRegistry()
        self.project = Project(
            target_registry=self.registry,
            branch_pattern=re.compile('master'),
        )
        self.job = Job(project=self.project)


class TestNames(TestJobBase):
    """ Test some of the job naming methods """
    def test_docker_tag_branch(self):
        """ Test ``Job.docker_tag`` when ``git_branch`` is set """
        self.job.git_branch = 'master'
        assert self.job.docker_tag == 'latest-master'

    def test_docker_tag_tag(self):
        """ Test ``Job.docker_tag`` when ``tag`` is set """
        self.job.tag = 'test'
        assert self.job.docker_tag == 'test'

    def test_docker_tag_none(self):
        """ Test ``Job.docker_tag`` when nothing is set """
        assert self.job.docker_tag == None

    def test_docker_tag_branch_tag(self):
        """ Test ``Job.docker_tag`` when ``git_branch``, and ``tag`` is set """
        self.job.tag = 'test'
        self.job.git_branch = 'master'
        assert self.job.docker_tag == 'test'

    @pytest.mark.parametrize('tag,branch,exp', [
        ('0.0.0', 'master', {'0.0.0', 'latest-master'}),
        ('v0.0.0', 'master', {'v0.0.0', 'latest-master'}),
        ('0.0', 'master', {'0.0', 'latest-master'}),
        (None, 'master', {'latest-master'}),
        ('0.0', None, {'0.0'}),
        ('0.0.0', None, {'0.0.0'}),
        ('v0.0.0', None, {'v0.0.0'}),
        (None, None, set()),
    ])
    def test_tags_set(self, tag, branch, exp):
        """ Test ``Job.tags_set`` """
        self.job.tag = tag
        self.job.git_branch = branch
        assert self.job.tags_set == exp

    @pytest.mark.parametrize('tag,branch,exp', [
        ('0.0.0', 'master', {'0.0.0', 'v0.0.0', 'latest-master'}),
        ('v0.0.0', 'master', {'0.0.0', 'v0.0.0', 'latest-master'}),
        ('0.0', 'master', {'0.0', 'latest-master'}),
        (None, 'master', {'latest-master'}),
        ('0.0', None, {'0.0'}),
        ('0.0.0', None, {'0.0.0', 'v0.0.0'}),
        ('v0.0.0', None, {'0.0.0', 'v0.0.0'}),
        (None, None, set()),
    ])
    def test_possible_tags_set(self, tag, branch, exp):
        """ Test ``Job.possible_tags_set`` """
        self.job.tag = tag
        self.job.git_branch = branch
        assert self.job.possible_tags_set == exp

    @pytest.mark.parametrize('tag,branch,exp', [
        ('0.0.0', 'master', {'0.0.0', 'v0.0.0'}),
        ('v0.0.0', 'master', {'0.0.0', 'v0.0.0'}),
        ('0.0', 'master', {'0.0'}),
        (None, 'master', set()),
        ('0.0', None, {'0.0'}),
        ('0.0.0', None, {'0.0.0', 'v0.0.0'}),
        ('v0.0.0', None, {'0.0.0', 'v0.0.0'}),
        (None, None, set()),
    ])
    def test_tag_tags_set(self, tag, branch, exp):
        """ Test ``Job.tag_tags_set`` """
        self.job.tag = tag
        self.job.git_branch = branch
        assert self.job.tag_tags_set == exp


class TestSemver(TestJobBase):
    """ Test semver related functionality for the ``Job`` model """
    @pytest.mark.parametrize('tag,exp', [
        ('0.0.0', dict(major=0, minor=0, patch=0)),
        ('1.2.3', dict(major=1, minor=2, patch=3)),
        ('7.11.2', dict(major=7, minor=11, patch=2)),
        ('v7.11.2', dict(major=7, minor=11, patch=2)),
        ('7.11.2-abc', dict(major=7, minor=11, patch=2, prerelease='abc')),
        ('7.11.2-abc+d', dict(
            major=7, minor=11, patch=2, prerelease='abc', build='d',
        )),
        ('v7.11.2-abc+d', dict(
            major=7, minor=11, patch=2, prerelease='abc', build='d',
        )),
    ])
    def test_tag_semver(self, tag, exp):
        """ Test semver-like parsings for ``Job.tag_semver`` """
        self.job.tag = tag

        actual = self.job.tag_semver
        for key, value in exp.items():
            assert actual.pop(key) == value

        for key, value in actual.items():
            assert value is None

    @pytest.mark.parametrize('tag', [
        'av1.1.1', 'a1.1.1', '1.1', '1', 'a.1.1', '1.a.1', '1.1.a', '1.1.1-½',
        '1.1.1-1+½', None,
    ])
    def test_tag_semver_invalid(self, tag):
        """ Test non-semver-like parsings for ``Job.tag_semver`` """
        self.job.tag = tag
        assert self.job.tag_semver == None

    @pytest.mark.parametrize('tag,exp_no_v', [
        ('0.0.0-ab+cd', '0.0.0-ab+cd'),
        ('v0.0.0-ab+cd', '0.0.0-ab+cd'),
    ])
    def test_str(self, tag, exp_no_v):
        """
        Test ``tag_semver_str`` and ``tag_semver_str_v`` with semver-like tags
        """
        self.job.tag = tag
        assert self.job.tag_semver_str == exp_no_v
        assert self.job.tag_semver_str_v == 'v%s' % exp_no_v

    @pytest.mark.parametrize('tag', [
        'a.0.0-ab+cd', 'a0.0.0-ab+cd',
    ])
    def test_str_invalid(self, tag):
        """
        Test ``tag_semver_str`` and ``tag_semver_str_v`` with non-semver-like
        tags
        """
        self.job.tag = tag
        assert self.job.tag_semver_str is None
        assert self.job.tag_semver_str_v is None


class TestService(object):
    """ Test generation of ``ServiceBase`` object for the ``Job`` """
    @pytest.mark.parametrize('kwargs,repo_name_,exp_display', [
        (
            dict(
                project=Project(
                    name='DockCI App',
                    target_registry=AuthenticatedRegistry(
                        base_name='localhost:5000'
                    )
                )
            ),
            'sprucedev/dockci',
            'DockCI App - localhost:5000/sprucedev/dockci:latest',
        ),
        (
            dict(
                tag='v0.0.9',
                project=Project(
                    name='CoolProject2000',
                    target_registry=AuthenticatedRegistry(
                        base_name='docker.io'
                    )
                )
            ),
            'myorg/coolproj',
            'CoolProject2000 - docker.io/myorg/coolproj:v0.0.9',
        ),
        (
            dict(
                project=Project(
                    name='DockCI App',
                    target_registry=AuthenticatedRegistry(
                        base_name='docker.io'
                    )
                )
            ),
            'dockci',
            'DockCI App - docker.io/dockci:latest',
        ),
    ])
    def test_service_display(self, mocker, kwargs, repo_name_, exp_display):
        job = Job(**kwargs)
        class MockJobConfig(object):
            repo_name = repo_name_
        job._job_config = MockJobConfig()
        job.id = 20

        assert job.service.display_full == exp_display


@pytest.mark.parametrize('source_enum', [PushableReasons, UnpushableReasons])
def test_push_reason_messages(source_enum):
    """ Ensure that all push reasons have messages """
    for member in source_enum:
        assert member in PUSH_REASON_MESSAGES
