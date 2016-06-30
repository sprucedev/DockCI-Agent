import io
import json
import subprocess

import docker
import pytest

from dockci.util import (client_kwargs_from_config,
                         GenFauxDockerLog,
                         git_head_ref_name,
                         IOFauxDockerLog,
                         is_git_ancestor,
                         )


class TestClientKwargsFromConfig(object):
    """ Tests for ``dockci.util.client_kwargs_from_config`` """
    @pytest.mark.parametrize('host_str,expected,expected_tls_dict', (
        ('https://localhost', {'base_url': 'https://localhost'}, {}),
        (
            'https://localhost assert_hostname=no',
            {'base_url': 'https://localhost'},
            {
                'assert_fingerprint': None,
                'assert_hostname': False,
                'ssl_version': 5,
            },
        ),
        (
            'https://localhost ssl_version=TLSv1',
            {'base_url': 'https://localhost'},
            {
                'assert_fingerprint': None,
                'assert_hostname': None,
                'ssl_version': 3,
            },
        ),
        (
            'https://localhost verify=no',
            {'base_url': 'https://localhost'},
            {
                'assert_fingerprint': None,
                'assert_hostname': None,
                'ssl_version': 5,
                'verify': False,
            },
        ),
        (
            'https://localhost assert_hostname=no ssl_version=TLSv1',
            {'base_url': 'https://localhost'},
            {
                'assert_fingerprint': None,
                'assert_hostname': False,
                'ssl_version': 3,
            },
        ),
    ))
    def test_parse_host_str(self,
                            host_str,
                            expected,
                            expected_tls_dict,
                            ):
        """ Test basic ``host_str`` parsing; no surprises """
        out = client_kwargs_from_config(host_str)
        out_tls = out.pop('tls', {})

        try:
            out_tls = out_tls.__dict__
        except AttributeError:
            pass

        assert out == expected
        assert out_tls == expected_tls_dict

    def test_parse_host_str_certs(self, tmpdir):
        """ Test setting all certificates """
        tmpdir.join('cert.pem').ensure()
        tmpdir.join('key.pem').ensure()
        tmpdir.join('ca.pem').ensure()

        out = client_kwargs_from_config(
            'http://l cert_path=%s' % tmpdir.strpath
        )

        assert out['tls'].cert == (
            tmpdir.join('cert.pem').strpath,
            tmpdir.join('key.pem').strpath,
        )
        assert out['tls'].verify == tmpdir.join('ca.pem').strpath

    @pytest.mark.parametrize('host_str_fs', (
        'http://l verify=no cert_path={cert_path}',
        'http://l cert_path={cert_path} verify=no',
    ))
    def test_no_verify_no_ca(self, host_str_fs, tmpdir):
        """ Test that ``verify=no`` overrides ``cert_path`` """
        tmpdir.join('cert.pem').ensure()
        tmpdir.join('key.pem').ensure()
        tmpdir.join('ca.pem').ensure()

        out = client_kwargs_from_config(
            host_str_fs.format(cert_path=tmpdir.strpath),
        )

        assert out['tls'].cert == (
            tmpdir.join('cert.pem').strpath,
            tmpdir.join('key.pem').strpath,
        )
        assert out['tls'].verify == False

    def test_certs_error(self, tmpdir):
        """ Test raising ``TLSParameterError`` when certs don't exist """
        with pytest.raises(docker.errors.TLSParameterError):
            client_kwargs_from_config(
                'http://l cert_path=%s' % tmpdir.strpath
            )

    def test_no_ca_no_error(self, tmpdir):
        """
        Ensure that when client cert/key exists, but the CA doesn't, cert
        params are set without verify
        """
        tmpdir.join('cert.pem').ensure()
        tmpdir.join('key.pem').ensure()

        out = client_kwargs_from_config(
            'http://l cert_path=%s' % tmpdir.strpath
        )

        assert out['tls'].cert == (
            tmpdir.join('cert.pem').strpath,
            tmpdir.join('key.pem').strpath,
        )
        assert out['tls'].verify == None


class TestGitRefNameOf(object):
    """ Test the ``git_head_ref_name`` function """
    @pytest.mark.parametrize('branch', ['master', 'otherbranch'])
    def test_master(self, tmpgitdir, branch):
        """ Test getting ref name when single commit on master """
        with tmpgitdir.join('file_a.txt').open('w') as handle:
            handle.write('first file')

        subprocess.check_call(['git', 'checkout', '-b', branch])
        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'first'])

        assert git_head_ref_name(tmpgitdir) == branch

    def test_multiple_branches(self, tmpgitdir):
        """ Test when branch is not master """
        with tmpgitdir.join('file_a.txt').open('w') as handle:
            handle.write('first file')

        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'first'])

        subprocess.check_call(['git', 'checkout', '-b', 'testbranch'])

        with tmpgitdir.join('file_b.txt').open('w') as handle:
            handle.write('second file')

        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'second'])

        assert git_head_ref_name(tmpgitdir) == 'testbranch'

    @pytest.mark.parametrize('branch', ['master', 'otherbranch'])
    def test_tagged(self, tmpgitdir, branch):
        """
        Test when a git commit is made, tagged, then described by it's tag
        """
        with tmpgitdir.join('file_a.txt').open('w') as handle:
            handle.write('first file')

        subprocess.check_call(['git', 'checkout', '-b', branch])
        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'first'])
        subprocess.check_call(['git', 'tag', '-a', 'v0', '-m', 'v0 first'])

        assert git_head_ref_name(tmpgitdir) == branch

    @pytest.mark.parametrize('branch', ['master', 'otherbranch'])
    def test_detached_head(self, tmpgitdir, branch):
        """ Test when in a detached head state """
        with tmpgitdir.join('file_a.txt').open('w') as handle:
            handle.write('first file')

        subprocess.check_call(['git', 'checkout', '-b', branch])
        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'first'])
        first_hash = subprocess.check_output(
            ['git', 'show', '-s', '--format=format:%H']).decode()
        detached_output = subprocess.check_output(
            ['git', 'checkout', first_hash],
            stderr=subprocess.STDOUT,
        ).decode()

        assert "You are in 'detached HEAD' state" in detached_output
        assert git_head_ref_name(tmpgitdir) == branch

    @pytest.mark.parametrize('branch', ['master', 'otherbranch'])
    def test_tagged_detached_head(self, tmpgitdir, branch):
        """ Test when in a detached head state, where the commit is tagged """
        with tmpgitdir.join('file_a.txt').open('w') as handle:
            handle.write('first file')

        subprocess.check_call(['git', 'checkout', '-b', branch])
        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'first'])
        subprocess.check_call(['git', 'tag', '-a', 'v0', '-m', 'v0 first'])
        first_hash = subprocess.check_output(
            ['git', 'show', '-s', '--format=format:%H']).decode()
        detached_output = subprocess.check_output(
            ['git', 'checkout', first_hash],
            stderr=subprocess.STDOUT,
        ).decode()

        assert "You are in 'detached HEAD' state" in detached_output
        assert git_head_ref_name(tmpgitdir) == branch


class TestGitAncestor(object):
    """ Tests the is_git_ancestor utility """
    def test_two_commits(self, tmpgitdir):
        """
        Ensure that a commit directly before another is correctly identified as
        an ancestor, and that the child is identified as not an ancestor
        """
        with tmpgitdir.join('file_a.txt').open('w') as handle:
            handle.write('first file')

        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'first'])
        first_hash = subprocess.check_output(
            ['git', 'show', '-s', '--format=format:%H']).decode()

        with tmpgitdir.join('file_b.txt').open('w') as handle:
            handle.write('second file')

        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'second'])
        second_hash = subprocess.check_output(
            ['git', 'show', '-s', '--format=format:%H']).decode()

        assert is_git_ancestor(tmpgitdir, first_hash, second_hash)
        assert not is_git_ancestor(tmpgitdir, second_hash, first_hash)


class TestGenFauxDockerLog(object):
    """ Tests the ``GenFauxDockerLog`` class """
    def test_defaults_and_update(self):
        log = GenFauxDockerLog()
        with log.more_defaults(keya='val a'):
            lines = list(log.update(keyb='val b'))
            assert len(lines) == 1
            assert json.loads(lines[0].decode()) == dict(
                keya='val a',
                keyb='val b',
            )

    def test_no_defaults_and_update(self):
        log = GenFauxDockerLog()
        lines = list(log.update(keyb='val b'))
        assert len(lines) == 1
        assert json.loads(lines[0].decode()) == dict(
            keyb='val b'
        )


class TestIOFauxDockerLog(object):
    """ Tests the ``IOFauxDockerLog`` class """
    def test_defaults_and_update(self):
        handle = io.BytesIO()
        log = IOFauxDockerLog(handle)
        with log.more_defaults(keya='val a'):
            log.update(keyb='val b')
            handle.seek(0)
            lines = handle.readlines()
            assert len(lines) == 1
            assert json.loads(lines[0].decode()) == dict(
                keya='val a',
                keyb='val b',
            )

    def test_no_defaults_and_update(self):
        handle = io.BytesIO()
        log = IOFauxDockerLog(handle)
        log.update(keyb='val b')
        handle.seek(0)
        lines = handle.readlines()
        assert len(lines) == 1
        assert json.loads(lines[0].decode()) == dict(
            keyb='val b'
        )
