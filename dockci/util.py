"""
Generic DockCI utils
"""
import re
import shlex
import socket
import ssl
import struct
import subprocess
import sys
import json

from contextlib import contextmanager
from ipaddress import ip_address

import docker.errors
import py.error  # pylint:disable=import-error

from py.path import local  # pylint:disable=import-error


def default_gateway():
    """
    Gets the IP address of the default gateway
    """
    with open('/proc/net/route') as handle:
        for line in handle:
            fields = line.strip().split()
            if fields[1] != '00000000' or not int(fields[3], 16) & 2:
                continue

            return ip_address(socket.inet_ntoa(
                struct.pack("<L", int(fields[2], 16))
            ))


def bytes_human_readable(num, suffix='B'):
    """
    Gets byte size in human readable format
    """
    for unit in ('', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'):
        if abs(num) < 1000.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1000.0

    return "%.1f%s%s" % (num, 'Y', suffix)


@contextmanager
def stream_write_status(handle, status, success, fail):
    """
    Context manager to write a status, followed by success message, or fail
    message if yield raises an exception
    """
    handle.write(status.encode())
    try:
        yield
        handle.write((" %s\n" % success).encode())
    except Exception:  # pylint:disable=broad-except
        handle.write((" %s\n" % fail).encode())
        raise


def is_hex_string(value, max_len=None):
    """
    Is the value a hex string (only characters 0-f)
    """
    if max_len:
        regex = r'^[a-fA-F0-9]{1,%d}$' % max_len
    else:
        regex = r'^[a-fA-F0-9]+$'

    return re.match(regex, value) is not None


def is_git_hash(value):
    """
    Validate a git commit hash for validity
    """
    return is_hex_string(value, 40)


def is_git_ancestor(workdir, parent_check, child_check):
    """
    Figures out if the second is a child of the first.

    See git merge-base --is-ancestor
    """
    if parent_check == child_check:
        return False

    proc = subprocess.Popen(
        ['git', 'merge-base', '--is-ancestor', parent_check, child_check],
        cwd=workdir.strpath,
    )
    proc.wait()

    return proc.returncode == 0


def git_head_ref_name(workdir, stderr=None):
    """ Gets the full git ref name of the HEAD ref """
    proc = subprocess.Popen([
            'git', 'name-rev',
            '--name-only', '--no-undefined',
            '--ref', 'refs/heads/*',
            'HEAD',
        ],
        stdout=subprocess.PIPE,
        stderr=stderr,
        cwd=workdir.strpath,
    )
    proc.wait()
    if proc.returncode == 0:
        return parse_branch_from_ref(
            proc.stdout.read().decode().strip(), strict=False,
        )

    return None


def bytes_str(value):
    """
    Turn bytes, or string value into both bytes and string

    Examples:

    >>> bytes_str('myval')
    (b'myval', 'myval')

    >>> bytes_str(b'myval')
    (b'myval', 'myval')
    """
    v_bytes = v_str = value
    try:
        v_bytes = value.encode()
    except AttributeError:
        pass

    try:
        v_str = value.decode()
    except AttributeError:
        pass

    return v_bytes, v_str


def docker_ensure_image(client,
                        service,
                        insecure_registry=False,
                        handle=None):
    """
    Ensure that an image for the service exists, pulling from repo/tag if not
    available. If handle is given (a handle to write to), the pull output will
    be streamed through.

    Returns the image id (might be different, if repo/tag is used and doesn't
    match the ID pulled down... This is bad, but no way around it)
    """
    try:
        return client.inspect_image(service.image)['Id']

    except docker.errors.APIError:
        if handle:
            docker_data = client.pull(service.repo_full,
                                      service.tag,
                                      insecure_registry=insecure_registry,
                                      stream=True,
                                      )

        else:
            docker_data = client.pull(service.repo_full,
                                      service.tag,
                                      insecure_registry=insecure_registry,
                                      ).split('\n')

        latest_id = None
        for line in docker_data:
            line_bytes, line_str = bytes_str(line)

            if handle:
                handle.write(line_bytes)

            data = json.loads(line_str)
            if 'id' in data:
                latest_id = data['id']

        return latest_id


class FauxDockerLogBase(object):
    """
    A contextual logger to output JSON lines to a handle
    """
    def __init__(self):
        self.defaults = {}

    @contextmanager
    def more_defaults(self, **kwargs):
        """
        Set some defaults to write to the JSON
        """
        if not kwargs:
            yield
            return

        pre_defaults = self.defaults
        self.defaults = dict(tuple(self.defaults.items()) +
                             tuple(kwargs.items()))
        yield
        self.defaults = pre_defaults

    def update(self, **kwargs):
        """
        Write a JSON line with kwargs, and defaults combined
        """
        with self.more_defaults(**kwargs):
            return self.handle_line(
                ('%s\n' % json.dumps(self.defaults)).encode()
            )

    def handle_line(self, line):
        """ Handle outputting a line to the user """
        raise NotImplementedError("Must override handle_line method")


class GenFauxDockerLog(FauxDockerLogBase):
    """ Generator for the faux Docker lines """
    def handle_line(self, line):
        yield line


class IOFauxDockerLog(FauxDockerLogBase):
    """ Writes faux Docker lines to a handle """
    def __init__(self, handle):
        super(IOFauxDockerLog, self).__init__()
        self.handle = handle

    def handle_line(self, line):
        self.handle.write(line)
        self.handle.flush()


def guess_multi_value(value):
    """
    Make the best kind of list from `value`. If it's already a list, or tuple,
    do nothing. If it's a value with new lines, split. If it's a single value
    without new lines, wrap in a list
    """
    if isinstance(value, (tuple, list)):
        return value

    if isinstance(value, str) and '\n' in value:
        return [line.strip() for line in value.split('\n')]

    return [value]


def write_all(handle, lines, flush=True):
    """ Encode, write, then flush the line """
    if isinstance(lines, (tuple, list)):
        for line in lines:
            write_all(handle, line, False)
    else:
        if isinstance(lines, bytes):
            handle.write(lines)
        else:
            handle.write(str(lines).encode())

        if flush:
            handle.flush()


def str2bool(value):
    """ Convert a string to a boolean, accounting for english-like terms """
    value = value.lower()

    try:
        return bool(int(value))
    except ValueError:
        pass

    return value in ('yes', 'true', 'y', 't')


BUILT_RE = re.compile(r'Successfully built ([0-9a-f]+)')


def built_docker_image_id(data):
    """ Get an image ID out of the Docker stream data """
    re_match = BUILT_RE.search(data.get('stream', ''))
    if re_match:
        return re_match.group(1)

    return None


def path_contained(outer_path, inner_path):
    """ Ensure that ``inner_path`` is contained within ``outer_path`` """
    common = inner_path.common(outer_path)
    try:
        # Account for symlinks
        return common.samefile(outer_path)

    except py.error.ENOENT:
        return common == outer_path


def client_kwargs_from_config(host_str):
    """ Generate Docker Client kwargs from the host string """
    docker_host, *arg_strings = shlex.split(host_str)
    docker_client_args = {
        'base_url': docker_host,
    }

    tls_args = {}
    for arg_string in arg_strings:
        arg_name, arg_val = arg_string.split('=', 1)
        if arg_name == 'cert_path':
            cert_path = py.path.local(arg_val)
            tls_args['client_cert'] = (
                cert_path.join('cert.pem').strpath,
                cert_path.join('key.pem').strpath,
            )

            # Assign the CA if it exists, and verify isn't overridden
            ca_path = cert_path.join('ca.pem')
            if ca_path.check() and tls_args.get('verify', None) != False:
                tls_args['verify'] = ca_path.strpath

        elif arg_name == 'assert_hostname':
            tls_args[arg_name] = str2bool(arg_val)

        elif arg_name == 'verify':
            tls_args[arg_name] = str2bool(arg_val)

        elif arg_name == 'ssl_version':
            tls_args['ssl_version'] = getattr(ssl, 'PROTOCOL_%s' % arg_val)

    if tls_args:
        docker_client_args['tls'] = docker.tls.TLSConfig(**tls_args)

    return docker_client_args


GIT_NAME_REV_BRANCH = re.compile(r'^(remotes/origin/|refs/heads/)([^~]+)')


def parse_branch_from_ref(ref, strict=True, relax=False):
    """ Get a branch name from a git symbolic name """
    parsed = _parse_from_ref(ref, GIT_NAME_REV_BRANCH, strict)
    if parsed is not None:
        return parsed
    elif relax and '/' not in ref:
        return ref

    return None


def _parse_from_ref(ref, regex, strict):
    """ Logic for the tag/branch ref parsers """
    ref_match = regex.search(ref)

    if ref_match:
        return ref_match.groups()[1]
    elif not strict:
        return ref

    return None


def project_root():
    """ Get the DockCI project root """
    return local(__file__).dirpath().join('..')


def bin_root():
    """ Get the bin directory of the execution env """
    return local(sys.prefix).join('bin')


def rabbit_stage_key(stage, mtype):
    """ RabbitMQ routing key for messages """
    return 'dockci.{project_slug}.{job_slug}.{stage_slug}.{mtype}'.format(
        project_slug=stage.job.project.slug,
        job_slug=stage.job.slug,
        stage_slug=stage.slug,
        mtype=mtype,
    )


def normalize_stream_lines(stream):
    """
    Generator to split an iterator (stream, list, generator, etc) into single
    line strings, or bytes (dependant on input)

    :param stream: iterable to split
    :type stream: iterator

    :rtype: generator

    Examples:

      >>> from io import BytesIO, StringIO
      >>> tuple(normalize_stream_lines(StringIO('test\\nsomething')))
      ('test\\n', 'something')
      >>> tuple(normalize_stream_lines(BytesIO(b'test\\nsomething')))
      (b'test\\n', b'something')
      >>> tuple(normalize_stream_lines(StringIO('test\\nsomething\\n')))
      ('test\\n', 'something\\n')
      >>> tuple(normalize_stream_lines(('test', 'something')))
      ('test', 'something')
      >>> tuple(normalize_stream_lines(('test\\n', 'something')))
      ('test\\n', 'something')
      >>> tuple(normalize_stream_lines(('test\\nsomething',)))
      ('test\\n', 'something')
      >>> tuple(normalize_stream_lines(('test\\nsomething\\n',)))
      ('test\\n', 'something\\n')
      >>> tuple(normalize_stream_lines(('test\\n\\nsomething',)))
      ('test\\n', '\\n', 'something')
    """
    for chunk in stream:
        nl_char = '\n' if isinstance(chunk, str) else b'\n'
        lines = chunk.split(nl_char)
        max_idx = len(lines) - 1
        for idx, line in enumerate(lines):
            if idx < max_idx:
                yield line + nl_char
            elif len(line) == 0:
                continue
            else:
                yield line
