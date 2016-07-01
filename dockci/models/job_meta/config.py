"""
Job metadata stored along side the code
"""

from marshmallow import fields, Schema
from yaml import safe_load as load_yaml

from dockci.models.base import BaseModel


def parse_util_file(file_data):
    """
    Parse the input/output lines of util config

    Examples:

    >>> files = parse_util_file('/util /work/thing')
    >>> files['from']
    '/util'
    >>> files['to']
    '/work/thing'

    >>> files = parse_util_file({'from': '/util', 'to': '/work/thing'})
    >>> files['from']
    '/util'
    >>> files['to']
    '/work/thing'

    >>> files = parse_util_file('/work/thing')
    >>> files['from']
    '/work/thing'
    >>> files['to']
    '/work/thing'

    >>> files = parse_util_file({'to': '/work/thing'})
    >>> files['from']
    '/work/thing'
    >>> files['to']
    '/work/thing'

    >>> files = parse_util_file({'from': '/util'})
    >>> files['from']
    '/util'
    >>> files['to']
    '/util'
    """
    if isinstance(file_data, dict):
        if 'from' not in file_data:
            return {'from': file_data['to'], 'to': file_data['to']}
        if 'to' not in file_data:
            return {'from': file_data['from'], 'to': file_data['from']}

        return file_data

    else:
        parts = file_data.split(' ')
        if len(parts) == 1:
            return {'from': file_data, 'to': file_data}
        else:
            return {'from': parts[0], 'to': parts[1]}


class FileCopyField(fields.Field):
    """ Field for deserializing file copy data in config """
    def _deserialize(self, value, attr, data):
        return parse_util_file(value)


class UtilitySchema(Schema):
    name = fields.Str(required=True)
    command = fields.Str()
    input = fields.List(FileCopyField, missing=lambda: [])
    output = fields.List(FileCopyField, missing=lambda: [])


class ServiceSchema(Schema):
    name = fields.Str(required=True)
    alias = fields.Str()
    environment = fields.Dict(missing=lambda: {})


class JobConfigSchema(Schema):
    """ Schema for loading and saving ``JobStageTmp`` models """
    job_output = fields.Dict(missing=lambda: {})
    services = fields.Nested(ServiceSchema, many=True, missing=lambda: [])
    utilities = fields.Nested(UtilitySchema, many=True, missing=lambda: [])

    dockerfile = fields.Str(missing='Dockerfile')
    repo_name = fields.Str()
    skip_tests = fields.Bool(missing=False)


class JobConfig(BaseModel):
    """ Job config, loaded from the repo """
    SCHEMA = JobConfigSchema(strict=True)

    job = None

    job_output = None
    services = None
    utilities = None
    dockerfile = 'Dockerfile'
    skip_tests = False

    _repo_name = None

    def __init__(self, **kwargs):
        self.set_all(**self.SCHEMA.load({}).data)
        super(JobConfig, self).__init__(**kwargs)

    @property
    def repo_name(self):
        """ Docker repository name

        :return str:

        Examples:

          >>> JobConfig(repo_name='testr').repo_name
          'testr'

          from ..job import Job
          from ..project import Project
          >>> proj = Project(slug='testp')
          >>> job = Job(project=proj)
          >>> conf = JobConfig(job=job)
          >>> conf.repo_name
          'testp'
        """
        if self._repo_name is not None:
            return self._repo_name
        return self.job.project.slug

    @repo_name.setter
    def repo_name(self, value):
        """ Set cached/overridden repo name """
        self._repo_name = value

    def load_yaml_file(self, path):
        """ Load the data in the file as YAML, then deserialize into the model
        with the schema

        :param path: Path to the YAML file to load
        :type path: py.path.local
        """
        with path.open('r') as handle:
            data = load_yaml(handle)

        self.set_all(**self.SCHEMA.load(data).data)
