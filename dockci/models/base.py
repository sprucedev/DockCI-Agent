""" Base model classes, mixins """

# TODO fewer lines somehow
# pylint:disable=too-many-lines

import logging
import re
import warnings

from collections import defaultdict

import requests

from marshmallow import fields, ValidationError

from dockci.server import CONFIG


class DateTimeOrNow(fields.DateTime):
    """ Extension to ``marshmallow.fields.DateTime`` to allow the value
    ``"now"`` to be used """
    def _serialize(self, value, attr, obj):
        if value is 'now':
            return 'now'
        return super(DateTimeOrNow, self)._serialize(value, attr, obj)


class RegexField(fields.Str):
    """ Serialize and deserialize regex to/from ``re`` objects """
    def _serialize(self, value, attr, obj):
        """ Serialize regex to a string

        :param value: regex object to serialize
        :type value: _sre.SRE_Pattern

        :rtype: str

        Examples:

          >>> RegexField()._serialize(re.compile('.*'), None, None)
          '.*'

          >>> type(RegexField()._serialize(None, None, None))
          <class 'NoneType'>
        """
        if value is None:
            return None
        return value.pattern

    def _deserialize(self, value, attr, data):
        """ Deserialize a string to a regex

        :param value: string to turn into regex object
        :type value: str

        :rtype: _sre.SRE_Pattern

        Examples:

          >>> pat = RegexField()._deserialize('.*', None, None)
          >>> type(pat)
          <class '_sre.SRE_Pattern'>
          >>> pat.pattern
          '.*'

          >>> type(RegexField()._deserialize(None, None, None))
          <class 'NoneType'>

          >>> RegexField()._deserialize('(', None, None)
          Traceback (most recent call last):
          ...
          marshmallow.exceptions.ValidationError: missing )...
        """
        if value is None:
            return None
        return self._compile_re(value)

    def _validate(self, value):
        """ Deserialize a string to a regex

        :param value: regex string to validate
        :type value: str

        :raise marshmallow.exceptions.ValidationError: when invalid regex

        Examples:

          >>> RegexField()._validate('.*')

          >>> RegexField()._validate(None)

          >>> RegexField()._validate('(')
          Traceback (most recent call last):
          ...
          marshmallow.exceptions.ValidationError: missing )...
        """
        super(RegexField, self)._validate(value)
        if value is None:
            return
        self._compile_re(value)

    def _compile_re(self, value):  # pylint:disable=no-self-use
        """ Compile a regex, or raise exception on failure """
        try:
            return re.compile(value)
        except re.error as ex:
            raise ValidationError(str(ex))


def abs_detail_url(url):
    """ Absolute URL from possibly partial URL, assuming DockCI as root

    Examples:

      >>> abs_detail_url('http://localhost')
      'http://localhost'

      >>> CONFIG.dockci_url = 'http://dockcitest'
      >>> abs_detail_url('/api/v1/test')
      'http://dockcitest/api/v1/test'
    """
    if url.startswith('http'):
        return url

    return '{dockci_url}{url}'.format(
        dockci_url=CONFIG.dockci_url,
        url=url,
    )


class BaseModel(object):
    """ Base model for loading/dumping to data """
    def __init__(self, **kwargs):
        self.set_all(**kwargs)

    def set_all(self, **kwargs):
        """ Set all attributes given in ``kwargs``. Also warns if ``SCHEMA``
        contains fields that aren't attributes on this model

        :param **kwargs: Attributes to set
        """

        logging.debug('Setting attributes %s', kwargs)
        for key, val in kwargs.items():
            if key == 'state':  # XXX figure out how to deal with this
                continue

            setattr(self, key, val)

        for key in self.SCHEMA.declared_fields.keys():
            if not hasattr(self, key):
                warnings.warn(
                    'Attribute "{key}" not declared on model '
                    '{module}.{klass}'.format(
                        key=key,
                        module=self.__class__.__module__,
                        klass=self.__class__.__name__,
                    ),
                    SyntaxWarning,
                )
                setattr(self, key, None)

    @property
    def SCHEMA(self):  # pylint:disable=invalid-name
        """ Schema for loading and saving class instances """
        raise NotImplementedError("Must override 'SCHEMA' attribute")


def request(
    method,
    url,
    params=None,
    data=None,
    json=None,
    headers=None,
):
    """ Wrapper around ``requests.request`` to add auth to
    everything by default """
    if headers is None:
        headers = {}

    final_headers = {'x-dockci-api-key': CONFIG.api_key}
    final_headers.update(headers)

    return requests.request(
        method, url,
        params=params,
        data=data,
        json=json,
        headers=final_headers,
    )


class RestModel(BaseModel):
    """ Base model for RESTful loading of data """
    _new = True

    @classmethod
    def load(cls, *args, **kwargs):
        """ Load the URL generated by the ``url_for`` method when passed this
        method's args. See :py:meth:`load_url`

        :param *args: Arguments for the class's ``url_for`` method
        :param **kwargs: Additional data to merge into the response

        :raise AssertionError: Response code not 200
        """
        return cls.load_url(cls.url_for(*args), **kwargs)

    @classmethod
    def load_url(cls, url, **kwargs):
        """ Serialize and save the model data to the given URL. Load retrieved
        data into the model

        :param url: Absolute URL to load from
        :type url: str
        :param kwargs: Additional data to merge into the response

        :raise AssertionError: Response code not 200
        """
        url = abs_detail_url(url)

        logging.debug('Loading %s', url)

        response = request('GET', url)

        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise Exception(response.json()['message'])

        data = kwargs.copy()
        data.update(response.json())
        # pylint:disable=no-member
        return cls(_new=False, **cls.SCHEMA.load(data).data)

    def save(self):
        """ Save to the URL given by the ``url`` attribute. See
        :py:meth:`save_to`

        :raise AssertionError: Response code not 2xx
        """
        self.save_to(self.url)

    def save_to(self, url):
        """ Serialize and save the model data to the given URL. Load retrieved
        data into the model

        :param url: Absolute URL to save to
        :type url: str

        :raise AssertionError: Response code not 2xx
        """
        url = abs_detail_url(url)
        data = self.SCHEMA.dump(self).data

        logging.warning('Saving %s with %s to %s', self, data, url)

        method = 'PUT' if self.is_new else 'PATCH'

        response = request(method, url, json=data)

        # XXX not an assert
        assert response.status_code >= 200 and response.status_code < 300, (
            "Failed to save. HTTP %d: %s" % (
                response.status_code,
                response.json()
            )
        )

        self.set_all(**self.SCHEMA.load(response.json()).data)

    @property
    def is_new(self):
        """ Check if the model is new, or existing

        :returns bool:
        """
        return bool(self._new)

    @property
    def url(self):
        """ URL for this model instance

        :return str:
        """
        raise NotImplementedError("Must override 'url' method")

    @classmethod
    def url_for(cls, *args):
        """ Generate the absolute URL to load a model instance

        :return str:
        """
        raise NotImplementedError("Must override 'url_for' class method")


SLUG_REPLACE_RE = re.compile(r'[^a-zA-Z0-9_]')


# TODO all these disabled pylint things should be reviewed later on. How would
#      we better split this up?
# pylint:disable=too-many-public-methods,too-many-instance-attributes
class ServiceBase(object):
    """
    Service object for storing utility/provision information

    Examples:

    Most examples can be seen in their relevant property docs.

    >>> svc = ServiceBase(meta={'config': 'fake'})
    >>> svc.meta
    {'config': 'fake'}
    """

    # pylint:disable=too-many-arguments
    def __init__(self,
                 name=None,
                 repo=None,
                 tag=None,
                 project=None,
                 job=None,
                 base_registry=None,
                 auth_registry=None,
                 meta=None,
                 use_db=True,
                 ):

        if meta is None:
            meta = {}

        self.meta = meta

        self.use_db = use_db

        self._auth_registry_dynamic = None
        self._project_dynamic = None
        self._job_dynamic = None

        self._name = None
        self._repo = None
        self._tag = None
        self._base_registry = None
        self._auth_registry = None
        self._project = None
        self._job = None

        self.name = name
        self.repo = repo
        self.tag = tag
        self.base_registry = base_registry
        self.auth_registry = auth_registry
        self.project = project
        self.job = job

    @classmethod
    def from_image(cls, image, name=None, meta=None, use_db=True):
        """
        Given an image name such as ``quay.io/thatpanda/dockci:latest``,
        creates a ``ServiceBase`` object.

        For a registry host to be identified, it must have both a repo
        namespace, and a repo name (otherwise Docker hub with a namespace is
        assumed).

        Examples:

        >>> svc = ServiceBase.from_image('registry/dockci', use_db=False)

        >>> svc.repo
        'registry/dockci'


        >>> svc = ServiceBase.from_image('registry/spruce/dockci', \
                                         use_db=False)
        >>> svc.base_registry
        'registry'

        >>> svc.repo
        'spruce/dockci'

        >>> svc.tag
        'latest'

        >>> svc = ServiceBase.from_image('registry/spruce/dockci:other', \
                                         use_db=False)
        >>> svc.tag
        'other'

        >>> svc = ServiceBase.from_image('dockci', 'DockCI App', use_db=False)

        >>> svc.repo
        'dockci'

        >>> svc.tag
        'latest'

        >>> svc.name
        'DockCI App'

        >>> svc = ServiceBase.from_image('registry:5000/spruce/dockci:other', \
                                         use_db=False)

        >>> svc.base_registry
        'registry:5000'

        >>> svc.tag
        'other'

        >>> svc = ServiceBase.from_image('dockci', \
                                         meta={'config': 'fake'}, \
                                         use_db=False)
        >>> svc.meta
        {'config': 'fake'}
        """
        path_parts = image.split('/', 2)
        if len(path_parts) != 3:
            base_registry = None
            repo_etc = image
        else:
            base_registry = path_parts[0]
            repo_etc = '/'.join(path_parts[1:])

        tag_parts = repo_etc.rsplit(':', 1)
        tag = None if len(tag_parts) != 2 else tag_parts[1]

        repo = tag_parts[0]

        return cls(base_registry=base_registry,
                   repo=repo,
                   tag=tag,
                   name=name,
                   meta=meta,
                   use_db=use_db,
                   )

    def clone_and_update(self, **kwargs):
        """
        Clone this ``ServiceBase``, and update some parameters

        Examples:

        >>> base = ServiceBase.from_image(
        ...     'quay.io/sprucedev/dockci:latest',
        ...     use_db=False,
        ... )

        >>> base.clone_and_update(tag='v0.0.9').display
        'quay.io/sprucedev/dockci:v0.0.9'
        >>> base.display
        'quay.io/sprucedev/dockci:latest'

        >>> clone = base.clone_and_update()
        >>> clone.tag = 'v0.0.9'
        >>> clone.display
        'quay.io/sprucedev/dockci:v0.0.9'
        >>> base.display
        'quay.io/sprucedev/dockci:latest'
        """
        final_kwargs = dict(
            name=self.name_raw,
            repo=self.repo_raw,
            tag=self.tag_raw,
            project=self.project_raw,
            job=self.job_raw,
            base_registry=self.base_registry_raw,
            auth_registry=self.auth_registry_raw,
            meta=self.meta,
            use_db=self.use_db,
        )
        final_kwargs.update(kwargs)
        return ServiceBase(**final_kwargs)

    @property
    def name_raw(self):
        """ Raw name given to this service """
        return self._name

    @property
    def name(self):
        """
        Human readable name for this service. Falls back to the repo

        Examples:

        >>> svc = ServiceBase.from_image('quay.io/spruce/dockci', use_db=False)
        >>> svc.name
        'spruce/dockci'

        >>> svc.has_name
        False

        >>> svc.name = 'Test Name'
        >>> svc.name
        'Test Name'

        >>> svc.has_name
        True

        >>> svc.display
        'Test Name - quay.io/spruce/dockci'
        """
        if self.has_name:
            return self.name_raw
        else:
            return self.repo

    @name.setter
    def name(self, value):
        """ Set the name """
        self._name = value

    @property
    def has_name(self):
        """ Whether or not a name was reliably given """
        return self.name_raw is not None

    @property
    def repo_raw(self):
        """ Raw repository given to this service """
        return self._repo

    @property
    def repo(self):
        """
        Repository for this service

        Examples:

        >>> svc = ServiceBase(base_registry='quay.io', \
                              tag='special', \
                              use_db=False)
        >>> svc.has_repo
        False

        >>> svc.repo = 'spruce/dockci'
        >>> svc.repo
        'spruce/dockci'

        >>> svc.has_repo
        True

        >>> svc.display
        'quay.io/spruce/dockci:special'
        """
        return self.repo_raw

    @repo.setter
    def repo(self, value):
        """ Set the repo """
        self._repo = value

    @property
    def has_repo(self):
        """ Whether or not a repository was reliably given """
        return self.repo_raw is not None

    @property
    def app_name(self):
        """
        Application name of the service. This is the last part of the repo,
        without the namespace path

        Examples:

        >>> svc = ServiceBase.from_image('quay.io/spruce/dockci', use_db=False)

        >>> svc.app_name
        'dockci'

        >>> svc = ServiceBase.from_image('quay.io/my/app:test', use_db=False)

        >>> svc.app_name
        'app'
        """
        if self.has_repo:
            return self.repo.rsplit('/', 1)[-1]

    @property
    def tag_raw(self):
        """ Raw tag given to this service """
        return self._tag

    @property
    def tag(self):
        """
        Tag for this service. Defaults to ``latest``

        Examples:

        >>> svc = ServiceBase.from_image('quay.io/spruce/dockci', use_db=False)

        >>> svc.tag
        'latest'

        >>> svc.has_tag
        False

        >>> svc.tag = 'special'
        >>> svc.tag
        'special'

        >>> svc.has_tag
        True

        >>> svc.display
        'quay.io/spruce/dockci:special'
        """
        if self.has_tag:
            return self.tag_raw
        else:
            return 'latest'

    @tag.setter
    def tag(self, value):
        """ Set the tag """
        self._tag = value

    @property
    def has_tag(self):
        """ Whether or not a tag was reliably given """
        return self.tag_raw is not None

    @property
    def base_registry_raw(self):
        """ Raw registry base name given to this service """
        return self._base_registry

    @property
    def base_registry(self):
        """
        A registry base name. This is the host name of the registry. Falls back
        to the authenticated registry ``base_name``, or defaults to
        ``docker.io`` if that's not given either

        Examples:

        >>> svc = ServiceBase.from_image('spruce/dockci', use_db=False)

        >>> svc.base_registry = 'docker.io'

        >>> svc.base_registry = 'quay.io'
        >>> svc.base_registry
        'quay.io'

        >>> svc.has_base_registry
        True

        >>> svc.display
        'quay.io/spruce/dockci'
        """
        return self._get_base_registry()

    @base_registry.setter
    def base_registry(self, value):
        """ Set the base_registry """
        if (
            value is not None and
            self.has_auth_registry and
            self.auth_registry.base_name != value
        ):
            raise ValueError(
                ("Existing auth_registry value '%s' doesn't match new "
                 "base_registry value") % self.auth_registry.base_name
            )

        self._base_registry = value

    def _get_base_registry(self, lookup_allow=None):
        """ Dynamically get the base_registry from other values """
        if lookup_allow is None:
            lookup_allow = defaultdict(lambda: True)

        if self.has_base_registry:
            return self.base_registry_raw

        elif lookup_allow['auth_registry']:
            lookup_allow['base_registry'] = False
            auth_registry = self._get_auth_registry(lookup_allow)
            if auth_registry is not None:
                return auth_registry.base_name

        return 'docker.io'

    @property
    def has_base_registry(self):
        """ Whether or not a registry base name was reliably given """
        return self.base_registry_raw is not None

    @property
    def auth_registry_raw(self):
        """ Raw authenticated registry given to this service """
        return self._auth_registry

    @property
    def auth_registry(self):
        """
        ``AuthenticatedRegistry`` required for this service. If not given,
        tries to lookup using the registry base name
        """
        return self._get_auth_registry()

    @auth_registry.setter
    def auth_registry(self, value):
        """ Set the auth_registry """
        if (
            value is not None and
            self.has_base_registry and
            self.base_registry != value
        ):
            raise ValueError(
                ("Existing base_registry value '%s' doesn't match new "
                 "auth_registry value") % self.base_registry
            )
        if value is not None and self.has_project:
            project = self.project
            if (
                self.project.target_registry is not None and
                self.project.target_registry != value
            ):
                raise ValueError(
                    ("Existing project target_registry value '%s' doesn't "
                     "match new auth_registry value") % project.target_registry
                )

        self._auth_registry = value

    def _get_auth_registry(self, lookup_allow=None):
        """ Dynamically get the auth_registry from other values """
        from .auth import AuthenticatedRegistry

        if lookup_allow is None:
            lookup_allow = defaultdict(lambda: True)

        # help pylint understand our return value
        if False:   # pylint:disable=using-constant-test
            return AuthenticatedRegistry()

        if self.auth_registry_raw is not None:
            return self.auth_registry_raw

        lookup_allow['auth_registry'] = False

        if (
            self.use_db and
            lookup_allow['base_registry'] and
            self.has_base_registry and
            self._auth_registry_dynamic is None
        ):
            self._auth_registry_dynamic = AuthenticatedRegistry.load(
                self._get_base_registry(lookup_allow),
            )

        if (
            lookup_allow['project'] and
            self._auth_registry_dynamic is None
        ):
            project = self._get_project()
            if project is not None:
                self._auth_registry_dynamic = project.target_registry

        if self._auth_registry_dynamic is None and self.use_db:
            self._auth_registry_dynamic = AuthenticatedRegistry.load(
                'docker.io',
            )

        return self._auth_registry_dynamic

    @property
    def has_auth_registry(self):
        """ Whether or not an authenticated registry was reliably given """
        return self._has_auth_registry()

    def _has_auth_registry(self, lookup_allow=None):
        """
        Figure out if we have a reliable ``auth_registry``source from other
        values
        """
        if self.auth_registry_raw is not None:
            return True

        if lookup_allow is None:
            lookup_allow = defaultdict(lambda: True)

        lookup_allow['has_auth_registry'] = False

        if lookup_allow['project']:
            project = self.project
            return project is not None and project.target_registry is not None

        return False

    @property
    def project_raw(self):
        """ Raw project given to this service """
        return self._project

    @property
    def project(self):
        """
        ``Project`` associated with this service. If not given, tries to lookup
        by matching the repository with the project slug. When a lookup occurs,
        and a registry is given to the service, the ``Project`` must have the
        same authenticated registry set
        """
        return self._get_project()

    @project.setter
    def project(self, value):
        """ Set the project """
        lookup_allow = defaultdict(lambda: True)
        lookup_allow['project'] = False

        if value is not None and value.target_registry is not None:
            if (
                self.has_base_registry and
                self.base_registry != value.target_registry.base_name
            ):
                raise ValueError(
                    ("Existing base_registry value '%s' doesn't match new "
                     "project target_registry value") % self.base_registry
                )

            if (
                self._has_auth_registry(lookup_allow) and
                self.auth_registry != value.target_registry
            ):
                raise ValueError(
                    ("Existing auth_registry value '%s' doesn't match new "
                     "project target_registry value") % self.auth_registry
                )

        if (
            value is not None and
            self.has_job and
            value != self.job.project
        ):
            raise ValueError(
                ("Existing job project value '%s' doesn't match new "
                 "project value") % self.job.project
            )

        self._project = value

    def _get_project(self, lookup_allow=None):
        """ Dynamically get the project from other values """
        from dockci.models.project import Project

        # help pylint understand our return value
        if False:  # pylint:disable=using-constant-test
            return Project()

        if lookup_allow is None:
            lookup_allow = defaultdict(lambda: True)

        if self.has_project:
            return self.project_raw

        lookup_allow['project'] = False

        if self._project_dynamic is None and self.use_db:
            if (
                self.has_base_registry or
                (
                    lookup_allow['has_auth_registry'] and
                    self._has_auth_registry(lookup_allow)
                )
            ):
                return None

            self._project_dynamic = Project.load(self.repo)

        return self._project_dynamic

    @property
    def has_project(self):
        """ Whether or not a project was reliably given """
        return self.project_raw is not None

    @property
    def job_raw(self):
        """ Raw job given to this service """
        return self._job

    @property
    def job(self):
        """
        ``Job`` associated with this service. If not given, tries to lookup
        by using the service project, and matching the tag

        >>> svc = ServiceBase(repo='postgres')
        >>> job = 'Fake Job'
        >>> svc.job = job
        >>> svc.job
        'Fake Job'
        """
        return self._get_job()

    @job.setter
    def job(self, value):
        """ Set the job """
        if (
            value is not None and
            self.has_project and
            value.project != self.project
        ):
            raise ValueError(
                ("Existing project value '%s' doesn't match new "
                 "job project value") % self.project
            )
        self._job = value

    def _get_job(self, lookup_allow=None):
        """ Dynamically get the job from other values """
        if lookup_allow is None:
            lookup_allow = defaultdict(lambda: True)

        if self.has_job:
            return self.job_raw

        lookup_allow['job'] = False

        if lookup_allow['project'] and self._job_dynamic is None:
            project = self._get_project(lookup_allow)
            if project is not None:
                self._job_dynamic = project.latest_job(
                    passed=True, versioned=True, tag=self.tag_raw,
                )

        return self._job_dynamic

    @property
    def has_job(self):
        """ Whether or not a job was reliably given """
        return self.job_raw is not None

    @property
    def display(self):
        """ Human readable display, hiding default elements """
        return self._display(full=False)

    @property
    def display_full(self):
        """ Human readable display, including defaults """
        return self._display(full=True)

    @property
    def image(self):
        """
        Pullable image for Docker

        Examples:

        >>> svc = ServiceBase.from_image('quay.io/spruce/dockci', use_db=False)

        >>> svc.image
        'quay.io/spruce/dockci'

        >>> svc.tag = 'latest'
        >>> svc.image
        'quay.io/spruce/dockci:latest'

        >>> svc.name = 'Test Name'
        >>> svc.image
        'quay.io/spruce/dockci:latest'
        """
        return self._display(full=False, name=False)

    @property
    def repo_full(self):
        """
        Similar to the ``repo`` property, but includes the registry

        Examples:

        >>> svc = ServiceBase.from_image('quay.io/spruce/dockci', use_db=False)

        >>> svc.repo_full
        'quay.io/spruce/dockci'

        >>> svc.tag = 'latest'
        >>> svc.repo_full
        'quay.io/spruce/dockci'

        >>> svc.name = 'Test Name'
        >>> svc.repo_full
        'quay.io/spruce/dockci'
        """
        return self._display(full=False, name=False, tag=False)

    @property
    def slug(self):
        """
        Get a slug for the service

        Examples:

        >>> svc = ServiceBase.from_image('spruce/dockci', use_db=False)

        >>> svc.slug
        'spruce_dockci'

        >>> svc.tag = 'latest'
        >>> svc.slug
        'spruce_dockci_latest'

        >>> svc.base_registry = 'registry:5000'
        >>> svc.slug
        'registry_5000_spruce_dockci_latest'
        """
        return SLUG_REPLACE_RE.sub("_", self.image)

    def _display(self, full, name=True, tag=True):
        """ Used for the display properties """
        string = ""

        if name and (full or self.has_name):
            string = "%s - " % self.name
        if full or self.has_base_registry or self.has_auth_registry:
            if self.has_auth_registry:
                string += '%s/' % self.auth_registry.base_name
            else:
                string += '%s/' % self.base_registry
        if full or self.has_repo:
            string += self.repo
        if tag and (full or self.has_tag):
            string += ":%s" % self.tag

        if string == '':
            return "No details"

        return string

    def __str__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.display)
