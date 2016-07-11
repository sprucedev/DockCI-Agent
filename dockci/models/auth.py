"""
Users and permissions models
"""
from marshmallow import Schema, fields

from dockci.server import CONFIG
from .base import RestModel


class AuthenticatedRegistrySchema(Schema):
    """ Schema for loading and saving ``AuthenticatedRegistry`` models """
    base_name = fields.Str(default=None, allow_none=True, load_only=True)
    display_name = fields.Str(default=None, allow_none=True)
    username = fields.Str(default=None, allow_none=True)
    password = fields.Str(default=None, allow_none=True)
    email = fields.Str(default=None, allow_none=True)
    insecure = fields.Bool(default=None, allow_none=True)


class AuthenticatedRegistry(RestModel):
    """ Quick and dirty list of job stages for the time being """
    SCHEMA = AuthenticatedRegistrySchema()

    base_name = None
    display_name = None
    username = None
    password = None
    email = None
    insecure = None

    @classmethod
    def url_for(cls, base_name):  # pylint:disable=arguments-differ
        """ Generate the absolute URL to load a registry from

        :param base_name: Base name for the registry
        :type base_name: str

        :return str: Absolute URL for registry with base name

        Examples:

          >>> CONFIG.dockci_url = 'http://dockcitest'
          >>> AuthenticatedRegistry.url_for('docker.io')
          'http://dockcitest/api/v1/registries/docker.io'
        """
        return '{dockci_url}/api/v1/registries/{base_name}'.format(
            dockci_url=CONFIG.dockci_url,
            base_name=base_name,
        )

    @property
    def url(self):
        """ URL for this registry """
        return AuthenticatedRegistry.url_for(self.base_name)

    def __str__(self):
        return '<{klass}: {base_name} ({username})>'.format(
            klass=self.__class__.__name__,
            base_name=self.base_name,
            username=self.username,
        )

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(tuple(
            (attr_name, getattr(self, attr_name))
            for attr_name in (
                'display_name', 'base_name',
                'username', 'password', 'email',
                'insecure',
            )
        ))
