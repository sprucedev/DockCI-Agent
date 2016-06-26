"""
Users and permissions models
"""
from marshmallow import Schema, fields

from .base import RestModel
from dockci.server import CONFIG


class AuthenticatedRegistrySchema(Schema):
    base_name = fields.Str(default=None, allow_none=True, load_only=True)
    display_name = fields.Str(default=None, allow_none=True)
    username = fields.Str(default=None, allow_none=True)
    password = fields.Str(default=None, allow_none=True)
    email = fields.Str(default=None, allow_none=True)
    insecure = fields.Bool(default=None, allow_none=True)


class AuthenticatedRegistry(RestModel):  # pylint:disable=no-init
    """ Quick and dirty list of job stages for the time being """
    SCHEMA = AuthenticatedRegistrySchema()

    @classmethod
    def url_for(_, base_name):
        return '{dockci_url}/api/v1/registries/{base_name}'.format(
            dockci_url=CONFIG.dockci_url,
            base_name=base_name,
        )

    @property
    def url(self):
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
