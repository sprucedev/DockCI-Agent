""" Data handling for parallel test stages """

from marshmallow import Schema, fields

from .base import BaseModel


class ShardDetailSchema(Schema):
    """ Schema for loading and saving ``ShardDetail`` models """
    image_detail = fields.Str(default=None, allow_none=True)
    next_detail = fields.Str(default=None, allow_none=True)


class ShardDetail(BaseModel):
    """ Details for how to pull an image from a ``ParallelTestController`` """
    SCHEMA = ShardDetailSchema(strict=True)

    image_detail = None
    next_detail = None


class TestShardMessageSchema(Schema):
    """ Schema for loading and saving ``TestShardMessage`` models """
    shard_detail = fields.Str()
    project_slug = fields.Str()
    job_slug = fields.Str()


class TestShardMessage(BaseModel):
    """ Details for starting a parallel test shard """
    SCHEMA = TestShardMessageSchema(strict=True)

    shard_detail = None
    project_slug = None
    job_slug = None

# {"d": //nodea/req/aaaaa}
# {"d": //nodea/req/bbbbb}
# {"d": //nodea/req/ccccc}

# -- //nodea/req/aaaaa
# {"i": //nodea/im/imageid, "n": //nodea/req/bbbbb}
# -- //nodea/req/bbbbb
# {"i": //nodeb/im/imageid, "n": //nodea/req/ccccc}
# -- //nodea/req/ccccc
# {"i": //nodec/im/imageid, "n": null}
