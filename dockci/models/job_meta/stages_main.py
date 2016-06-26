"""
Main job stages that constitute a job
"""

import json

from dockci.models.base import ServiceBase
from dockci.models.job_meta.stages import DockerStage
from dockci.util import built_docker_image_id


class BuildStage(DockerStage):
    """
    Tell the Docker host to job
    """

    slug = 'docker_build'

    def __init__(self, job, workdir):
        super(BuildStage, self).__init__(job)
        self.workdir = workdir
        self.tag = None
        self.no_cache = None

    @property
    def dockerfile(self):
        """ Dockerfile used to build """
        return self.job.job_config.dockerfile

    def get_services(self):
        """ Services for registries required by Dockerfile FROM """
        with self.workdir.join(self.dockerfile).open() as dockerfile_handle:
            for line in dockerfile_handle:
                line = line.strip()
                if line.startswith('FROM '):
                    return [ServiceBase.from_image(
                        line[5:].strip(),
                        name='Base Image',
                    )]

        return []

    def runnable_docker(self):
        """
        Determine the image tag, and cache flag value, then trigger a Docker
        image job, returning the output stream so that DockerStage can handle
        the output
        """
        # Don't use the docker caches if a version tag is defined
        no_cache = (self.job.tag is not None)

        return self.job.docker_client.build(path=self.workdir.strpath,
                                            dockerfile=self.dockerfile,
                                            nocache=no_cache,
                                            rm=True,
                                            stream=True)

    def on_done(self, line):
        """
        Check the final line for success, and image id
        """
        if line:
            if isinstance(line, bytes):
                line = line.decode()

            line_data = json.loads(line)
            self.job.image_id = built_docker_image_id(line_data)
            if self.job.image_id is not None:
                return 0

        return 1


class TestStage(DockerStage):
    """
    Tell the Docker host to run the CI command
    """

    slug = 'docker_test'

    def runnable(self, handle):
        """
        Check if we should skip tests before handing over to the
        ``DockerStage`` runnable to execute Docker-based tests
        """
        if self.job.job_config.skip_tests:
            handle.write("Skipping tests, as per configuration".encode())
            self.job.exit_code = 0
            self.job.save()
            return 0

        return super(TestStage, self).runnable(handle)

    def runnable_docker(self):
        """
        Create a container instance, attach to its outputs and then start it,
        returning the output stream
        """
        container_details = self.job.docker_client.create_container(
            self.job.image_id, 'ci'
        )
        self.job.container_id = container_details['Id']
        self.job.save()

        def link_tuple(service_info):
            """
            Turn our provisioned service info dict into an alias string for
            Docker
            """
            if 'name' not in service_info:
                service_info['name'] = \
                    self.job.docker_client.inspect_container(
                        service_info['id']
                    )['Name'][1:]  # slice to remove the / from start

            if 'alias' not in service_info:
                if isinstance(service_info['config'], dict):
                    service_info['alias'] = service_info['config'].get(
                        'alias',
                        service_info['service'].app_name
                    )

                else:
                    service_info['alias'] = service_info['service'].app_name

            return (service_info['name'], service_info['alias'])

        stream = self.job.docker_client.attach(
            self.job.container_id,
            stream=True,
        )
        self.job.docker_client.start(
            self.job.container_id,
            links=[
                link_tuple(service_info)
                # pylint:disable=protected-access
                for service_info in self.job._provisioned_containers
            ]
        )

        return stream

    def on_done(self, _):
        """
        Check container exit code and return True on 0, or False otherwise
        """
        details = self.job.docker_client.inspect_container(
            self.job.container_id,
        )
        self.job.exit_code = details['State']['ExitCode']
        self.job.save()
        return details['State']['ExitCode']
