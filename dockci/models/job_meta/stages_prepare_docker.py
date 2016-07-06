""" Docker-based preparation for the main job stages """

import json
import tarfile

from collections import defaultdict

import docker
import docker.errors
import py.error  # pylint:disable=import-error
import py.path  # pylint:disable=import-error

from dockci.exceptions import (AlreadyBuiltError,
                               DockerAPIError,
                               StageFailedError,
                               )
from dockci.models.base import ServiceBase
from dockci.models.blob import FilesystemBlob
from dockci.models.job_meta.stages import JobStageBase
from dockci.server import CONFIG
from dockci.util import (built_docker_image_id,
                         docker_ensure_image,
                         IOFauxDockerLog,
                         normalize_stream_lines,
                         path_contained,
                         )


class InlineProjectStage(JobStageBase):
    """ Stage to run project containers inline in another project job """
    def get_services(self):
        """ Get the services associated with the projects in this stage """
        raise NotImplementedError(
            "You must override the 'get_services' method"
        )

    def id_for_service(self, slug):  # pylint:disable=no-self-use
        """ Get the event series ID for a given service's slug """
        return slug

    def runnable(self, handle):
        """
        Resolve project containers, and pass control to ``runnable_inline``
        """
        all_okay = True
        faux_log = IOFauxDockerLog(handle)
        for service in self.get_services():

            # pylint:disable=no-member
            defaults = {'id': self.id_for_service(service.app_name)}
            with faux_log.more_defaults(**defaults):

                defaults = {'status': "Finding service %s" % service.display}
                with faux_log.more_defaults(**defaults):
                    faux_log.update()

                    service_project = service.project
                    service_job = service.job

                    if service_project is None:
                        faux_log.update(progress="No project found")

                    elif service_job is None:
                        faux_log.update(
                            error="No successful, versioned job for %s" % (
                                service_project.name
                            ),
                        )
                        all_okay = False
                        continue

                    else:
                        service.tag = service_job.tag

                defaults = {'status': "Pulling container image %s" % (
                    service.display,
                )}
                with faux_log.more_defaults(**defaults):
                    faux_log.update()

                    auth_registry = service.auth_registry
                    if auth_registry is not None:
                        ensure_kwargs = dict(
                            insecure_registry=service.auth_registry.insecure,
                        )
                    else:
                        ensure_kwargs = {}

                    try:
                        image_id = docker_ensure_image(
                            self.job.docker_client,
                            service,
                            handle=handle,
                            **ensure_kwargs
                        )

                    except docker.errors.APIError as ex:
                        faux_log.update(error=ex.explanation.decode())
                        all_okay = False
                        continue

                    if image_id is None:
                        faux_log.update(error="Not found")
                        all_okay = False
                        continue

                    faux_log.update(progress="Done")

                all_okay &= self.runnable_inline(
                    service,
                    image_id,
                    handle,
                    faux_log,
                )

        return 0 if all_okay else 1

    def runnable_inline(self, service, image_id, handle, faux_log):
        """ Executed for each service """
        raise NotImplementedError(
            "You must override the 'runnable_inline' method"
        )


class PushPrepStage(JobStageBase):
    """ Ensure versioned tags haven't already been built """
    slug = 'docker_push_prep'

    def set_old_image_ids(self, handle):
        """
        Set the ``_old_image_ids`` attribute on the job so that cleanup knows
        to remove other images that this job replaces
        """
        possible_tags_set = {
            self.job.service.clone_and_update(tag=tag).image
            for tag in self.job.possible_tags_set
        }
        tags_set = {
            self.job.service.clone_and_update(tag=tag).image
            for tag in self.job.tags_set
        }

        # This should work when docker/docker#18181 is fixed
        # self.job.docker_client.images(name=self.job.docker_image_name)

        # pylint:disable=protected-access

        for image in self.job.docker_client.images():
            matched_tags = {
                tag for tag in possible_tags_set
                if tag in image['RepoTags']
            }
            if matched_tags:
                handle.write((
                    "Matched tags; will replace image '%s':\n" % image['Id']
                ).encode())
                for tag in matched_tags:
                    handle.write(("  %s\n" % tag).encode())
                handle.flush()

                repo_tags_set = set(image['RepoTags'])

                # All this job's possible tags that are on the image
                # Later, we clean up by removing the tags that our new image
                #   will be tagged with
                to_cleanup = repo_tags_set & possible_tags_set

                self.job._old_image_ids.extend(list(to_cleanup))

                # If we're removing all the tags, delete the image too
                if repo_tags_set.issubset(possible_tags_set):
                    self.job._old_image_ids.append(image['Id'])
                    handle.write(
                        "  No tags remain; deleting the image too\n".encode())
                else:
                    handle.write(
                        "  Tags remain; won't delete the image\n".encode())
                    for tag in repo_tags_set - possible_tags_set:
                        handle.write(("    %s\n" % tag).encode())

                handle.flush()

        # Don't immediately delete our own tags
        for tag in tags_set:
            try:
                while True:  # Remove tags until ValueError
                    self.job._old_image_ids.remove(tag)
            except ValueError:
                pass

    def check_existing_job(self, handle):
        """ Check the tag to see if there's a job already built """
        handle.write("Checking for previous job... ".encode())
        handle.flush()

        for tag in self.job.tag_tags_set:
            job = self.job.project.latest_job(
                passed=True,
                tag=tag,
            )

            if job is not None:
                raise AlreadyBuiltError(
                    'Version %s of %s already built' % (
                        self.job.tag,
                        self.job.project.slug,
                    )
                )

        handle.write("OKAY!\n".encode())
        handle.flush()

    def runnable(self, handle):
        if self.job.tag_push_candidate:
            self.check_existing_job(handle)

        self.set_old_image_ids(handle)


class ProvisionStage(InlineProjectStage):
    """
    Provision the services that are required for this job
    """

    slug = 'docker_provision'

    def get_services(self):
        return [
            ServiceBase.from_image(conf['name'],
                                   name=conf.get('alias', None),
                                   meta={'config': conf},
                                   )
            for conf in self.job.job_config.services
        ]

    def runnable_inline(self, service, image_id, handle, faux_log):
        defaults = {'status': "Starting service %s" % service.display}
        with faux_log.more_defaults(**defaults):
            faux_log.update()

            service_config = service.meta['config']

            service_kwargs = {
                key: value for key, value in service_config.items()
                if key in ('command', 'environment')
            }

            try:
                container = self.job.docker_client.create_container(
                    image=image_id,
                    **service_kwargs
                )
                self.job.docker_client.start(container['Id'])

                # Store the provisioning info
                # pylint:disable=protected-access
                self.job._provisioned_containers.append({
                    'service': service,
                    'config': service_config,
                    'id': container['Id'],
                })
                faux_log.update(progress="Done")

            except docker.errors.APIError as ex:
                faux_log.update(error=ex.explanation.decode())
                return False

        return True


class UtilStage(InlineProjectStage):
    """ Create, and run a utility stage container """
    def __init__(self, job, workdir, slug_suffix, config):
        super(UtilStage, self).__init__(job)
        self.workdir = workdir
        self.slug = "utility_%s" % slug_suffix
        self.config = config

    def get_services(self):
        return [
            ServiceBase.from_image(self.config['name'],
                                   meta={'config': self.config},
                                   )
        ]

    def add_files(self, base_image_id, faux_log):
        """
        Add files in the util config to a temporary image that will be used for
        running the util

        Args:
          base_image_id (str): Image ID to use in the Dockerfile FROM
          faux_log: The faux docker log object

        Returns:
          str: New image ID with files added
          bool: False if failure
        """
        input_files = self.config['input']

        success = True
        if not input_files:
            faux_log.update(progress="Skipped")
            return base_image_id

        # Create the temp Dockerfile
        tmp_file = py.path.local.mkdtemp(self.workdir).join("Dockerfile")
        with tmp_file.open('w') as h_dockerfile:
            h_dockerfile.write('FROM %s\n' % base_image_id)
            for file_line in input_files:
                h_dockerfile.write('ADD %s %s\n' % (
                    file_line['from'],
                    file_line['to'],
                ))

        # Run the build
        rel_workdir = self.workdir.bestrelpath(tmp_file)
        output = self.job.docker_client.build(
            path=self.workdir.strpath,
            dockerfile=rel_workdir,
            nocache=True,
            rm=True,
            forcerm=True,
            stream=True,
        )

        # Watch for errors
        for line in normalize_stream_lines(output):
            if len(line) == 0:
                continue
            data = json.loads(line)
            if 'errorDetail' in data:
                faux_log.update(**data)
                success = False

        self.job.docker_client.close()

        if success:
            image_id = built_docker_image_id(data)
            if image_id is None:
                faux_log.update(status="Couldn't determine new image ID",
                                progress="Failed")
                return False

            faux_log.update(progress="Done")
            return image_id

        else:
            faux_log.update(progress="Failed")
            return False

    def run_util(self, image_id, handle, faux_log):
        """
        Run the temp util image with the config command, and output the stream
        to the given file handle

        Args:
          image_id (str): New util image to run, with files added
          handle: File-like object to stream the Docker output to
          faux_log: The faux docker log object

        Returns:
          tuple(str, bool): Container ID, and success/fail
        """
        service_kwargs = {
            key: value for key, value in self.config.items()
            if key in ('command', 'environment')
        }
        container = {}
        try:
            container = self.job.docker_client.create_container(
                image=image_id,
                **service_kwargs
            )
            stream = self.job.docker_client.attach(
                container['Id'],
                stream=True,
            )
            self.job.docker_client.start(container['Id'])

        except docker.errors.APIError as ex:
            faux_log.update(error=ex.explanation.decode())
            return container.get('Id', None), False

        for line in stream:
            if isinstance(line, bytes):
                handle.write(line)
            else:
                handle.write(line.encode())

            handle.flush()

        return container['Id'], True

    def retrieve_files(self, container_id, faux_log):
        """
        Retrieve the files in the job config from the utility container

        Args:
          container_id (str): ID of a container to copy files from. Most likely
            the completed utility container
          faux_log: The faux docker log object

        Returns:
          bool: True when all files retrieved as expected, False otherwise
        """
        output_files = self.config['output']
        success = True
        if not output_files:
            faux_log.update(id="output", progress="Skipped")

        for output_idx, output_set in enumerate(output_files):
            if isinstance(output_set, dict):
                try:
                    remote_spath = output_set['from']
                except KeyError:
                    defaults = {
                        'id': "output.%s" % output_idx,
                        'progress': "Failed",
                    }
                    with faux_log.more_defaults(**defaults):
                        faux_log.update(status="Reading configuration")
                        faux_log.update(error="No required 'from' parameter")
                    success = False
                    continue

                local_spath = output_set.get('to', '.')
            else:
                local_spath = '.'
                remote_spath = output_set

            defaults = {
                'id': "output.%s" % local_spath,
                'status': "Copying from '%s'" % remote_spath,
            }
            with faux_log.more_defaults(**defaults):
                faux_log.update()
                local_path = self.workdir.join(local_spath)
                if not path_contained(self.workdir, local_path):
                    faux_log.update(
                        error="Path not contained within the working "
                              "directory",
                        progress="Failed",
                    )
                    success = False
                    continue

                response = self.job.docker_client.copy(
                    container_id, remote_spath
                )

                intermediate = tarfile.open(name='output.tar',
                                            mode='r|',
                                            fileobj=response)
                intermediate.extractall(local_path.strpath)

                faux_log.update(progress="Done")

        return success

    def cleanup(self,
                base_image_id,
                image_id,
                container_id,
                faux_log,
                ):
        """
        Cleanup after the util stage is done processing. Removes the contanier,
        and temp image. Doesn't remove the image if it hasn't changed from the
        base image

        Args:
          base_image_id (str): Original ID of the utility base image
          image_id (str): ID of the image used by the utility run
          container_id (str): ID of the container the utility run created
          faux_log: The faux docker log object

        Returns:
          bool: Whether the cleanup was successful or not
        """
        def cleanup_container():
            """ Remove the container """
            self.job.docker_client.remove_container(container_id)
            return True

        def cleanup_image():
            """ Remove the image, unless it's base """
            if image_id is None:
                return False
            min_len = min(len(base_image_id), len(image_id))
            if base_image_id[:min_len] == image_id[:min_len]:
                return False

            self.job.docker_client.remove_image(image_id)
            return True

        success = True
        cleanups = (
            ('container', cleanup_container, container_id),
            ('image', cleanup_image, image_id),
        )
        for obj_name, func, obj_id in cleanups:
            defaults = {
                'id': "cleanup.%s" % obj_id,
                'status': "Cleaning up %s" % obj_name
            }
            with faux_log.more_defaults(**defaults):
                faux_log.update()
                try:
                    done = func()
                    faux_log.update(
                        progress="Done" if done else "Skipped"
                    )

                except docker.errors.APIError as ex:
                    faux_log.update(error=ex.explanation.decode())
                    success = False

        return success

    def generate_data(self, service, base_image_id, handle, faux_log):
        """
        Adds files, runs the container, retrieves output, and cleans up
        """
        defaults = {
            'id': "input",
            'status': "Adding files",
        }
        with faux_log.more_defaults(**defaults):
            faux_log.update()
            image_id = self.add_files(base_image_id, faux_log)
            if image_id is False:
                return False

        container_id = None
        success = True
        try:
            defaults = {'status': "Starting utility %s" % service.display}
            with faux_log.more_defaults(**defaults):
                faux_log.update()
                container_id, success = self.run_util(
                    image_id, handle, faux_log,
                )

            if success:
                with faux_log.more_defaults(id="cleanup"):
                    faux_log.update(status="Collecting status")
                    exit_code = self.job.docker_client.inspect_container(
                        container_id
                    )['State']['ExitCode']

                if exit_code != 0:
                    faux_log.update(
                        id="exit",
                        error="Exit code was %d" % exit_code
                    )
                    success = False

            if success:
                defaults = {'status': "Getting files"}
                with faux_log.more_defaults(**defaults):
                    faux_log.update()
                    success = success & self.retrieve_files(
                        container_id, faux_log,
                    )

        except Exception:
            self.cleanup(base_image_id,
                         image_id,
                         container_id,
                         faux_log,
                         )
            raise

        else:
            success = success & self.cleanup(base_image_id,
                                             image_id,
                                             container_id,
                                             faux_log,
                                             )

        return success

    def runnable_inline(self, service, base_image_id, handle, faux_log):
        """
        Inline runner for utility projects

        Args:
          service (dockci.models.base.ServiceBase): Service that this stage
            uses the image from
          base_image_id (str): Image ID of the utility base
          handle: Stream handle for raw output
          faux_log: The faux docker log object

        Returns:
          bool: True on all success, False on at least 1 failure
        """
        input_files = self.config['input']
        output_files = self.config['output']
        blob_store = None

        if input_files:
            blob_store = FilesystemBlob.from_files(
                CONFIG.blob_path,
                self.workdir,
                [
                    self.workdir.join(input_data['from'])
                    for input_data
                    in input_files
                ],
                meta={'image': service.image},
            )

            for output_data in output_files:
                blob_store.add_data(output_data['to'])

        if blob_store and blob_store.exists:
            blob_store.extract()
            return True

        else:
            ret = self.generate_data(service, base_image_id, handle, faux_log)
            if ret and blob_store:
                blob_store.write()
            return ret

    @classmethod
    def slug_suffixes(cls, utility_names):
        """ See ``slug_suffixes_gen`` """
        return list(cls.slug_suffixes_gen(utility_names))

    @classmethod
    def slug_suffixes_gen(cls, utility_names):
        """
        Generate utility names into unique slug suffixes by adding a counter to
        the end, if there are duplicates
        """
        totals = defaultdict(int)
        for name in utility_names:
            totals[name] += 1

        counters = defaultdict(int)
        for name in utility_names:
            if totals[name] > 1:
                counters[name] += 1
                yield '%s_%d' % (name, counters[name])

            else:
                yield name


class DockerLoginStage(JobStageBase):
    """ Find, and login to registries that have auth config """
    slug = 'docker_login'

    def __init__(self, job, workdir):
        super(DockerLoginStage, self).__init__(job)
        self.workdir = workdir

    def login_registry(self, handle, username, password, email, base_name):
        """ Handle login to the given registry model """
        err = None
        try:
            response = self.job.docker_client.login(
                username=username,
                password=password,
                email=email,
                registry=base_name,
            )
            handle.write(('%s\n' % response['Status']).encode())
            handle.flush()

        except KeyError:
            err = "Unknown response: %s" % response

        except docker.errors.APIError as ex:
            err = str(DockerAPIError(
                self.job.docker_client, ex,
            ))

        if err:
            handle.write(('FAILED: %s\n' % err).encode())
            handle.flush()

            raise StageFailedError(
                message=err,
                handled=True,
            )

    def handle_registry(self, handle, base_name, registry):
        """ Handle login if necessary """
        auth_registry = (
            registry is not None and (
                registry.username is not None or
                registry.password is not None or
                registry.email is not None
            )
        )

        if auth_registry:
            handle.write(("Logging into '%s' registry: " % (
                registry.display_name,
            )).encode())
            handle.flush()
            self.login_registry(
                handle,
                registry.username,
                registry.password,
                registry.email,
                base_name,
            )

        else:
            display_name = registry.display_name if registry else base_name
            handle.write(("Unauthenticated for '%s' registry\n" % (
                display_name,
            )).encode())
            handle.flush()

    def runnable(self, handle):
        """ Load the Dockerfile, scan for FROM line, login """
        registries = {}
        # pylint:disable=protected-access
        for stage in self.job._stage_objects.values():
            if hasattr(stage, 'get_services'):
                for service in stage.get_services():
                    registry_value = registries.setdefault(
                        service.base_registry, None,
                    )
                    registry = service.auth_registry
                    if registry_value is None and registry is not None:
                        registries[service.base_registry] = registry

        for base_name, registry in registries.items():
            self.handle_registry(handle, base_name, registry)

        return 0
