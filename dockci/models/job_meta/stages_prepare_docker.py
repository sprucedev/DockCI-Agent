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
from dockci.models.auth import AuthenticatedRegistry
from dockci.models.job_meta.stages import JobStageBase
from dockci.models.project import Project
from dockci.util import (base_name_from_image,
                         built_docker_image_id,
                         docker_ensure_image,
                         FauxDockerLog,
                         path_contained,
                         )


class InlineProjectStage(JobStageBase):
    """ Stage to run project containers inline in another project job """
    _projects = None

    def get_project_slugs(self):
        """ Get all project slugs that relate to this inline stage """
        raise NotImplementedError(
            "You must override the 'get_project_slugs' method"
        )

    def get_projects(self):
        """
        Get all the project model objects that relate to this inline stage
        """
        if self._projects is None:
            self._projects = {
                slug: Project.query.filter_by(slug=slug).first()
                for slug in self.get_project_slugs()
            }

        return self._projects

    def id_for_project(self, project_slug):
        """ Get the event series ID for a given project's slug """
        # pylint:disable=no-member
        return '%s_%s' % (self.slug, project_slug)

    def runnable(self, handle):
        """
        Resolve project containers, and pass control to ``runnable_inline``
        """
        all_okay = True
        faux_log = FauxDockerLog(handle)
        for project_slug, service_project in self.get_projects().items():

            # pylint:disable=no-member
            defaults = {'id': self.id_for_project(project_slug)}
            with faux_log.more_defaults(**defaults):

                defaults = {'status': "Finding service %s" % project_slug}
                with faux_log.more_defaults(**defaults):
                    faux_log.update()

                    if not service_project:
                        faux_log.update(error="No project found")
                        all_okay = False
                        continue

                    service_job = service_project.latest_job(passed=True,
                                                             versioned=True)
                    if not service_job:
                        faux_log.update(
                            error="No successful, versioned job for %s" % (
                                service_project.name
                            ),
                        )
                        all_okay = False
                        continue

                defaults = {'status': "Pulling container image %s:%s" % (
                    service_job.docker_image_name, service_job.tag
                )}
                with faux_log.more_defaults(**defaults):
                    faux_log.update()

                    try:
                        image_id = docker_ensure_image(
                            self.job.docker_client,
                            service_job.image_id,
                            service_job.docker_image_name,
                            service_job.tag,
                            insecure_registry=(
                                service_job.project.target_registry.insecure
                            ),
                            handle=handle,
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
                    service_job,
                    image_id,
                    handle,
                    faux_log,
                )

        return 0 if all_okay else 1

    def runnable_inline(self, service_job, image_id, handle, faux_log):
        """ Executed for each service job """
        raise NotImplementedError(
            "You must override the 'get_project_slugs' method"
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
            '%s:%s' % (self.job.docker_image_name, tag)
            for tag in self.job.possible_tags_set
        }
        tags_set = {
            '%s:%s' % (self.job.docker_image_name, tag)
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
                to_cleanup = repo_tags_set.intersection(possible_tags_set)

                self.job._old_image_ids.extend(list(to_cleanup))

                # If we're removing all the tags, delete the image too
                if repo_tags_set.issubset(possible_tags_set):
                    self.job._old_image_ids.append(image['Id'])

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

        from dockci.models.job import Job, JobResult
        job_count = self.job.project.jobs.filter(
            Job.result.in_((JobResult.success.value, None)),
            Job.tag.in_(self.job.tag_tags_set),
        ).count()

        if job_count:
            raise AlreadyBuiltError(
                'Version %s of %s already built' % (
                    self.job.tag,
                    self.job.project.slug,
                )
            )
        else:
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

    def get_project_slugs(self):
        return set(self.job.job_config.services.keys())

    def runnable_inline(self, service_job, image_id, handle, faux_log):
        service_project = service_job.project
        service_config = self.job.job_config.services[service_project.slug]

        defaults = {'status': "Starting service %s %s" % (
            service_project.name,
            service_job.tag,
        )}
        with faux_log.more_defaults(**defaults):
            faux_log.update()

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
                    'project_slug': service_project.slug,
                    'config': service_config,
                    'id': container['Id']
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

    def get_project_slugs(self):
        return (self.config['name'],)

    def id_for_project(self, project_slug):
        return project_slug

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
        success = True

        input_files = self.config.get('input', ())
        if not input_files:
            faux_log.update(progress="Skipped")
            return base_image_id

        # Create the temp Dockerfile
        tmp_file = py.path.local.mkdtemp(self.workdir).join("Dockerfile")
        with tmp_file.open('w') as h_dockerfile:
            h_dockerfile.write('FROM %s\n' % base_image_id)
            for file_line in input_files:
                h_dockerfile.write('ADD %s\n' % file_line)

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
        for line in output:
            data = json.loads(line.decode())
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

    def retrieve_files(self, container_id, faux_log, files_id):
        """
        Retrieve the files in the job config from the utility container

        Args:
          container_id (str): ID of a container to copy files from. Most likely
            the completed utility container
          faux_log: The faux docker log object
          files_id: Log ID for the output retrieval stage. Used as both an ID,
            and a prefix

        Returns:
          bool: True when all files retrieved as expected, False otherwise
        """
        output_files = self.config.get('output', [])
        success = True
        if not output_files:
            faux_log.update(id=files_id, progress="Skipped")

        for output_idx, output_set in enumerate(output_files):
            if isinstance(output_set, dict):
                try:
                    remote_spath = output_set['from']
                except KeyError:
                    defaults = {
                        'id': '%s-%s' % (files_id, output_idx),
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
                'id': '%s-%s' % (files_id, local_spath),
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
                cleanup_id,
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
          cleanup_id (str): Base ID for the faux_log

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
                'id': '%s-%s' % (cleanup_id, obj_id),
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

    def runnable_inline(self, service_job, base_image_id, handle, faux_log):
        """
        Inline runner for utility projects. Adds files, runs the container,
        retrieves output, and cleans up

        Args:
          service_job (dockci.models.job.Job): External Job model that this
            stage uses the image from
          base_image_id (str): Image ID of the versioned job to use
          handle: Stream handle for raw output
          faux_log: The faux docker log object

        Returns:
          bool: True on all success, False on at least 1 failure
        """
        utility_project = service_job.project

        defaults = {
            'id': "%s-input" % self.id_for_project(utility_project.slug),
            'status': "Adding files",
        }
        with faux_log.more_defaults(**defaults):
            faux_log.update()
            image_id = self.add_files(base_image_id, faux_log)
            if image_id is False:
                return False

        container_id = None
        success = True
        cleanup_id = "%s-cleanup" % self.id_for_project(utility_project.slug)
        try:
            defaults = {'status': "Starting %s utility %s" % (
                utility_project.name,
                service_job.tag,
            )}
            with faux_log.more_defaults(**defaults):
                faux_log.update()
                container_id, success = self.run_util(
                    image_id, handle, faux_log,
                )

            if success:
                with faux_log.more_defaults(id=cleanup_id):
                    faux_log.update(status="Collecting status")
                    exit_code = self.job.docker_client.inspect_container(
                        container_id
                    )['State']['ExitCode']

                if exit_code != 0:
                    faux_log.update(
                        id="%s-exit" % self.id_for_project(
                            utility_project.slug,
                        ),
                        error="Exit code was %d" % exit_code
                    )
                    success = False

            if success:
                files_id = (
                    "%s-output" % self.id_for_project(utility_project.slug))
                defaults = {'status': "Getting files"}
                with faux_log.more_defaults(**defaults):
                    faux_log.update()
                    success = success & self.retrieve_files(
                        container_id, faux_log, files_id,
                    )

        except Exception:
            self.cleanup(base_image_id,
                         image_id,
                         container_id,
                         faux_log,
                         cleanup_id,
                         )
            raise

        else:
            success = success & self.cleanup(base_image_id,
                                             image_id,
                                             container_id,
                                             faux_log,
                                             cleanup_id,
                                             )

        return success

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

    def handle_registry(self, handle, base_name_or_reg):
        """
        Find the given registry, and handle login. If ``base_name`` is
        ``None``, lookup entry ``docker.io`` registry model
        """
        if isinstance(base_name_or_reg, AuthenticatedRegistry):
            registry = base_name_or_reg
            base_name = (None
                         if registry.base_name == 'docker.io'
                         else registry.base_name)
        else:
            base_name = base_name_or_reg
            query = self.job.db_session.query(AuthenticatedRegistry)
            registry = query.filter_by(
                base_name=base_name or 'docker.io',
            ).first()

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
                display_name or 'docker.io',
            )).encode())
            handle.flush()

    def registries_from_dockerfile(self):
        """ Registry set for registries required by Dockerfile FROM """
        dockerfile = self.workdir.join(self.job.job_config.dockerfile)
        with dockerfile.open() as dockerfile_handle:
            for line in dockerfile_handle:
                line = line.strip()
                if line.startswith('FROM '):
                    return {base_name_from_image(line[5:].strip())}

        return set()

    def registries_from_inline_stages(self):
        """ Registry set for registries required by utilities """
        full_set = set()
        # pylint:disable=protected-access
        for stage in self.job._stage_objects.values():
            if isinstance(stage, InlineProjectStage):
                full_set.update((
                    project.target_registry
                    for project in stage.get_projects().values()
                    if (
                        project is not None and
                        project.target_registry is not None
                    )
                ))

        return full_set

    def registries_from_push(self):
        """ Registry set for registries required in project push """
        if self.job.push_candidate:
            return {self.job.project.target_registry}

        return set()

    def runnable(self, handle):
        """ Load the Dockerfile, scan for FROM line, login """
        registry_set = set.union(
            self.registries_from_dockerfile(),
            self.registries_from_inline_stages(),
            self.registries_from_push(),
        )

        # Dedupe AuthenticatedRegistry objects with base_name strings
        for base_name_or_reg in registry_set.copy():
            if isinstance(base_name_or_reg, AuthenticatedRegistry):
                try:
                    if base_name_or_reg.base_name == 'docker.io':
                        registry_set.remove(None)
                    else:
                        registry_set.remove(base_name_or_reg.base_name)
                except KeyError:
                    pass

        for base_name_or_reg in registry_set:
            self.handle_registry(handle, base_name_or_reg)

        return 0