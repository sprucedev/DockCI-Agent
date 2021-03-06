"""
Stages in a Job
"""

import logging
import json

import requests.exceptions

from dockci.exceptions import (AlreadyRunError,
                               DockerUnreachableError,
                               StageFailedError,
                               )
from dockci.server import pika_conn, redis_pool
from dockci.stage_io import StageIO
from dockci.util import bytes_str, normalize_stream_lines, rabbit_stage_key


class JobStageBase(object):
    """
    A logged stage to a job
    """

    def __init__(self, job):
        self.job = job
        self.returncode = None

    def runnable(self, handle):
        """ Executeable portion of the stage """
        raise NotImplementedError("You must override the 'runnable' method")

    pika_conn = None
    _pika_channel = None

    @property
    def pika_channel(self):
        """ Get the RabbitMQ channel """
        if self._pika_channel is None:
            self._pika_channel = self.pika_conn.channel()

        return self._pika_channel

    @property
    def rabbit_status_key(self):
        """ RabbitMQ routing key for status messages """
        return rabbit_stage_key(self, 'status')

    def update_status(self, data):
        """ Update the job's status in the front end """
        self.pika_channel.basic_publish(
            exchange='dockci.job',
            routing_key=self.rabbit_status_key,
            body=json.dumps(data),
        )

    def update_status_complete(self, stage, success):
        """ Update the job status, and the DB """
        self.update_status(dict(
            state='done',
            success=success,
        ))
        stage.success = success
        stage.save()

    def run(self, expected_rc=0):
        """
        Start the child process, streaming it's output to the associated file,
        and block until it returns. Returns True if the return code matches
        ``expected_rc``. If ``expected_rc`` is None, always return true
        """
        # pylint:disable=no-member
        logging.getLogger('dockci.job.stages').debug(
            "Starting '%s' job stage for job '%s'",
            self.slug, self.job.slug,
        )

        if self.returncode is not None:
            raise AlreadyRunError(self)

        from dockci.models.job import JobStageTmp
        stage = JobStageTmp(job=self.job, slug=self.slug)

        with pika_conn() as pika_conn_:
            self.pika_conn = pika_conn_
            self.update_status({'state': 'starting'})
            with redis_pool() as redis_pool_:
                handle = StageIO(
                    self,
                    redis_pool=redis_pool_,
                    pika_conn=pika_conn_,
                )
                stage.save()

                try:
                    self.returncode = self.runnable(handle)

                except StageFailedError as ex:
                    if not ex.handled:
                        handle.write(("FAILED: %s\n" % ex).encode())
                        handle.flush()

                    self.update_status_complete(stage, False)
                    return False

            if expected_rc is None:
                self.update_status_complete(stage, True)
                return True

            success = self.returncode == expected_rc
            self.update_status_complete(stage, success)

        if not success:
            logging.getLogger('dockci.job.stages').debug(
                "Stage '%s' expected return of '%s'. Actually got '%s'",
                self.slug, expected_rc, self.returncode,
            )

        return success


class JobStage(JobStageBase):
    """ Ad-hoc job stage """

    def __init__(self, job, slug, runnable=None, workdir=None):
        super(JobStage, self).__init__(job)
        self.slug = slug
        self.workdir = workdir
        self._runnable = runnable

    def runnable(self, *args, **kwargs):
        """ Wrapper for runnable to avoid ambiguity """
        return self._runnable(*args, **kwargs)


class DockerStage(JobStageBase):
    """
    Wrapper around common Docker command process. Will send output lines to
    file, and optionally use callbacks to notify on each line, and
    completion
    """

    def runnable_docker(self):
        """ Commands to execute to get a Docker stream """
        raise NotImplementedError(
            "You must override the 'runnable_docker' method"
        )

    def on_line(self, line):
        """ Method called for each line in the output """
        pass

    def on_line_error(self, data):  # pylint:disable=no-self-use
        """
        Method called for each line with either 'error', or 'errorDetail' in
        the parsed output
        """
        try:
            message = data['error']
        except KeyError:
            try:
                message = data['errorDetail']['message']
            except KeyError:
                message = "Unknown error: %s" % data

        # Always handled because we output the Docker JSON first
        raise StageFailedError(handled=True, message=message)

    def on_done(self, line):
        """
        Method called when the Docker command is complete. Line given is the
        last line that Docker gave
        """
        pass

    def runnable(self, handle):
        """
        Perform the Docker command given
        """
        try:
            output = self.runnable_docker()
            self.job.docker_client.close()
        except requests.exceptions.ConnectionError as ex:
            self.job.docker_client.close()
            raise DockerUnreachableError(self.job.docker_client, ex)

        if not output:
            return 0

        line = ''
        for line in normalize_stream_lines(output):
            line_bytes, line_str = bytes_str(line)

            handle.write(line_bytes)

            if len(line_bytes) == 0:
                continue

            self.on_line(line_str)

            # Automatically handle error lines
            try:
                line_data = json.loads(line_str)
                if 'error' in line_data or 'errorDetail' in line_data:
                    self.on_line_error(line_data)

            except ValueError:
                pass

        on_done_ret = self.on_done(line_str)
        if on_done_ret is not None:
            return on_done_ret

        elif line:
            return 0

        return 1
