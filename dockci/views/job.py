"""
Views related to job management
"""

import re

from flask import abort, flash, redirect, render_template, request
from flask_security import current_user, login_required

from dockci.models.job import Job
from dockci.server import APP, OAUTH_APPS
from dockci.util import model_flash, request_fill


@APP.route('/jobs/<slug>', methods=('GET', 'POST'))
def job_view(slug):
    """
    View to display a job
    """
    job = Job(slug)
    if not job.exists():
        abort(404)

    request_fill(job, ('name', 'repo', 'github_secret',
                       'hipchat_api_token', 'hipchat_room'))

    page_size = int(request.args.get('page_size', 20))
    page_offset = int(request.args.get('page_offset', 0))
    versioned = 'versioned' in request.args

    if versioned:
        builds = list(job.filtered_builds(passed=True, versioned=True))
    else:
        builds = job.builds

    prev_page_offset = max(page_offset - page_size, 0)
    if page_offset < 1:
        prev_page_offset = None

    next_page_offset = page_offset + page_size
    if next_page_offset > len(builds):
        next_page_offset = None

    builds = builds[page_offset:page_offset + page_size]
    return render_template('job.html',
                           job=job,
                           builds=builds,
                           versioned=versioned,
                           prev_page_offset=prev_page_offset,
                           next_page_offset=next_page_offset,
                           page_size=page_size)


@APP.route('/jobs/<slug>/edit', methods=('GET', 'POST'))
@login_required
def job_edit_view(slug):
    """
    View to edit a job
    """
    job = Job(slug)
    if not job.exists():
        abort(404)

    return job_input_view(job, 'edit', (
        'name', 'repo', 'github_secret', 'github_repo_id',
        'hipchat_api_token', 'hipchat_room',
    ))


@APP.route('/jobs/new', methods=('GET', 'POST'))
@login_required
def job_new_view():
    """
    View to make a new job
    """
    job = Job()
    return job_input_view(job, 'new', (
        'slug', 'name', 'repo', 'github_secret', 'github_repo_id',
        'hipchat_api_token', 'hipchat_room',
    ))


def job_input_view(job, edit_operation, fields):
    """ Generic view for job editing """
    if request.method == 'POST':
        old_github_repo_id = job.github_repo_id
        try:
            old_github_api_hook_endpoint = job.github_api_hook_endpoint
        except ValueError:
            old_github_api_hook_endpoint = None

        saved = request_fill(
            job, fields,
            save=request.args.get('repo_type', None) != 'github',
        )

        if request.args.get('repo_type', None) == 'github':
            valid = model_flash(job, save=False)
            if valid:
                result = None
                # Repo ID hasn't changed
                if old_github_repo_id == job.github_repo_id:
                    if not job.github_hook_id:
                        result = job.add_github_webhook()

                else:
                    if old_github_api_hook_endpoint:
                        # TODO check this
                        OAUTH_APPS['github'].delete(
                            old_github_api_hook_endpoint
                        )

                    result = job.add_github_webhook()

                if result is not None:
                    saved = result.status == 201
                    if result.status != 201:
                        flash(result.data.get(
                            'message',
                            ("Unexpected response from GitHub. "
                             "HTTP status %d") % result.status
                        ), 'danger')
                else:
                    job.save()
                    saved = True

            else:
                saved = False

        if saved:
            return redirect('/jobs/{job_slug}'.format(job_slug=job.slug))

    if 'repo_type' in request.args:
        default_repo_type = request.args['repo_type']

    elif current_user is None or not current_user.is_authenticated():
        default_repo_type = 'manual'

    elif 'github' in current_user.oauth_tokens:
        default_repo_type = 'github'

    else:
        default_repo_type = 'manual'

    re.sub(r'[^\w\s]', '', default_repo_type)

    return render_template('job_edit.html',
                           job=job,
                           edit_operation=edit_operation,
                           default_repo_type=default_repo_type,
                           )
