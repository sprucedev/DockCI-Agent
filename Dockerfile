FROM debian:jessie

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y \
        git libffi-dev libgit2-dev locales \
        python3 python3-dev python3-setuptools
RUN easy_install3 pip wheel virtualenv
RUN ln -s $(which nodejs) /usr/bin/node

RUN echo 'en_AU.UTF-8 UTF-8' > /etc/locale.gen && locale-gen && update-locale LANG=en_AU.UTF-8
ENV LANG en_AU.UTF-8

RUN mkdir -p /code/data
WORKDIR /code

ENV WHEELS_ONLY=1
ADD requirements.txt /code/requirements.txt
ADD test-requirements.txt /code/test-requirements.txt
ADD _deps_python.sh /code/_deps_python.sh
RUN ./_deps_python.sh

ADD entrypoint.sh /code/entrypoint.sh
ADD manage.py /code/manage.py
ADD dockci /code/dockci
ADD tests /code/tests
ADD pylint.conf /code/pylint.conf

ENTRYPOINT ["/code/entrypoint.sh"]
CMD ["run"]
