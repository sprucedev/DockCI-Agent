FROM alpine:3.4

ENV DEBIAN_FRONTEND noninteractive
RUN apk add --no-cache \
        libffi-dev libgit2-dev \
        bash python3 python3-dev
RUN apk add --no-cache gcc musl-dev
RUN pip3 install wheel virtualenv

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
