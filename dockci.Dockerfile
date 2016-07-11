FROM alpine:3.4

RUN apk add --no-cache \
        bash libffi libgit2 git python3
RUN pip3 install wheel virtualenv

RUN mkdir -p /code/data
WORKDIR /code

ADD requirements.txt /code/requirements.txt
ADD test-requirements.txt /code/test-requirements.txt
ADD util/wheelhouse/work /code/wheelhouse
ADD _deps_python.sh /code/_deps_python.sh
RUN ./_deps_python.sh

ADD entrypoint.sh /code/entrypoint.sh
ADD manage.py /code/manage.py
ADD dockci /code/dockci
ADD tests /code/tests
ADD pylint.conf /code/pylint.conf

ENTRYPOINT ["/code/entrypoint.sh"]
CMD ["run"]
