FROM postgres:12

RUN set -ex \
    && apt-get update \
    && apt-get install -yqq \
        build-essential \
        python3-dev \
        python3-pip \
        postgresql-server-dev-${PG_MAJOR} \
    && pip3 install --upgrade pip pgxnclient setuptools \
    && pgxn install multicorn

COPY --chown=postgres:postges . .

RUN python3 setup.py install
