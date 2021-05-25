FROM postgres:12

COPY ./requirements.txt requirements.txt

RUN set -ex \
    && apt-get update \
    && apt-get install -yqq \
        build-essential \
        python3-dev \
        python3-pip \
        postgresql-server-dev-${PG_MAJOR} \
    && pip3 install --upgrade pip \
    && pip3 install -r requirements.txt \
    && pgxn install multicorn

COPY --chown=postgres:postgres ./fred_fdw fred_fdw
