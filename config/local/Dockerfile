FROM python:3.10
ENV PYTHONUNBUFFERED 1

RUN mkdir -p /usr/src/app && \
    mkdir -p /usr/src/config

# Install Poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | POETRY_HOME=/opt/poetry python && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

COPY config/local/pyproject.toml ./

# Allow installing dev dependencies to run tests
ARG INSTALL_DEV=false
RUN bash -c "if [ $INSTALL_DEV == 'true' ] ; then poetry install --no-root ; else poetry install --no-root --no-dev ; fi"

WORKDIR /usr/src/app

COPY ./src .
COPY config/local /usr/src/config

ENTRYPOINT ["../config/start_app.sh"]