FROM python:3.8-slim

ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        wget \
        locales \
        time \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/share/zoneinfo/Europe/Zurich /etc/localtime

RUN pip install --no-cache-dir \
    lxml \
    requests \
    tqdm \
    pillow \
    pytz \
    pyshacl \
    python-dateutil \
    rdflib \
    tqdm \
    urllib3 \
    pillow-heif

# Install Taskfile task runner (https://taskfile.dev)
RUN sh -c "$(curl -sSL https://taskfile.dev/install.sh)" -- -d

RUN mkdir -p /scripts /data /assets

WORKDIR /scripts

VOLUME /data
VOLUME /scripts
VOLUME /assets

ENTRYPOINT ["tail", "-f", "/dev/null"]
