# syntax=docker/dockerfile:1.7
#
# olefied — production olefy: pooled workers + scan timeout + backpressure.
# See docker/olefyd.py for the runtime design and README.md for the throughput model.
#
# Multi-stage: build the venv with a toolchain, ship only the runtime + venv.
# glibc base (not alpine) so oletools/lxml/msoffcrypto install from manylinux
# wheels — no cargo/gcc in the final image, smaller attack surface, faster builds.

ARG PYTHON_VERSION=3.13
# Upstream olefy is pulled fresh at build time (not vendored), so every build
# tracks Heinlein's latest. Pin OLEFY_REF to a tag/commit if you ever want a
# reproducible build; bump CACHEBUST (or build --no-cache) to force a re-pull
# when OLEFY_REF is a moving branch.
ARG OLEFY_REF=master
ARG CACHEBUST=0

# ---- builder ---------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-trixie AS builder
ARG OLEFY_REF
ARG CACHEBUST

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Toolchain only for the rare sdist; wheels cover the common case. curl pulls
# olefy.py + requirements.txt from upstream.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get update && \
    apt-get install -y --no-install-recommends gcc libffi-dev curl ca-certificates

# Fetch upstream olefy.py + requirements.txt FRESH each build (CACHEBUST busts
# the layer cache so a moving OLEFY_REF is actually re-pulled).
RUN echo "cachebust=${CACHEBUST}" && \
    base="https://raw.githubusercontent.com/HeinleinSupport/olefy/${OLEFY_REF}" && \
    curl -fsSL "$base/olefy.py"         -o /opt/olefy.py && \
    curl -fsSL "$base/requirements.txt" -o /opt/requirements.txt && \
    test -s /opt/olefy.py && test -s /opt/requirements.txt

RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip && \
    # upstream olefy deps + redis (olefyd's optional result cache; redis.asyncio
    # ships inside the redis package)
    /opt/venv/bin/pip install -r /opt/requirements.txt redis

# ---- runtime ---------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-trixie

LABEL org.opencontainers.image.title="olefied" \
      org.opencontainers.image.description="Pooled, timeout-guarded olefy (oletools-over-TCP) for high-throughput mail scanning" \
      org.opencontainers.image.source="https://github.com/myguard-labs/rspamd-olefy" \
      org.opencontainers.image.authors="Thijs Eilander <eilander@myguard.nl>"

# libmagic1: required by python-magic. netcat-openbsd: HEALTHCHECK PING (nc -N).
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get update && \
    apt-get install -y --no-install-recommends libmagic1 netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
# olefy.py comes from upstream (fetched in the builder stage), not the repo.
COPY --from=builder /opt/olefy.py /usr/local/bin/olefy.py
COPY docker/olefyd.py /usr/local/bin/olefyd.py
COPY docker/olefy_ping.sh /usr/local/bin/olefy_ping.sh

RUN chmod 0755 /usr/local/bin/olefy.py /usr/local/bin/olefyd.py /usr/local/bin/olefy_ping.sh && \
    useradd --system --uid 1000 --no-create-home --shell /usr/sbin/nologin olefy

ENV PATH="/opt/venv/bin:${PATH}" \
    # point olefy.py's subprocess at the venv interpreter + olevba console script
    OLEFY_PYTHON_PATH="/opt/venv/bin/python" \
    OLEFY_OLEVBA_PATH="/opt/venv/bin/olevba" \
    OLEFIED_OLEFY_PATH="/usr/local/bin/olefy.py" \
    # public listener
    OLEFY_BINDADDRESS="0.0.0.0" \
    OLEFY_BINDPORT="10050" \
    # quiet by default — per-scan info logs are noise at scale; raise to 20/10 to debug
    OLEFY_LOGLVL="30" \
    # scratch dir; mount this as tmpfs when running --read-only
    OLEFY_TMPDIR="/tmp"
    # worker count defaults to nproc; tune throughput/limits via OLEFIED_* (see README)

USER olefy
EXPOSE 10050

# PING the public listener; any worker answers PONG. Fails the container if the
# pool can't service a trivial request (all workers wedged / dispatcher dead).
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD /usr/local/bin/olefy_ping.sh 127.0.0.1 "$OLEFY_BINDPORT" || exit 1

ENTRYPOINT ["/opt/venv/bin/python", "/usr/local/bin/olefyd.py"]
