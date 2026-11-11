FROM python:3.13-slim AS base
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_ROOT_USER_ACTION=ignore PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY pyproject.toml ./
COPY rivus_collect ./rivus_collect
COPY venues ./venues

FROM base AS test
RUN pip install -e ".[dev]"
COPY tests ./tests
CMD ["sh", "-c", "ruff check . && pytest -q"]

FROM base AS runtime
RUN pip install . && useradd --system collector
USER collector
ENTRYPOINT ["python", "-m", "rivus_collect"]
