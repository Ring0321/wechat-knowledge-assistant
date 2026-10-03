ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM ${PYTHON_IMAGE} AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /srv/app
RUN pip install --no-cache-dir uv==0.12.7
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources && printf 'Acquire::Retries "3";\n' > /etc/apt/apt.conf.d/80-retries && apt-get update && apt-get install -y --no-install-recommends ffmpeg tesseract-ocr tesseract-ocr-chi-sim tesseract-ocr-eng fonts-dejavu-core && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
COPY pyproject.toml uv.lock README.md LICENSE ./

FROM base AS production
RUN uv sync --locked --no-dev --no-install-project
COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home app
ENV PATH="/srv/app/.venv/bin:$PATH"
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

FROM base AS test
RUN uv sync --locked --no-install-project && .venv/bin/python -m pytest --version
COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./
COPY tests ./tests
COPY scripts ./scripts
ENV PATH="/srv/app/.venv/bin:$PATH"
CMD ["python", "-m", "pytest", "tests/integration", "-v"]

FROM base AS browser
RUN uv sync --locked --no-dev --no-install-project
ENV PATH="/srv/app/.venv/bin:$PATH" PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources && printf 'Acquire::Retries "3";\n' > /etc/apt/apt.conf.d/80-retries && playwright install --with-deps chromium --only-shell && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
RUN groupadd --gid 10001 browser && useradd --uid 10001 --gid browser --no-create-home browser
USER 10001:10001
CMD ["playwright", "run-server", "--host", "0.0.0.0", "--port", "3000"]
