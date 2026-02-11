FROM python:3.14-slim

WORKDIR /app/

RUN apt-get update && apt-get install -y \
    curl \
    bash \
    gcc \
    build-essential \
    gettext \
    gnupg \
    git \
    musl-dev \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Install uv
# Ref: https://docs.astral.sh/uv/guides/integration/docker/#installing-uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Compile bytecode
# Ref: https://docs.astral.sh/uv/guides/integration/docker/#compiling-bytecode
ENV UV_COMPILE_BYTECODE=1

RUN uv venv .venv
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN pip install --no-cache-dir codecov-cli
ENV PYTHONPATH=/app/
ENV PYTHONUNBUFFERED=1

# uv Cache
# Ref: https://docs.astral.sh/uv/guides/integration/docker/#caching
ENV UV_LINK_MODE=copy

# Copy dependency files
COPY ./uv.lock ./pyproject.toml ./

RUN uv sync --frozen --no-install-project --all-groups --extra all

COPY ./src/ ./src/
COPY ./tests/ ./tests/
COPY README.md ./
COPY LICENSE ./
COPY CONTRIBUTING.md ./

COPY .env.example .env
