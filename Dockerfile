FROM python:3.11-slim

WORKDIR /workspace

# Copy uv precompiled binary for fast installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uv/bin/

# Copy dependencies manifest
COPY pyproject.toml uv.lock ./

# Sync dependencies globally in the container
RUN /uv/bin/uv pip install --system -r pyproject.toml

# Copy source code and Lua script files
COPY app/ ./app
COPY docs/ ./docs

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
