FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

# caminhos separados
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV DBT_PROFILES_DIR=/opt/dagster/app/dbt
WORKDIR /opt/dagster/app
ENV VIRTUAL_ENV=/opt/dagster/app/.venv
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"

RUN mkdir -p $DAGSTER_HOME

# deps primeiro (cache docker)
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# código depois
COPY project ./project
COPY workspace.yaml .

EXPOSE 3000

CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
