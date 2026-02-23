FROM ghcr.io/astral-sh/uv:python3.11-bookworm

# caminhos separados
ENV DAGSTER_HOME=/opt/dagster/dagster_home
WORKDIR /opt/dagster/app

RUN mkdir -p $DAGSTER_HOME

# deps primeiro (cache docker)
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen

# código depois
COPY project ./project
COPY workspace.yaml .

EXPOSE 3000

CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
