# Usando a imagem oficial do uv com Python 3.11
FROM ghcr.io/astral-sh/uv:python3.11-trixie-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Variáveis de ambiente para o Airflow e para o uv
ENV AIRFLOW_HOME=/app/airflow
ENV UV_PROJECT_ENVIRONMENT=/usr/local
ENV UV_SYSTEM_PYTHON=1
ARG AIRFLOW_VERSION=3.1.3

# 1. Copia apenas os arquivos de definição de dependências primeiro
# Isso aproveita o cache do Docker: se o lock não mudar, ele não reinstala nada
COPY pyproject.toml uv.lock ./

# 2. Instala as dependências diretamente no ambiente do sistema (mais leve para containers)
# O --frozen garante que ele use exatamente o que está no seu uv.lock
RUN uv sync --frozen --no-cache

# 1. Instalando o Airflow com as Constraints oficiais
# Usamos o shell do Docker para capturar a versão do python dinamicamente
RUN AIRFLOW_VERSION=3.1.3 && \
    PYTHON_VERSION=$(uv run python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') && \
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" && \
    uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 3. Copia o restante dos arquivos do projeto
# COPY . .

# Expõe a porta padrão do Airflow Webserver
EXPOSE 8080

# Comando para iniciar o Airflow em modo standalone
# Usamos o 'uv run' para garantir que o contexto do projeto seja respeitado
CMD ["uv", "run", "airflow", "standalone"]
