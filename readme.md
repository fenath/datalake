# 🚀 Data Lakehouse Pessoal — MinIO + Iceberg + Trino

Projeto de **Data Lakehouse pessoal** construído para ingestão, armazenamento, processamento e análise de dados financeiros, utilizando tecnologias modernas do ecossistema open source.

O ambiente integra armazenamento objeto, tabela analítica ACID, engine SQL distribuída e ferramentas de análise exploratória.

---

## 🧱 Arquitetura

Stack utilizada:

* **Object Storage:** MinIO
* **Formato de Tabela Lakehouse:** Apache Iceberg (via REST Catalog)
* **Query Engine SQL:** Trino
* **BI / Visualização:** Metabase
* **Processamento Distribuído:** Apache Spark (PySpark)
* **Processamento DataFrame em memória:** Polars
* **Ambiente de Exploração:** Jupyter Notebook

---

## 🏗️ Visão da Arquitetura

```text
                +----------------------+
                |      Metabase        |
                +----------+-----------+
                           |
                           v
                    +-------------+
                    |    Trino    |
                    +------+------+ 
                           |
                           v
                  +------------------+
                  | Apache Iceberg   |
                  |  (REST Catalog)  |
                  +--------+---------+
                           |
                           v
                       +--------+
                       | MinIO  |
                       +--------+

        +-----------------------------------+
        | PySpark / Polars / Jupyter        |
        | (Ingestão, Transformação, EDA)    |
        +-----------------------------------+
```

---

## 🎯 Objetivo do Projeto

Construir um **Data Lakehouse moderno** para:

* Centralizar dados financeiros pessoais (extratos bancários)
* Consolidar dados operacionais de uma empresa de venda de carros
* Testar arquitetura analítica baseada em Iceberg
* Explorar versionamento de dados, schema evolution e time travel
* Criar dashboards financeiros e operacionais

---

## 📂 Domínios de Dados

### 1️⃣ Financeiro Pessoal

* Extratos bancários (CSV)
* Transações categorizadas
* Receitas e despesas
* Consolidação mensal
* Análises:

  * Fluxo de caixa
  * Despesas por categoria
  * Evolução patrimonial

---

### 2️⃣ Empresa de Venda de Carros

* Estoque de veículos
* Histórico de vendas
* Margem por veículo
* Ticket médio
* Tempo médio em estoque
* Receita mensal

---

## 🧊 Por que Apache Iceberg?

Uso do Iceberg permite:

* ACID sobre object storage
* Versionamento de dados
* Time travel queries
* Schema evolution
* Particionamento oculto
* Merge / Upsert eficientes

Exemplo de consulta time travel no Trino:

```sql
SELECT *
FROM vendas FOR VERSION AS OF 123456789;
```

---

## 🔄 Fluxo de Ingestão

### 🟢 Camada Bronze

* Dados brutos ingeridos no MinIO
* Formato original preservado

### 🟡 Camada Silver

* Limpeza
* Normalização
* Conversão de tipos
* Padronização de datas

### 🔵 Camada Gold

* Tabelas analíticas
* Agregações
* Métricas de negócio

Processamento realizado com:

* PySpark (grandes volumes)
* Polars (processamento rápido local)
* Jupyter para EDA

---

## 📊 Consultas via Trino

O Trino é utilizado para:

* Consultas SQL federadas
* Integração com Metabase
* Análises ad hoc
* Validação de dados transformados

Exemplo:

```sql
SELECT 
    date_trunc('month', data_venda) AS mes,
    SUM(valor_venda) AS receita_total
FROM gold.vendas
GROUP BY 1
ORDER BY 1;
```

---

## 📈 Dashboards (Metabase)

Painéis criados:

### Financeiro Pessoal

* Despesas por categoria
* Receita vs Despesa
* Evolução mensal

### Empresa

* Receita mensal
* Margem por veículo
* Giro de estoque
* Ticket médio

---

## ⚙️ Estrutura do Projeto

```text
datalake/
│
├── docker/
│   ├── minio/
│   ├── trino/
│   ├── iceberg-rest/
│   └── metabase/
│
├── notebooks/
│   ├── financeiro/
│   └── empresa/
│
├── jobs/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── sql/
    ├── ddl/
    └── analytics/
```

---

## 🧪 Aprendizados Técnicos

* Implementação prática de arquitetura Lakehouse
* Uso de Iceberg com REST catalog
* Integração Trino + Iceberg + MinIO
* Estratégias de particionamento
* Evolução de schema
* Otimização de consultas
* Estratégias de modelagem analítica

---

## 🚀 Como Executar

### 1️⃣ Subir infraestrutura

```bash
docker-compose up -d
```

Serviços disponíveis:

* MinIO → [http://localhost:9000](http://localhost:9000)
* Trino → [http://localhost:8080](http://localhost:8080)
* Metabase → [http://localhost:3000](http://localhost:3000)

---

### 2️⃣ Criar tabelas Iceberg

Executar scripts em `sql/ddl/` via Trino.

---

### 3️⃣ Rodar ingestões

Executar notebooks ou jobs PySpark:

```bash
python jobs/silver/transform_financeiro.py
```

---

## 📌 Próximos Passos

* [ ] Implementar CDC
* [ ] Automação com Airflow
* [ ] Camada de qualidade de dados
* [ ] Testes de performance
* [ ] Métricas de observabilidade
* [ ] Deploy em cloud

---

## 🧠 Motivação

Projeto criado como laboratório prático para:

* Dominar arquitetura Lakehouse
* Consolidar conhecimentos em engenharia de dados
* Aplicar conceitos modernos em um cenário real
* Criar um ambiente analítico próprio e controlado

---

## 📜 Licença

Uso pessoal / educacional.

## Baseado em: 
- [Engenharia de Dados na Prática: Criando um Data Lake em casa!](https://www.youtube.com/watch?v=ntp-OfixCm4)

Artigos utilizados para realizar o projeto: 

- Streamlining Big Data with Spark: Writing and Reading Delta Lake Format on MinIO-S3 Storage - medium 
- Setting Up Trino with Hive to Query Delta Lake Data on MinIO: A Scalable Big Data Solution
- https://www.datalib.com.br/post/como-instalar-um-cluster-do-apache-spark-no-docker-desktop-utilizando-compose
- https://blog.min.io/a-developers-introduction-to-apache-iceberg-using-minio/


Apache spark + minio
- trino
- Hive
- postgres
- superset

Todos esses serviços rodam em uma docker

conceito S3 no minio

Criação de um datalake

Vou tentar colocar tudo em container docker

# O catálogo 'iceberg' já está configurado no spark-defaults.conf
spark = SparkSession.builder.getOrCreate()

# Criar namespace (database)
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.db")

# Criar tabela
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.db.teste (
  id bigint,
  nome string
) USING iceberg
""")

# Inserir dados de teste
spark.sql("""
INSERT INTO iceberg.db.teste VALUES 
  (1, 'Alice'),
  (2, 'Bob'),
  (3, 'Carlos')
""")

# Consultar
spark.sql("SELECT * FROM iceberg.db.teste").show()
