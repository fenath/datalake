# 🚀 Personal Data Lakehouse — MinIO + Iceberg + Trino

A **personal Data Lakehouse project** built for ingestion, storage, processing, and analysis of financial data using modern open-source ecosystem technologies.

The environment integrates object storage, ACID analytical tables, a distributed SQL engine, and exploratory analysis tools.

---

## 🧱 Architecture

Stack used:

* **Object Storage:** MinIO
* **Lakehouse Table Format:** Apache Iceberg (via REST Catalog)
* **SQL Query Engine:** Trino
* **BI / Visualization:** Metabase
* **Distributed Processing:** Apache Spark (PySpark)
* **In-memory DataFrame Processing:** Polars
* **Exploration Environment:** Jupyter Notebook

---

## 🏗️ Architecture Overview

```text
                +----------------------+
                |       Metabase       |
                +----------+-----------+
                           |
                           v
                    +-------------+
                    |    Trino    |
                    +------+------+ 
                           |
                           v
                  +------------------+
                  |  Apache Iceberg  |
                  |   (REST Catalog) |
                  +--------+---------+
                           |
                           v
                       +--------+
                       |  MinIO |
                       +--------+

        +-----------------------------------+
        |  PySpark / Polars / Jupyter       |
        | (Ingestion, Transformation, EDA)  |
        +-----------------------------------+
```

---

## 🎯 Project Goal

Build a **modern Data Lakehouse** to:

* Centralize personal financial data (bank statements)
* Consolidate operational data from a car sales business
* Experiment with Iceberg-based analytical architecture
* Explore data versioning, schema evolution, and time travel
* Create financial and operational dashboards

---

## 📂 Data Domains

### 1️⃣ Personal Finance

* Bank statements (CSV)
* Categorized transactions
* Income and expenses
* Monthly consolidation
* Analyses:

  * Cash flow
  * Expenses by category
  * Net worth evolution

---

### 2️⃣ Car Sales Business

* Vehicle inventory
* Sales history
* Margin per vehicle
* Average ticket
* Average time in inventory
* Monthly revenue

---

## 🧊 Why Apache Iceberg?

Using Iceberg enables:

* ACID transactions on object storage
* Data versioning
* Time travel queries
* Schema evolution
* Hidden partitioning
* Efficient merge / upsert operations

Example time travel query in Trino:

```sql
SELECT *
FROM vendas FOR VERSION AS OF 123456789;
```

---

## 🔄 Ingestion Flow

### 🟢 Bronze Layer

* Raw data ingested into MinIO
* Original format preserved

### 🟡 Silver Layer

* Data cleaning
* Normalization
* Type conversion
* Date standardization

### 🔵 Gold Layer

* Analytical tables
* Aggregations
* Business metrics

Processing performed using:

* PySpark (large volumes)
* Polars (fast local processing)
* Jupyter for EDA

---

## 📊 Queries via Trino

Trino is used for:

* Federated SQL queries
* Integration with Metabase
* Ad hoc analysis
* Validation of transformed data

Example:

```sql
SELECT 
    date_trunc('month', data_venda) AS month,
    SUM(valor_venda) AS total_revenue
FROM gold.vendas
GROUP BY 1
ORDER BY 1;
```

---

## 📈 Dashboards (Metabase)

Created dashboards:

### Personal Finance

* Expenses by category
* Income vs Expenses
* Monthly evolution

### Business

* Monthly revenue
* Margin per vehicle
* Inventory turnover
* Average ticket

---

## ⚙️ Project Structure

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
│   ├── finance/
│   └── business/
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

## 🧪 Technical Learnings

* Practical implementation of a Lakehouse architecture
* Using Iceberg with a REST catalog
* Integration between Trino + Iceberg + MinIO
* Partitioning strategies
* Schema evolution
* Query optimization
* Analytical modeling strategies

---

## 🚀 How to Run

### 1️⃣ Start the infrastructure

```bash
docker-compose up -d
```

Available services:

* MinIO → [http://localhost:9000](http://localhost:9000)
* Trino → [http://localhost:8080](http://localhost:8080)
* Metabase → [http://localhost:3000](http://localhost:3000)

---

### 2️⃣ Create Iceberg tables

Run scripts in `sql/ddl/` via Trino.

---

### 3️⃣ Run ingestion jobs

Execute notebooks or PySpark jobs:

```bash
python jobs/silver/transform_financeiro.py
```

---

## 📌 Next Steps

* [ ] Implement CDC
* [ ] Automation with Airflow
* [ ] Data quality layer
* [ ] Performance testing
* [ ] Observability metrics
* [ ] Cloud deployment

---

## 🧠 Motivation

This project was created as a practical lab to:

* Master Lakehouse architecture
* Consolidate knowledge in data engineering
* Apply modern concepts in a real scenario
* Build a personal and controlled analytical environment

---

## 📜 License

Personal / educational use.

---

## Based on

* [Data Engineering in Practice: Building a Data Lake at Home!](https://www.youtube.com/watch?v=ntp-OfixCm4)

Articles used during the project:

* Streamlining Big Data with Spark: Writing and Reading Delta Lake Format on MinIO-S3 Storage (Medium)
* Setting Up Trino with Hive to Query Delta Lake Data on MinIO: A Scalable Big Data Solution
* [https://www.datalib.com.br/post/como-instalar-um-cluster-do-apache-spark-no-docker-desktop-utilizando-compose](https://www.datalib.com.br/post/como-instalar-um-cluster-do-apache-spark-no-docker-desktop-utilizando-compose)
* [https://blog.min.io/a-developers-introduction-to-apache-iceberg-using-minio/](https://blog.min.io/a-developers-introduction-to-apache-iceberg-using-minio/)
