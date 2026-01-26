[Engenharia de Dados na Prática: Criando um Data Lake em casa!](https://www.youtube.com/watch?v=ntp-OfixCm4)



Apache spark + minio
- trino
- Hive
- postgres
- superset

Todos esses serviços rodam em uma docker

conceito S3 no minio

Criação de um datalake

Artigos utilizados para realizar o projeto: 
- Streamlining Big Data with Spark: Writing and Reading Delta Lake Format on MinIO-S3 Storage - medium 
- Setting Up Trino with Hive to Query Delta Lake Data on MinIO: A Scalable Big Data Solution
- https://www.datalib.com.br/post/como-instalar-um-cluster-do-apache-spark-no-docker-desktop-utilizando-compose
- https://blog.min.io/a-developers-introduction-to-apache-iceberg-using-minio/

Vou tentar colocar tudo em container docker


Testar o jupyter:


from pyspark.sql import SparkSession

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
