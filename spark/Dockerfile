FROM tabulario/spark-iceberg

# Usuário root para instalar os jars
USER root

# Define as versões
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

# Baixa os JARs diretamente para a pasta de jars do Spark
# O hadoop-aws precisa do aws-java-sdk-bundle para funcionar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Volta para o usuário padrão da imagem
USER root
