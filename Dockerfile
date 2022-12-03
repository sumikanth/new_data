FROM apache/airflow:latest-python3.8
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
# RUN apt update -y && sudo apt upgrade -y && \
#     apt-get install -y wget build-essential checkinstall libncursesw5-dev  libssl-dev  libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev 
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3
# ENV SPARK_HOME /usr/local/spark
# ENV SPARK_VERSION="3.1.2"
# ENV HADOOP_VERSION="3.2"
# USER root
# # Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
# RUN cd "/tmp" && \
#         wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
#         tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
#         mkdir -p "${SPARK_HOME}/bin" && \
#         mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
#         cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
#         cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
#         rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# # Create SPARK_HOME env var
# RUN export SPARK_HOME
# ENV PATH $PATH:/usr/local/spark/bin
# USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r requirements.txt
COPY ./maven_download.py maven_download.py 
COPY ./maven-downloads.txt maven-downloads.txt
RUN python maven_download.py