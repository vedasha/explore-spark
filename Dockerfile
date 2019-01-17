FROM openjdk:8

ENV DEBIAN_FRONTEND noninteractive
ENV SCALA_VERSION 2.11
ENV SPARK_VERSION 2.3.1


RUN apt-get update && \
    apt-get install -y wget dnsutils vim && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean 

RUN wget -q http://apache.mirrors.pair.com/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz -O /tmp/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
	&& ls -alh /tmp/spark-* \
	&& tar xfz /tmp/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz -C /opt \
	&& rm /tmp/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

EXPOSE 4040 4041
