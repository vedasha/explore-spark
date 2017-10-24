FROM openjdk:8

RUN apt-get -qq update \
    && apt-get -qq install locales vim git curl zip

RUN curl -O https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz \
    && tar xvf spark-2.2.0-bin-hadoop2.7.tgz -C /opt/ \
    && ln -s /opt/spark-2.2.0-bin-hadoop2.7 /opt/spark \
    && rm spark-2.2.0-bin-hadoop2.7.tgz

ENV SPARK_HOME /opt/spark-2.2.0-bin-hadoop2.7

ENV PATH $SPARK_HOME/bin:$PATH

RUN ls -al /bin/sh \
	&& rm /bin/sh \
	&& ln -sf /bin/bash /bin/sh

RUN curl -s https://get.sdkman.io | bash \
	&& source "/root/.sdkman/bin/sdkman-init.sh" \
	&& sdk install kotlin

RUN apt-get -qq clean