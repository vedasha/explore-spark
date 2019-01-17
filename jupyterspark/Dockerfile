FROM python:3.7-slim

RUN apt-get update && \
    apt-get install -y wget dnsutils vim curl git openjdk-8-jdk

RUN pip install jupyter

RUN git clone https://github.com/jupyter-scala/jupyter-scala.git \
  && cd jupyter-scala \
  && ./jupyter-scala script \
  && jupyter kernelspec list


