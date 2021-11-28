FROM openjdk:8-alpine

RUN apk --update add wget tar bash git
RUN wget https://downloads.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
RUN tar -xzf spark-3.2.0-bin-hadoop3.2.tgz && \
    mv spark-3.2.0-bin-hadoop3.2 /spark && \
    rm spark-3.2.0-bin-hadoop3.2.tgz

RUN wget https://github.com/sbt/sbt/releases/download/v1.5.5/sbt-1.5.5.tgz
RUN tar -xzf sbt-1.5.5.tgz && \
    rm sbt-1.5.5.tgz
RUN echo "-Dsbt.classloader.close=false" >> /sbt/conf/sbtopts
RUN mkdir /app
WORKDIR /app

LABEL maintainer="Priyanka Mittal <priyankamittal09@gmail.com>"

COPY test-script.sh test-script.sh
COPY start-script.sh start-script.sh
COPY spark-search-analysis-assembly-0.1.jar spark-search-analysis-assembly-0.1.jar
ADD external-reference.conf /app


