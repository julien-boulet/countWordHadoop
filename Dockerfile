FROM gradle:jdk8 as compile

COPY . /home/source/java
WORKDIR /home/source/java

USER root
RUN chown -R gradle /home/source/java

USER gradle
RUN gradle clean build

FROM bde2020/hadoop-namenode:2.0.0-hadoop3.1.2-java8

WORKDIR /
COPY --from=compile "/home/source/java/build/libs/countWordHadoop.jar" .

CMD ["/run.sh"]
