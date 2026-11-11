FROM flink:2.2.1-scala_2.12-java17

# The Kafka connector is released separately from Flink: 5.0.0 is the build for Flink 2.2.
ARG KAFKA_CONNECTOR=5.0.0-2.2
USER root
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
 && mkdir -p /flink-state && chown flink:flink /flink-state

USER flink

COPY --chown=flink:flink sql /opt/flink/sql
COPY --chown=flink:flink submit.sh run-sql-tests.sh /opt/flink/
COPY --chown=flink:flink tests /opt/flink/tests
