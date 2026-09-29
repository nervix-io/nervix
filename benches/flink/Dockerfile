FROM flink:2.0.1-scala_2.12-java17

ARG KAFKA_CONNECTOR_VERSION=4.0.0-2.0
ARG KAFKA_CONNECTOR_SHA256=b28e99412699baf87454a8e4dde14016c5e944b2617409b5105622d2638e16bf

RUN curl --fail --location --silent --show-error \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      --output /opt/flink/lib/flink-sql-connector-kafka.jar \
    && echo "${KAFKA_CONNECTOR_SHA256}  /opt/flink/lib/flink-sql-connector-kafka.jar" | sha256sum --check - \
    && chown flink:flink /opt/flink/lib/flink-sql-connector-kafka.jar

COPY --chown=flink:flink start.sh /opt/flink/benchmark-start.sh
RUN chmod 755 /opt/flink/benchmark-start.sh

# The catalog runs up to 16 Kafka lanes plus stateful SQL operators in one TaskManager.
# Flink's image default of 1728m exhausts the JVM heap under that parallelism.
ENV TASK_MANAGER_NUMBER_OF_TASK_SLOTS=64 \
    FLINK_PROPERTIES="taskmanager.memory.process.size: 8g"
