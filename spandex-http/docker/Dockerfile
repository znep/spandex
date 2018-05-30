FROM socrata/java8

# Service specific settings.
ENV SPANDEX_SECONDARY_ES_PORT 9300
ENV SPANDEX_SECONDARY_ES_CLUSTER_NAME spandex
ENV SPANDEX_SECONDARY_ES_INDEX_ALIAS spandex
ENV SERVER_NAME spandex-http

# Some paths/locations we care about.
ENV SERVER_ROOT      /srv/${SERVER_NAME}/
ENV SERVER_CONFIG    ${SERVER_NAME}.conf
ENV SERVER_ARTIFACT  ${SERVER_NAME}-assembly.jar
ENV CONFIG_LINE      -Dconfig.file=/etc/${SERVER_CONFIG}

# Environment settings.
ENV JAVA_XMX 512m
ENV JMX_PORT 8049
ENV LOG_LEVEL INFO
ENV JAVA_MAX_METASPACE 64m

# Add other required resources to the container
EXPOSE 8042
EXPOSE 8049

ADD ship.d /etc/ship.d
ADD ${SERVER_CONFIG}.j2 /etc/
ADD ${SERVER_ARTIFACT} ${SERVER_ROOT}
