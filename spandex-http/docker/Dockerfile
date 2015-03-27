FROM socrata/java

# Some paths/locations we care about.
ENV SEVICE_NAME spandex-http

ENV SERVER_ARTIFACT  spandex-http-assembly.jar
ENV SERVER_ROOT      /srv/spandex-http/
ENV SERVER_CONFIG    spandex-http.conf
ENV LOGBACK_CONFIG   spandex-http-logback.xml

# Environment settings.
ENV JAVA_MEM 2048m
ENV SPANDEX_ES_CLUSTER_NAME spandex

# Add other required resources to the container
# WORKDIR ${SERVER_ROOT}

ADD ${SERVER_CONFIG}.j2 /etc/
ADD ${LOGBACK_CONFIG}.j2 /etc/
ADD ${SERVER_ARTIFACT} ${SERVER_ROOT}

ADD ship.d /etc/ship.d

EXPOSE 8042
