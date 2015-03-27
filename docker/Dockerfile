FROM socrata/java

WORKDIR /srv/spandex-http

# Some paths/locations we care about.
ENV SEVICE_NAME spandex-http

ENV SERVER_ARTIFACT  spandex-http-assembly.jar
ENV SERVER_ROOT      /srv/spandex-http
ENV SERVER_CONFIG    /etc/spandex-http.conf
ENV LOGBACK_CONFIG   /etc/spandex-http-logback.xml

# Environment settings.
ENV JAVA_MEM 2048m
ENV SPANDEX_ES_CLUSTER_NAME spandex

# Add other required resources to the container
Add ${SERVER_CONFIG}.conf.j2 /etc/
Add ${LOGBACK_CONFIG}.j2 /etc/

ADD ship.d /etc/ship.d

EXPOSE 8042
