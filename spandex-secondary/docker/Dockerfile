# TODO update to reelease dependency
FROM registry.docker.aws-us-west-2-infrastructure.socrata.net:5000/internal/secondary-watcher:v0.3.1-SNAPSHOT-tmp5

# TODO expose JMX
# EXPOSE 

ENV SECONDARY_ARTIFACT spandex-secondary-assembly.jar
ENV SECONDARY_CONFIG secondary.conf

ENV SPANDEX_SECONDARY_ES_CLUSTER_NAME spandex
ENV SPANDEX_SECONDARY_ES_PORT 9300
ENV SPANDEX_SECONDARY_NUM_WORKERS 5

WORKDIR $SERVER_ROOT

COPY ${SECONDARY_CONFIG}.j2 $SERVER_ROOT/
COPY $SECONDARY_ARTIFACT $SERVER_ROOT/plugins/