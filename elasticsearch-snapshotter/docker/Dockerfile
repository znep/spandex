FROM socrata/base

RUN apt-get update

RUN apt-get -y install jq

ENV IGNORE_UNAVAILABLE      true
ENV INCLUDE_GLOBAL_STATE    true

ADD ship.d /etc/ship.d
