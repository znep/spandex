FROM socrata/py3_ruby

WORKDIR /srv

RUN mkdir /srv/spandex-purge
ADD list_bellerophon_uids.rb /srv/spandex-purge/
ADD purge /srv/spandex-purge/
ADD analyze.py /srv/spandex-purge/
ADD requirements.txt /srv/spandex-purge
ADD mail.py /srv/spandex-purge

RUN DEBIAN_FRONTEND=noninteractive apt-get -y update && apt-get install -y postgresql postgresql-contrib jq bc sendmail

ADD Gemfile /tmp/Gemfile
ADD Gemfile.lock /tmp/Gemfile.lock
WORKDIR /tmp
RUN bundle install

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_PASSWORD
ARG ARTIFACTORY_REPO
ENV ARTIFACTORY_INDEX_URL="https://${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}@${ARTIFACTORY_REPO}"

RUN pip install \
    --index-url "${ARTIFACTORY_INDEX_URL}" \
    -r /srv/spandex-purge/requirements.txt

COPY ship.d /etc/ship.d

WORKDIR /srv/spandex-purge
