FROM socrata/python3

WORKDIR /srv

RUN mkdir /srv/spandex-log-scraper
ADD scrape_logs.py /srv/spandex-log-scraper/
ADD requirements.txt /srv/spandex-log-scraper/

ARG ARTIFACTORY_USER
ARG ARTIFACTORY_PASSWORD
ARG ARTIFACTORY_REPO
ENV ARTIFACTORY_INDEX_URL="https://${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}@${ARTIFACTORY_REPO}"

RUN pip install \
    --index-url "${ARTIFACTORY_INDEX_URL}" \
    -r /srv/spandex-log-scraper/requirements.txt

COPY ship.d /etc/ship.d

WORKDIR /srv/spandex-log-scraper
