FROM socrata/python3

WORKDIR /srv

RUN mkdir /srv/spandex-log-scraper
ADD spandex-log-scraper/scrape_logs.py /srv/spandex-log-scraper/
ADD spandex-log-scraper/requirements.txt /srv/spandex-log-scraper/

RUN pip install \
    --index-url https://shared-engr:AP6AAz7NGVeUWrEujHcqQBvbB6a27DQLR4zX32JMjHSWV26hn@repo.socrata.com/artifactory/api/pypi/pypi/simple \
    -r spandex-log-scraper/requirements.txt

COPY spandex-log-scraper/docker/ship.d /etc/ship.d

WORKDIR /srv/spandex-log-scraper
