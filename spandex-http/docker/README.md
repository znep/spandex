# Spandex Http Docker Config #
To build the image, run:
    `docker build -t spandex-http .`

Or, if you want to replace old versions:
    `docker build --rm -t spandex-http .`

## Required Environment Variables ##
* `SPANDEX_SECONDARY_ES_HOST` - The hostname of the elastic search cluster.


## Optional Runtime Variables ##
See the [DockerFile](Dockerfile) for defaults.

* `SPANDEX_SECONDARY_ES_PORT` - The listening port of the elastic search cluster.
* `SPANDEX_SECONDARY_ES_CLUSTER_NAME` - The name of the ES cluster.
* `SPANDEX_SECONDARY_ES_INDEX_ALIAS` - The index or alias to consume in the ES cluster.
* `JAVA_XMX`                - Sets the JVM heap size.
