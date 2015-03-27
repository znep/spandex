# Spandex Http Docker Config #
To build the image, run:

    docker build --rm -t spandex-http .

## Required Environment Variables ##
* `SPANDEX_ES_HOST` - The hostname of the elastic search cluster.
* `SPANDEX_ES_PORT` - The listening port of the elastic search cluster.

## Optional Runtime Variables ##
See the `DockerFile` for defaults.

* `SPANDEX_ES_CLUSTER_NAME` - The name of the ES cluster.
* `JAVA_MEM`                - Sets the JVM heap size.
