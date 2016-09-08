# Elasticsearch Snapshotter Docker Thingy #

## Required Environment Variables ##
* `AWS_REGION` - Region of Amazon Web Services containing the ec2 and s3 assets
* `ES_HOST` - The hostname of the elastic search cluster
* `ES_PORT` - The http port of the elastic search cluster
* `INDICES` - One or multiple comma separated index names to snapshot, also can include wildcards
* `SNAPSHOT_NAME` - The snapshot configuration name to use or create, and s3 bucket name

## Optional Runtime Variables ##
See the [DockerFile](Dockerfile) for defaults.

* `SNAPSHOT_ID` - What to name this moment in time snapshot, defaults to now yyyyMMddHHmm
* `IGNORE_UNAVAILABLE` - If indices are unavailable, ignore them and continue snapshot with existing indices
* `INCLUDE_GLOBAL_STATE` - Include cluster state in the snapshot, can be excluded at restore time also

Runs via tasks-chronos (see https://github.com/socrata/tasks-chronos/)
