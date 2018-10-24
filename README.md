# Spandex

Project Owner: discovery-l@socrata.com

This repository is home to all the back-end code that provides the column autocomplete
functionality for the following:

1. search cards on Socrata Data Lenses
2. finance apps
3. viz canvas filter bar

The repository consists of numerous sub-projects:

* spandex-common: the shared bits (data contracts, client code, etc.)
* spandex-secondary: the indexing service; the primary writer to our autocomplete Elasticsearch index
* spandex-http: an HTTP service that provides REST APIs for column autocomplete
* spandex-data-loader: a command-line program for loading data into the Spandex index directly
* spandex-integration-tests: a suite of integration tests

## Getting started (quickly)

This assumes you have run the Socrata onramp script.

Ensure you have pulled the latest from master:

```sh
git pull
```

Any configuration customizations should be made to a `configs/local.conf` file that is
gitignored. `configs/sample.conf` contains reasonable defaults that should work out of the box.

Build the spandex artifacts if you haven't already done so:

```sh
./bin/build.sh
```

At this point, you should be ready to run secondary-watcher-spandex and spandex-http:

``` sh
./bin/start_spandex_secondary_watcher
./bin/start_spandex_http
```

If you have not yet created a spandex index in Elasticsearch, this will create one for you. We always
create a timestamped index (eg. "spandex-20180510", which is then aliased to "spandex").

Running Spandex requires that you have the NBE stack up-and-running. Specifically, you will need
core, data-coordinator, and soda-fountain at a minimum.

## FAQ

Q: I'm getting a `NoNodeAvailableException` and it seems as though spandex-http and
secondary-watcher-spandex are unable to connect to the cluster.

A: Try restarting Elasticsearch: `brew services restart elasticsearch@5.6`. If that doesn't solve
the problem, make sure that your cluster is named appropriately (it should be named "es_dev"):

```sh
curl -s -XGET localhost:9200 | jq -r '.cluster_name'
```

If that isn't right, update your elasticsearch.yml file and restart Elasticsearch.

Q: Why does nothing happen when I delete my dataset from the UI? I would expect to see a flurry of
indexing activity?

A: soda-fountain doesn't send the delete down to data-coordinator for a certain number of days to
allow undeletes

## In-depth guide

### Install ElasticSearch

This following setup instructions are not necessary if you run the Socrata stack installation
script.

Make sure you're running ES >= 5.4.1. On Macs, this is best done via Homebrew.

``` sh
brew install elasticsearch@5.6
```

Or if you like to do things the hard way, you can download and install manually. Follow the
instructions here:

https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html

Next, you'll want to update the `cluster.name` setting in the `elasticsearch.yml` configuration
file. On OS X, you're likely to find it here:
`/usr/local/etc/elasticsearch/elasticsearch.yml`. Uncomment the `cluster.name` key and set the
value to "es_dev" like so:

```yml
cluster.name: es_dev
```

This is the same value expected by Cetera, allowing you to use the same Elasticsearch process for
development.

### Setting the Elasticsearch license

See the Discovery team about acquiring a company license, or download a basic license here:

https://register.elastic.co/

Once you've downloaded the license, you can install it by following the instructions here:

https://www.elastic.co/guide/en/x-pack/5.6/license-management.html

### Optionally Install Kibana

Kibana is an Elasticsearch plugin that simplifies monitoring of your Elasticsearch cluster. Among
its many features, it includes Sense, which is a handy, interactive tool for querying your
Elasticsearch cluster. It’s useful for testing queries for well-formedness (which is particularly
helpful given the complexity of the ES query DSL).

Follow the instructions here:

```
https://www.elastic.co/guide/en/kibana/current/install.html
```


### Optionally Install X-pack

Spandex now uses basic auth to authenticate requests to Elasticsearch.  This security is provided by the X-Pack extension.  If xpack is not installed spandex will still work, however, if you're working on code related to the Elasticsearch client, it may be a good idea to install X-Pack locally to better simulate the production environment.  Instructions for installing X-Pack can be found here 

````
https://www.elastic.co/guide/en/x-pack/5.6/installing-xpack.html
````

Once X-pack is installed, create a user 

````
curl --header "Content-Type: application/json" -X POST -u elastic --data '{"password" : "password1", "roles" : [ "superuser" ]}' http://localhost:9200/_xpack/security/user/spandex_service
````

The default password for the super user ```elastic``` is ```changeme```

If you have installed X-pack locally and want to use the scripts in the bin directory that interact with Elasticsearch, the environment variables ```SPANDEX_ES_USER``` and ```SPANDEX_ES_PASSWORD``` must be set.

### Start/stop/restart Elasticsearch

```sh
brew services (start|stop|restart) elasticsearch@5.6
```

### Setup spandex-secondary

spandex-secondary is a Soda Server secondary service that replicates datasets to an Elasticsearch
index, which is the store used by spandex-http to service autocomplete HTTP requests
(Spandex-Http).

To register Spandex secondary in truth DB, -- this is typically handled by the onramp install
script -- do the following:

```sh
psql -U blist -d datacoordinator -c "INSERT INTO secondary_stores_config (store_id, next_run_time, interval_in_seconds, group_name) values ('spandex', now(), 5, 'autocomplete');"
```

Data Coordinator will need to be restarted with a configuration naming spandex as a secondary group (but this should
already [be the case](https://github.com/socrata-platform/data-coordinator/blob/master/configs/sample-data-coordinator.conf#L25)).

### Run spandex-secondary (secondary-watcher-spandex)

This is how to run spandex-secondary:

```sh
$ ./bin/start_spandex_secondary
```

### Replicating a specific dataset to Spandex

Execute the following query against Soda Fountain:

``` sh
curl -X POST http://localhost:6010/dataset-copy/_{4x4}/spandex
```

### spandex-http (autocomplete service)

spandex-http returns autocomplete suggestions for a given dataset, column and prefix. This is how
you run spandex-http:

```sh
$ ./bin/start_spandex_http
```

### Running integration tests

When developing on Spandex, you'll want to run the included integration tests to make sure you
haven't broken anything. Integration tests are defined in the `spandex-integration-tests`
subproject. These integration tests require a running Elasticsearch process. To execute them from
sbt:

```
sbt spandex-integration-tests/test
```

Or from the SBT console:

```
project spandex-integration-tests
test
```

### Smoke tests

#### Check that ElasticSearch is up and running

```sh
curl http://localhost:9200/_status
```

#### Check that Spandex is up and can connect to ES

```sh
curl http://localhost:8042/health
```

#### Check that the spandex secondary is replicating data to ES

The following searches should all return documents (hits.total should be > 0)

```sh
curl http://localhost:9200/spandex/dataset_copy/_search
curl http://localhost:9200/spandex/column/_search
curl http://localhost:9200/spandex/column_value/_search
```

#### Issue an autocomplete query directly to Spandex

Spandex only accepts internal system IDs of datasets and columns, and relies on soda-fountain and
core to map external facing IDs. You can get this mapping as follows, replacing `{4x4}`,
`{field_name}`, and `{query_prefix}` below:

```sh
psql sodafountain -w -c "copy (select datasets.dataset_system_id, copies.copy_number, columns.column_id from datasets inner join columns on datasets.dataset_system_id = columns.dataset_system_id inner join dataset_copies as copies on copies.dataset_system_id = datasets.dataset_system_id where datasets.resource_name = '_{4x4}' and columns.column_name = '{field_name}' order by copies.copy_number desc limit 1) to stdout with delimiter as '/'" | awk '{print "http://localhost:8042/suggest/" $0 "/{query_prefix}"}'
```

This should output a URL that you can use to query spandex-http. The example below queries for
values starting with "j".

```sh
curl http://localhost:8042/suggest/primus.195/2/9gid-5p8z/j
```

The response should look something like this:

```json
{
  "options" : [ {
    "text" : "John Smith",
    "score" : 2.0
  } ]
}
```

The next example queries for some 10 values in that column, distinct but random.

```sh
curl http://localhost:8042/suggest/primus.195/2/9gid-5p8z
```

The response has the same format, but score is meaningless.

#### Querying autocomplete via Core

Querying autocomplete through the full stack looks like this:

```sh
curl {domain}/api/views/{4x4}/columns/{field_name}/suggest?text={query}
```

eg. (References a Kratos synthetic dataset looking at the monster_5 column for text matching "fur")

```sh
curl http://opendata.socrata.com/api/views/m27q-b6tw/columns/monster_5/suggest?text=fur
```
