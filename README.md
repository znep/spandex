# Spandex

Dataset cell-level autocomplete with Elasticsearch.

[![Codacy Badge](https://www.codacy.com/project/badge/821a4d00582d4c4b8a4641ee1ee94393)](https://www.codacy.com/public/johnkrah/spandex)

## Install ElasticSearch

This following setup instructions are not necessary if you run the Socrata stack installation
script.

Make sure you're running ES >= 5.4.1. On Macs, this is best done via Homebrew.

``` sh
brew install elasticsearch
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

## Get a free license from Elasticsearch

https://register.elastic.co/

Once you've downloaded the license, you can install it by following the instructions here:

https://www.elastic.co/guide/en/x-pack/current/installing-license.html

## Optionally Install Kibana

Kibana is an Elasticsearch plugin that simplifies monitoring of your Elasticsearch cluster. Among
its many features, it includes Sense, which is a handy, interactive tool for querying your
Elasticsearch cluster. It’s useful for testing queries for well-formedness (which is particularly
helpful given the complexity of the ES query DSL).

On OS X, you can install Kibana using Homebrew:

```sh
brew install kibana
```

Or you can follow the instructions here:

```
https://www.elastic.co/guide/en/kibana/current/install.html
```

## Start Elastic Search

```sh
brew services (start|stop) elasticsearch
```

## Setup spandex-secondary

spandex-secondary is a Soda Server secondary service that replicates datasets to an Elasticsearch
index so, which is the store used by spandex-http to service autocomplete HTTP requests
(Spandex-Http). Below are the steps to get the secondary running on your local machine.

### Run spandex-secondary (secondary-watcher-spandex)
This is how to run spandex-secondary:
```sh
$ cd $YOUR_SPANDEX_REPO
$ sbt -Dconfig.file=configs/application.conf spandex-secondary/run
```

### Register Spandex secondary in truth DB and restart Data Coordinator

```sh
psql -U blist -d datacoordinator -c "INSERT INTO secondary_stores_config (store_id, next_run_time, interval_in_seconds, group_name) values ('spandex', now(), 5, 'autocomplete');"
```

Data Coordinator will need to be restarted with a configuration naming spandex as a secondary group (but this should
already [be the case](https://github.com/socrata-platform/data-coordinator/blob/master/configs/sample-data-coordinator.conf#L25)).

### Configuration

 * Configuration for development setup is maintained in [configs/application.conf](https://github.com/socrata/spandex/blob/master/configs/application.conf).
 * You can create overrides to this in the .gitignored file `configs/spandex.conf`.

### Replicating a specific dataset to Spandex

Execute the following query against Soda Fountain:

``` sh
curl -X POST http://localhost:6010/dataset-copy/_{4x4}/spandex
```

## spandex-http (autocomplete service) ##

spandex-http returns autocomplete suggestions for a given dataset, column and prefix. This is how
you run spandex-http:

```sh
$ cd $YOUR_SPANDEX_REPO
$ sbt -Dconfig.file=configs/application.conf spandex-http/run
```

## Smoke tests
##### Check that ElasticSearch is up and running

```sh
curl http://localhost:9200/_status
```

##### Check that Spandex is up and can connect to ES

```sh
curl http://localhost:8042/health
```

##### Check that the spandex secondary is replicating data to ES

The following searches should all return documents (hits.total should be > 0)

```sh
curl http://localhost:9200/spandex/field_value/_search?q=*
curl http://localhost:9200/spandex/column_map/_search?q=*
curl http://localhost:9200/spandex/dataset_copy/_search?q=*
```

##### Issue an autocomplete query directly to Spandex

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

##### Querying autocomplete via Core

Querying autocomplete through the full stack looks like this:

```sh
curl {domain}/api/views/{4x4}/columns/{field_name}/suggest?text={query}
```

eg. (References a Kratos synthetic dataset looking at the monster_5 column for text matching "fur")

```sh
curl http://opendata.socrata.com/api/views/m27q-b6tw/columns/monster_5/suggest?text=fur
```
