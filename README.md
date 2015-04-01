spandex
=======
autocomplete with elasticsearch

[![Codacy Badge](https://www.codacy.com/project/badge/821a4d00582d4c4b8a4641ee1ee94393)](https://www.codacy.com/public/johnkrah/spandex)

##Installing ElasticSearch
Make sure you're running ES 1.4.4 (NOT 1.5 which is installed by default by brew). https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.4.tar.gz

In your install directory, edit "config/elasticsearch.yml" to un-comment the cluster.name key and set the value to "spandex" like this:
```
cluster.name: spandex
```

## spandex-secondary
spandex-secondary is a Soda Server secondary service that replicates datasets to Elastic Search so that they work with autocomplete (Spandex-Http). Below are the steps to get the secondary running on your local machine.

###Build the secondary artifact
```sh
$ cd $YOUR_SPANDEX_REPO
$ sbt clean assembly
$ ln -s $YOUR_SPANDEX_REPO/spandex-secondary/target/scala-2.10/spandex-secondary-assembly-*.jar ~/secondary-stores
```

###Register Spandex secondary in truth DB
```sh
$ psql -U blist -d datacoordinator
datacoordinator=# INSERT INTO secondary_stores_config (store_id, next_run_time, interval_in_seconds) values ('spandex', now(), 5)
```

###Configuration changes
* Ensure you are using the most up-to-date version of soda2.conf as per the onramp repo.
* Append the entire contents of [reference.conf](https://github.com/socrata/spandex/blob/master/spandex-common/src/main/resources/reference.conf) to your `soda2.conf` file.

###Service restarts
Restart data coordinator and secondary watcher.
If all went well, the secondary watcher log should be free of errors and the last few lines should look something like this:
```sh
[Worker 1 for secondary spandex] INFO SecondaryWatcher 2015-03-31 10:51:06,661 update-next-runtime: 1ms; [["store-id","spandex"]]
[Worker 2 for secondary pg] INFO SecondaryWatcher 2015-03-31 10:51:06,661 update-next-runtime: 1ms; [["store-id","pg"]]
[Worker 2 for secondary spandex] INFO SecondaryWatcher 2015-03-31 10:51:06,664 update-next-runtime: 30ms; [["store-id","spandex"]]
[Worker 1 for secondary pg] INFO SecondaryWatcher 2015-03-31 10:51:06,664 update-next-runtime: 33ms; [["store-id","pg"]]
```
###Replicating a specific dataset to Spandex
Execute the following query against Soda Fountain:
`curl -X POST http://localhost:6010/dataset-copy/_{4x4}/spandex`

## spandex-http (autocomplete service) ##
spandex-http returns autocomplete suggestions for a given dataset, column and prefix. This is how you run spandex-http:
```sh
$ cd $YOUR_SPANDEX_REPO
$ sbt spandex-http/run
```

##Smoke tests
#####Check that ElasticSearch is up and running
```
curl http://localhost:9200/_status
```

#####Check that Spandex is up and can connect to ES
```
curl http://localhost:8042/health
```

#####Check that the spandex secondary is replicating data to ES
The following searches should all return documents (hits.total should be > 0)
```
curl http://localhost:9200/spandex/field_value/_search?q=*
curl http://localhost:9200/spandex/column_map/_search?q=*
curl http://localhost:9200/spandex/dataset_copy/_search?q=*
```

#####Issue an autocomplete query directly to Spandex
Spandex only accepts internal system IDs of datasets and columns, and relies on upper layers (soda-fountain and core) to map external facing IDs.
You can get this mapping as follows, replacing `{4x4}`, `{field_name}`, and `{query_prefix}` below:
```
psql sodafountain -w -c "copy (select datasets.dataset_system_id, copies.copy_number, columns.column_id from datasets inner join columns on datasets.dataset_system_id = columns.dataset_system_id inner join dataset_copies as copies on copies.dataset_system_id = datasets.dataset_system_id where datasets.resource_name = '_{4x4}' and columns.column_name = '{field_name}' order by copies.copy_number desc limit 1) to stdout with delimiter as '/'" | awk '{print "http://localhost:8042/suggest/" $0 "/{query_prefix}"}'
```
This should output a URL that you can use to query spandex-http. The example below queries for values starting with "j".
```
curl http://localhost:8042/suggest/primus.195/2/9gid-5p8z/j
```

The response should look something like this:
```
{
  "suggest" : [ {
    "text" : "j",
    "offset" : 0,
    "length" : 1,
    "options" : [ {
      "text" : "John Smith",
      "score" : 2.0
    } ]
  } ]
}
```

#####Querying autocomplete via Core
Querying autocomplete through the full stack looks like this: 
```
curl {domain}/api/views/{4x4}/columns/{field_name}/suggest/{query_prefix}
```
eg.
```
curl http://opendata.socrata.com/api/views/m27q-b6tw/columns/monster_5/suggest/fur
```

