spandex
=======
autocomplete with elasticsearch

Make sure you're running ES 1.4.4 (NOT 1.5 which is installed by default by brew). https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.4.tar.gz

Make sure your /etc/soda2.conf file is up-to-date. You'll need to add the metrics section "com.socrata.coordinator.secondary-watcher" to look like this:
com.socrata.coordinator.secondary-watcher = ${com.socrata.coordinator.common} {
  database.app-name = "data coordinator secondary watcher"
  claim-timeout = 5m
  watcher-id = 61e9a209-98e7-4daa-9c43-5778a96e1d8a
  metrics {
    # Should be unique for each service
    prefix = "com.socrata.data.coordinator"
    log-metrics = false
    enable-graphite = false
  }
  tmpdir = ${java.io.tmpdir}
}

Edit your local elastic search configuration "config/elasticserach.yml" to un-comment the cluster.name key and set the value to "spandex" like this:

cluster.name: spandex


[![Codacy Badge](https://www.codacy.com/project/badge/821a4d00582d4c4b8a4641ee1ee94393)](https://www.codacy.com/public/johnkrah/spandex)

## Spandex-Http (autocomplete service) ##
Spandex-Http returns autocomplete suggestions for a given dataset, column and prefix.

```sh
$ cd spandex
$ ./sbt
> container:start
> browse
```

If `browse` doesn't launch your browser, manually open [http://localhost:8042/](http://localhost:8042/) in your browser.

You can search what is currently indexed in spandex like this (searches "crimeType" for strings starting with "nar"):

http://localhost:8042/suggest/primus.1234/2/3/dat

Where `primus` is your pg secondary name
Where `1234` is a tablespace number in your local machine
Where `dat` is something.something.something

Like this `http://localhost:8080/views/m27q-b6tw/columns/monster_5/suggest/fur`

Also, you can interact directly with elastic search via its web API by browsing here (should show the entire index):

http://localhost:9200/spandex/_search?q=*

## Spandex-Secondary
Spandex-Secondary is a Soda Server secondary service that replicates the data required for autocomplete to Elastic Search. Below are the steps to get the secondary running on your local machine.

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
* Append the entire contents of [reference.conf](https://github.com/socrata/spandex/blob/master/spandex-common/src/main/resources/reference.conf) to your `soda2.conf` file.
* Find the `com.socrata.coordinator.common.secondary` section of your `soda2.conf` file.
    * Add spandex to `defaultGroups`:
```
      secondary {
        defaultGroups = [pg,spandex]
        ...
```
    * Add the following element under `groups`:
```
      spandex {
        numReplicas = 1
        instances = [spandex]
      }
```
    * Add the following element under `instances`:
```
    spandex {
        secondaryType = spandex
        config = ${com.socrata.spandex}
        numWorkers = 2
    }
```

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
