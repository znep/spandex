spandex
=======
autocomplete with elasticsearch

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

Also, you can interact directly with elastic search via its web API by browsing here (should show the entire index):

http://localhost:9200/spandex/_search?q=*

## Spandex-Secondary
Spandex-Secondary is a Soda Server secondary service that replicates the data required for autocomplete to Elastic Search.

```sh
$ cd $YOUR_SPANDEX_REPO
$ sbt clean assembly
$ ln -s $YOUR_SPANDEX_REPO/spandex-secondary/target/scala-2.10/spandex-secondary-assembly-*.jar ~/secondary-stores
$ psql -U blist -d datacoordinator
datacoordinator=# INSERT INTO secondary_stores_config (store_id, next_run_time, interval_in_seconds) values ('spandex', now(), 5)
```

TODO : Document soda2.conf changes
TODO : Document running SW and replicating to Spandex 
