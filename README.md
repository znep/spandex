spandex
=======
autocomplete with elasticsearch

## Build & Run ##

```sh
$ cd spandex
$ ./sbt
> container:start
> browse
```

If `browse` doesn't launch your browser, manually open [http://localhost:8042/](http://localhost:8042/) in your browser.

You can search what is currently indexed in spandex like this (searches "crimeType" for strings starting with "nar"):

[http://localhost:8042/suggest/qnmj-8ku6/crimeType/nar](http://localhost:8042/suggest/qnmj-8ku6/crimeType/nar)

Also, you can interact directly with elastic search via its web API by browsing here (should show the entire index):

[http://localhost:9200/spandex/qnmj-8ku6/_search?q=*](http://localhost:9200/spandex/qnmj-8ku6/_search?q=*)
