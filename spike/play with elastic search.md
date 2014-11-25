health and meta info  
```
john@Kraken:/usr/local/src/socrata/spandex$ curl 'eel:9200/_cat/health?v'
epoch      timestamp cluster       status node.total node.data shards pri relo init unassign 
1416425022 14:23:42  elasticsearch yellow          1         1     15  15    0    0       15 

john@Kraken:/usr/local/src/socrata/spandex$ curl 'eel:9200/_cat/nodes?v'
host ip        heap.percent ram.percent load node.role master name                  
Eel  127.0.1.1            7          14 0.00 d         *      Ahab                  
ayu  127.0.1.1            3          50 0.19 d         m      Radion the Atomic Man 

john@Kraken:/usr/local/src/socrata/spandex$ curl 'eel:9200/_cat/indices?v'
health status index    pri rep docs.count docs.deleted store.size pri.store.size 
yellow open   chicago    5   1         10            0     25.6kb         25.6kb 
yellow open   portland   5   1          0            0       575b           575b 
yellow open   customer   5   1          5            0     12.9kb         12.9kb 
```

how to create and delete an index 
```
john@Kraken:/usr/local/src/socrata/spandex$ curl -XPUT 'eel:9200/portland?pretty'
{
  "acknowledged" : true
}

john@Kraken:/usr/local/src/socrata/spandex$ curl -XDELETE 'eel:9200/portland?pretty'
{
  "acknowledged" : true
}
```

how to create a field specifically, not allowed  
```
curl -XPUT 'eel:9200/chicago/crime_type?pretty'
```

how to create element by id  
```
john@Kraken:~$ curl -XPUT 'eel:9200/chicago/crime_type/1?pretty' -d '{"name":"Hello World"}'
{
  "_index" : "chicago",
  "_type" : "crime_type",
  "_id" : "1",
  "_version" : 1,
  "created" : true
}
```

how to create element, and pick up id from the response  
```
john@Kraken:~$ curl -XPOST 'eel:9200/chicago/crime_type?pretty' -d '{"name":"NARCOTICS"}'
{
  "_index" : "chicago",
  "_type" : "crime_type",
  "_id" : "AUnFuc3nzvgmWRUah8tF",
  "_version" : 1,
  "created" : true
}
2>/dev/null |sed -n 's/\s*"_id" : "\([^"]*\)",/\1/ p'
```

how to update an element by id: with full document replacement or scripted modification  
```
john@Kraken:/usr/local/src/socrata/spandex$ curl -XPOST 'eel:9200/chicago/crime_type/AUnJhoprzvgmWRUah8tO/_update?pretty' -d '{"doc": {"name":"NARCOTICS", "count":42}}'
{
  "_index" : "chicago",
  "_type" : "crime_type",
  "_id" : "AUnJhoprzvgmWRUah8tO",
  "_version" : 2
}

john@Kraken:/usr/local/src/socrata/spandex$ curl -XPOST 'eel:9200/chicago/crime_type/AUnJhoprzvgmWRUah8tO/_update?pretty' -d '{"script": "ctx._source.count += 27"}'
{
  "_index" : "chicago",
  "_type" : "crime_type",
  "_id" : "AUnJhoprzvgmWRUah8tO",
  "_version" : 3
}
```

how to delete an element by id or by query  
```
john@Kraken:/usr/local/src/socrata/spandex$ curl -XDELETE 'eel:9200/chicago/crime_type/1?pretty'
{
  "found" : true,
  "_index" : "chicago",
  "_type" : "crime_type",
  "_id" : "1",
  "_version" : 2
}

john@Kraken:/usr/local/src/socrata/spandex$ curl -XDELETE 'eel:9200/chicago/crime_type/_query?pretty' -d '{"query":{"match":{"name":"NARCOTICS"}}}'
{
  "_indices" : {
    "chicago" : {
      "_shards" : {
        "total" : 5,
        "successful" : 5,
        "failed" : 0
      }
    }
  }
}
```

_bulk is available for all create update delete operations  
definitely use bulk operations. inserting 2M documents individually took 387 minutes overnight. On the plus side the index load was very low.  
bulk inserting 2M documents in one request took 34 seconds. Constructing the document took 21 seconds.  

additional meta data from elasticsearch service.  
```
curl 'eel:9200/_cat/shards?v'
curl 'eel:9200/_cluster/health?pretty'
curl 'eel:9200/_nodes?pretty'
```

SPIKE time. Creating a new index if 2M versus adding 2M to an existing index.  
it seems to take longer (~2x) to create a new index, but consumes less store space.  
my two vm cluster topped out at 79 shards over 8 indices.  
