how to add an index, not required  
```
curl -XPUT 'eel:9200/chicago?pretty'
```

how to add a field specifically, not allowed  
```
curl -XPUT 'eel:9200/chicago/crime_type?pretty'
```

how to add element by id  
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

how to add element, and pick up id from the response  
```
john@Kraken:~$ curl -XPOST 'eel:9200/chicago/crime_type?pretty' -d '{"name":"NARCOTICS"}'
{
  "_index" : "chicago",
  "_type" : "crime_type",
  "_id" : "AUnFuc3nzvgmWRUah8tF",
  "_version" : 1,
  "created" : true
}
```

