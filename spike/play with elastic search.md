elasticsearch 1.4
=================

[reference](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/index.html)  
[autocomplete](http://www.elasticsearch.org/blog/you-complete-me/)  
[google group](https://groups.google.com/forum/?fromgroups#!forum/elasticsearch)  
[hosted es](https://qbox.io/)  

_bulk is available for all create update delete operations  
definitely use bulk operations. inserting 2M documents individually took 387 minutes overnight. On the plus side the index load was very low.  
bulk inserting 2M documents in one request took 34 seconds. Constructing the document took 21 seconds.  

SPIKE time
----------
 Creating a new index if 2M versus adding 2M to an existing index.  
it seems to take longer (~2x) to create a new index, but consumes less store space.  
my two vm cluster topped out at 79 shards over 8 indices.  

other ideas...  
-----------
use [Luke](https://code.google.com/p/luke/) 
or [Skywalker](https://www.google.com/url?q=https%3A%2F%2Fgithub.com%2Fjprante%2Felasticsearch-skywalker%23readme&sa=D&sntz=1&usg=AFQjCNEz6THdr8bohLv1ewqvIg0CQxC12A) 
to optimize   

optimize manually 
```/_optimize?max_num_segments=1```

index aliases  

index config mappings and settings:  
disable full document source storage, 
enable compression, 
explicitly set shards and replicas 
```  
{
  "settings": {
    "number_of_shards":   2,
    "number_of_replicas": 1,
    "analysis": {
      "filter": {
        "myStop": {
          "stopwords": [
            "the", "a", "an"
          ],
          "type": "stop"
    },
    "analyzer": {
      "ngram-index": {
        "tokenizer": "lowercase",
        "filter": [
          "myStop"
        ],
        "type": "custom"
      }
    },
    "similarity": {
      "search": {
        "type": "org.elasicsearch.index.similarity.CustomSimilarityProvider"
      },
      "index": {
        "type": "org.elasicsearch.index.similarity.CustomSimilarityProvider"
      }
    }
  },
  "mappings": {               
    "_source": {"enabled": "false"},
    "_all": {"enabled": "false"},
    "properties": {
      "freq": {
        "store": "yes",
        "compress": "true",
        "index_options": "docs",
        "omit_terms_freq_and_positions" : "true",
        "omit_norms": "true",
        "type": "long",
        "index": "not_analyzed"
      },
      "gram": {
        "store": "yes",
        "compress": "true",
        "index_options": "docs",
        "omit_terms_freq_and_positions" : "true",
        "omit_norms": "true",
        "type": "string",
        "analyzer": "ngram-index"
  }
}
```
