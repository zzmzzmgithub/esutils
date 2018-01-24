# ElasticSearch Utils

## About
**esutils** is a Java library that aims to make interaction with ElasticSearch a bit more fluent.
* Javadocs: https://wirktop.github.io/esutils/current/apidocs/index.html
* Code coverage: https://wirktop.github.io/esutils/current/code-coverage/index.html
* License: Apache 2.0 http://www.apache.org/licenses/LICENSE-2.0.txt
* Unit tests download and run an ElasticSearch instance locally (embedded is not supported anymore so
we shouldn't be testing with it: https://www.elastic.co/blog/elasticsearch-the-server)
* We are using [Jackson](https://github.com/FasterXML/jackson) for serialization/deserialization
## Highlights

* Stream search results
```
Stream<Person> people = search.search(...)
                              .map(search.hitToPojo(Person.class));
```

* Batch Indexing
```
try (IndexBatch batch = indexer.batch()) {
    batch.add(doc);
}

```
* Alias-wrapped bucket: https://github.com/wirktop/esutils/wiki/AliasWrappedBucket
## Maven

```
<dependency>
  <groupId>com.wirktop</groupId>
  <artifactId>esutils</artifactId>
  <version>0.9.7</version>
</dependency>
```

## High level concepts
The design revolves around an [`ElasticSearchClient`](https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/ElasticSearchClient.html) component
which produces sub-components like [`Search`](https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/search/Search.html),
[`Scroll`](https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/search/Scroll.html),
[`Indexer`](https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/index/Indexer.html) or
[`Admin`](https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/Admin.html).

`Search` and `Indexer` instances are bound to a [`DataBucket`](https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/DataBucket.html) which is a "pointer" to
an (index,type) tuple.

`DataBucket` can be extended for custom behaviours like prefixing/suffixing index or type names, etc. It is the easiest way to achieve dynamic names for the
index (or type) at runtime, and can be useful for multi-tenancy, etc. Unit tests contain such customisation examples (check `DataBucket` subclasses in the project).

A more elaborate customisation of `DataBucket` is [`AliasWrappedBucket`](https://github.com/wirktop/esutils/wiki/AliasWrappedBucket).

## Creating a client instance
You can pass either an instance of `org.elasticsearch.client.Client` or, alternatively, a list of hostname:port tuples and a cluster name. 
```
org.elasticsearch.client.Client client = ...
ElasticSearchClient esClient = new ElasticSearchClient(client);
ElasticSearchClient esClient2 = new ElasticSearchClient(Arrays.asList("localhost:9300"), "cluster-x");
```

## Indexing
https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/index/Indexer.html
#### Index documents
```
SomePojo object = ...
Indexer indexer = esClient.indexer(bucket);
indexer.indexObject(object);
indexer.indexObject(id, object);
indexer.indexObject(id, object, true);
indexer.indexJson(jsonDocAsString);
```
You can optionally pass an `id` to specify the id to index with, and a `boolean` to wait for refresh (defaults to `false`).
The `index()` methods always return the `_id` of the newly indexed document.

Certain index methods accept a [Document](https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/Document.html) instance which is a simple container 
for both an ID and the content of the document. For bulk indexing this is particularly important, where passing an ID for each document might be required.

#### Bulk index documents
```
Collection<Document> documents = ...
esClient.indexer(bucket).bulkIndex(documents);
```

#### Batch indexing
`IndexBatch` is useful when indexing a `Stream` or any other data set with unknown size. The component retains a temporary buffer and triggers an `indexer.bulkIndex()`
automatically whenever the number of items in the buffer reaches a given size (the batch size). It also implements `AutoCloseable` to deal with the last items that
might not form a complete batch for an automatic index.  
```
Stream<Map<String,Object>> docs = ...
try (IndexBatch batch = indexer.batch(100)) {
    docs.forEach((doc) -> batch.add(doc));
}
```

## Searching and Scrolling
https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/search/Search.html
https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/search/Scroll.html

A series of search methods return a `Stream` that can be used to transparently process the complete result set, without having to worry about paging.
```
// Search (ES _search)
search.seach(QueryBuilders.matchAllQuery(), 10)
                .forEach((hit) -> {...});
                
// Scroll (ES _scroll)
search.scroll().scroll(QueryBuilders.matchAllQuery())
                .forEach((hit) -> {...});

// Seaveral get by _id methods
Map<String, Object> map = search.getMap(id);
String jsonDoc = search.getStr(id);
TestPojo pojo = search.get(id, TestPojo.class);
```

## Admin
https://wirktop.github.io/esutils/current/apidocs/com/wirktop/esutils/Admin.html
```
ElasticSearchClient client = new ElasticSearchClient(...);
Admin admin = client.admin();

// Check index exists
boolean x = admin.indexExists(index);

// Create index
admin.createIndex(index, numberOfShards);

// Create template
client.admin().createTemplate("mytemplate", "{...}");
```
