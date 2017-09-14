# ElasticSearch Utils

## About
**esutils** is a Java library that aims to make interaction with ElasticSearch a bit more fluent.
* Javadocs: https://wirktop.github.io/esutils/etc/apidocs/index.html 
* Code coverage: https://wirktop.github.io/esutils/etc/code-coverage/jacoco-ut/index.html
* License: Apache 2.0 http://www.apache.org/licenses/LICENSE-2.0.txt

##### Other considerations
* Unit tests use an actual ElasticSearch instance, starts/stops automatically on build (embedded is not supported anymore so we shouldn't be testing with it: https://www.elastic.co/blog/elasticsearch-the-server)

### Maven

```
<dependency>
  <groupId>com.wirktop</groupId>
  <artifactId>esutils</artifactId>
  <version>0.3.0</version>
</dependency>
```

## High level concepts
The design revolves around an [`ElasticSearchClient`](https://wirktop.github.io/esutils/etc/apidocs/com/wirktop/esutils/ElasticSearchClient.html) component which can produce sub-components like [`Search`](https://wirktop.github.io/esutils/etc/apidocs/com/wirktop/esutils/Search.html) or [`Admin`](https://wirktop.github.io/esutils/etc/apidocs/com/wirktop/esutils/Admin.html).
The objective is to have _some_ separation of concerns, but also to achieve a certain fluency in the API and component dependencies.

For simplicity, the `Search` component and its sub-components are designed to interact with a specific index and type, passed through
a `DataBucket` parameter. A [`DataBucket`](https://wirktop.github.io/esutils/etc/apidocs/com/wirktop/esutils/DataBucket.html) is a "pointer" to an (index,type) tuple. Custom functionality and naming overrides 
can then be implemented by extending this class. One example is [`AliasWrappedBucket`](https://github.com/wirktop/esutils/wiki/AliasWrappedBucket).

This gives the API user more flexibility in deciding which components should be application state and which can be produced dynamically.

## Creating a client instance
You can pass either an instance of `org.elasticsearch.client.Client` or, alternatively, a list of hostname:port tuples and a cluster name. 
```
org.elasticsearch.client.Client client = ...
ElasticSearchClient esClient1 = new ElasticSearchClient(client);
ElasticSearchClient esClient2 = new ElasticSearchClient(Arrays.asList("localhost:9300"), "cluster-x");
```

## Create a `Search` instance
```
ElasticSearchClient esClient = ...
DataBucket dataBucket = new DataBucket("index1", "type1")

// The recommended approach to create an index when using DataBuckets 
esClient.admin().createIndex(dataBucket);

Search search = esClient.search(dataBucket); 
```

## Indexing
https://wirktop.github.io/esutils/etc/apidocs/com/wirktop/esutils/index/Indexer.html
#### Index documents
```
SomePojo document = ...
search.indexer().index(document);
search.indexer().index(id, document);
search.indexer().index(id, document, true);
```
You can optionally pass an `id` to specify the id to index with, and a `boolean` to wait for refresh (defaults to `false`).
The `index()` methods always return the `_id` of the newly indexed document.

Certain index methods accept a [Document](https://wirktop.github.io/esutils/etc/apidocs/com/wirktop/esutils/Document.html) instance which is a simple container 
for both an ID and the content of the document. For bulk indexing this is particularly important, where passing an ID for each document might be required.

#### Bulk index documents
```
Collection<Document> documents = ...
search.indexer().bulkIndex(documents);
```

#### Batch indexing
This is a useful mechanism to index a `Stream` or any other potentially unbounded data set.
`IndexBatch` implements `AutoCloseable` which means it requires no cleanup (i.e. bulk-indexing the last items that might not be a complete batch) 
```
Stream<Map<String,Object>> docs = ...
try (IndexBatch batch = indexer.batch(100)) {
    docs.forEach((doc) -> batch.add(doc));
}
```

## Searching
https://wirktop.github.io/esutils/etc/apidocs/com/wirktop/esutils/search/Search.html
```
// Search (ES _search)
search.seach(QueryBuilders.matchAllQuery(), 10)
                .forEach((hit) -> {...});
                
// Scroll (ES _scroll)
search.scroll(QueryBuilders.matchAllQuery())
                .forEach((hit) -> {...});

// Seaveral get by _id methods
Map<String, Object> map = search.getMap(id);
String jsonDoc = search.getStr(id);
TestPojo pojo = search.get(id, TestPojo.class);
```

## Admin
https://wirktop.github.io/esutils/etc/apidocs/com/wirktop/esutils/admin/Admin.html
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
