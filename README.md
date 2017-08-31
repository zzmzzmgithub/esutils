# ElasticSearch Utils

## About
**esutils** is a Java library that adds some fluency to the interaction with ElasticSearch.
* Javadocs: https://cosmin-marginean.github.io/esutils/etc/javadoc/ 
* Code coverage: https://cosmin-marginean.github.io/esutils/etc/code-coverage/jacoco-ut/index.html
* License: Apache 2.0 http://www.apache.org/licenses/LICENSE-2.0.txt

##### Other considerations
* Unit tests use an actual ElasticSearch instance, starts/stops automatically on build (embedded is not supported anymore so we shouldn't be testing with it: https://www.elastic.co/blog/elasticsearch-the-server)

### Maven

```
<dependency>
  <groupId>com.wirktop</groupId>
  <artifactId>esutils</artifactId>
  <version>0.1.3</version>
</dependency>
```

## High level concepts
The design revolves around an `ElasticSearchClient` component which in turn can produce sub-components like `Search`, `Admin` etc.
The objective is to have _some_ separation of concerns, but to also have some uniformity in the API and component dependencies.

For simplicity, the `Search` component and its sub-components are designed to interact with a specific index and type, passed through
a `DataBucket` parameter. A `DataBucket` is a "pointer" to an (index,type) tuple. Custom functionality and naming conventions 
can then be implemented by extending this class. One example is `AliasWrappedBucket` (more on that below).

This way each instance is logically isolated and makes for a cleaner client code. It also gives the API user more flexibility in deciding 
which components should be application state and which can be produced dynamically.

## Creating a client instance
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
https://cosmin-marginean.github.io/esutils/etc/javadoc/com/wirktop/esutils/index/Indexer.html
#### Index documents
```
search.indexer().index(document);
search.indexer().index(id, document);
search.indexer().index(id, document, true);
```
You can optionally pass an `id` to specify the id to index with, and a `boolean` to wait for refresh (defaults to `false`).
The `index()` methods always return the `_id` of the newly indexed document.

The `document` object can be one of the following:
* `Map<String,Object>`
* `org.json.JSONObject`
* `String` (the JSON string for this document)
* Any Java object (this would serialize it to JSON)

#### Bulk index documents
```
Collection<JSONObject> jsonDocs = ...
search.indexer().bulkIndexJson(jsonDocs);
search.indexer().bulkIndexJson(jsonDocs, "myId"); // Pass an _id field to index with

Collection<Map<String,Object> documentsMap = ...
search.indexer().bulkIndex(documentsMap);
search.indexer().bulkIndex(documentsMap, "myId"); // Pass an _id field to index with
```
Note: The id field is checked against each document and only applied when found and of type `String` (no error reported when inconsistent).

#### Batch indexing
This is a useful mechanism to index a `Stream` or any other potentially unbounded data set.
`IndexBatch` implements `AutoCloseable` which means it requires no cleanup (i.e. bulk-indexing the last items that might not be a complete batch) 
```
Stream<JSONObject> docs = ...
try (IndexBatch batch = indexer.batch(100)) {
    docs.forEach((doc) -> batch.add(doc));
}
```

## Searching
https://cosmin-marginean.github.io/esutils/etc/javadoc/com/wirktop/esutils/search/Search.html
```
// Search (ES _search)
search.seach(QueryBuilders.matchAllQuery(), 10)
                .forEach((hit) -> {...});
                
// Scroll (ES _scroll)
search.scroll(QueryBuilders.matchAllQuery())
                .forEach((hit) -> {...});

// Seaveral get by _id methods
Map<String, Object> map = search.getMap(id);
JSONObject json = search.getJson(id);
String jsonDoc = search.getStr(id);
TestPojo pojo = search.get(id, TestPojo.class);
```

## Admin
https://cosmin-marginean.github.io/esutils/etc/javadoc/com/wirktop/esutils/admin/Admin.html
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
