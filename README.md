# ElasticSearch Utils

THIS IS WORK IN PROGRESS

## About
**esutils** is a Java library that adds some fluency to the interaction with ElasticSeach from a Java environment.

Other considerations:
* Unit tests use an actual ElasticSearch instance, starts/stops automatically on build (embedded is not supported anymore so we shouldn't be testing with it: https://www.elastic.co/blog/elasticsearch-the-server)
* Code coverage: https://cosmin-marginean.github.io/esutils/etc/code-coverage/jacoco-ut/index.html
* License: Apache 2.0 http://www.apache.org/licenses/LICENSE-2.0.txt
 
## High level concepts
The design revolves around an `ElasticSearchClient` component which in turn can produce sub-components like `Search`, `Admin` etc.
The objective to have _some_ separation of concerns but also to have some uniformity in the API and component dependencies.

For simplicity, the `Search` component and its sub-components are designed to interact with a specific index and type (mandatory params).
This way each instance is somewhat isolated and makes for a cleaner client code. 

Example:
```
Client client = ...
ElasticSearchClient esClient1 = new ElasticSearchClient(client);
ElasticSearchClient esClient2 = new ElasticSearchClient(Arrays.asList("localhost:9300"), "cluster-x");

Search search = esClient1.search("index1", "type1");
Indexer = search.indexer(); 
```

## Indexing
#### Index documents
```
search.indexer().index(document);
search.indexer().index(id, document);
search.indexer().index(id, document, true);
```
You can optionally pass an `id` to specify an id to index with, and a `boolean` to wait for refresh (defaults to `false` when not specified).
Index methods return the `_id` of the indexed document.

`document` can be one of the following:
* `Map<String,Object>`
* `org.json.JSONObject`
* `String` (the JSON string for this document)
* Any Java object (this would serialize it to JSON)

#### Bulk index documents
```
Collection<JSONObject> documents = ...
search.indexer().bulkIndexJson(documents);

Collection<Map<String,Object> documentsMap = ...
search.indexer().bulkIndex(documents);
```

Additionally, you can specify a field that can be used as `_id` from either the `JSONObject` or the `Map` elements in the passed collection.
```
Collection<JSONObject> documents = ...
search.indexer().bulkIndexJson(documents, "myId");

Collection<Map<String,Object> documentsMap = ...
search.indexer().bulkIndex(documents, "id");
```
The field is checked against each document and only applied when found and of type `String` (no error reported when inconsistent).

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
#### Search data
```
search.seach(QueryBuilders.matchAllQuery(), 10)
                .forEach((hit) -> {...});
```

#### Scroll data
```
search.scroll(QueryBuilders.matchAllQuery())
                .forEach((hit) -> {...});
```

#### Get by ID
```
Map<String, Object> map = search.getMap(id);
JSONObject json = search.getJson(id);
String jsonDoc = search.getStr(id);
TestPojo pojo = search.get(id, TestPojo.class);
```

## Admin
#### Templates
```
ElasticSearchClient client = new ElasticSearchClient(..);
client.admin().createTemplate("mytemplate", "{...}");
```
