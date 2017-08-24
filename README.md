# ElasticSearch Utils

THIS IS WORK IN PROGRESS

## About
**esutils** is a Java library that adds some fluency to the interaction with ElasticSeach in a Java environment. Please check the Features section for the complete set of functionalities.

Other considerations:
* Unit tests use an actual ElasticSearch instance (no unsupported embedded etc)
* Decent code coverage: https://cosmin-marginean.github.io/esutils/etc/code-coverage/jacoco-ut/index.html
* License: Apache 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
 
## Features
### High level concepts
The design revolves around a `Search` component which offers some direct ES operations (get, search, etc), but it also aggregates sub-components like `Indexer` and
`Admin` to attain _some_ separation of concerns but also to have some uniformity in the API and component dependencies.

For simplicity, the `Search` component and its sub-components are designed to interact with a specific index and type. This way each instance is somewhat isolated
and makes for a cleaner client code. 

### Builder pattern for client
```
    Search.builder()
          .client(client())
          .index(index)
          .type(type)
          .build();
```
Alternatively, you can pass the host details directly:
```
    Search.transportBuilder()
          .node("host1:9300")
          .node("host2:9300")
          .index(index)
          .type(type)
          .build();
```

### Index documents
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
* `String` (a the JSON string for this document)

### Bulk index documents
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

### Batch indexing
This is a useful mechanism to index a `Stream` or any other potentially unbounded data set.
`IndexBatch` implements `AutoCloseable` which means it requires no cleanup (i.e. bulk-indexing the last items that might not be a complete batch) 
```
Stream<JSONObject> docs = ...
try (IndexBatch batch = indexer.batch(100)) {
    docs.forEach((doc) -> batch.add(doc));
}

```