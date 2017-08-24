package com.worktop.esutils;

import com.worktop.esutils.index.IndexBatch;
import com.worktop.esutils.index.Indexer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Cosmin Marginean
 */
public class IndexerTest extends TestBase {

    @Test
    public void testIndexSimpleMap() throws Exception {
        Indexer indexer = search("index1", "type1").indexer();
        Map<String, Object> document = docAsMap("sample-doc-1.json");
        String newId = indexer.index(document);
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.search().index(), indexer.search().type(), newId));
    }

    @Test
    public void testIndexSimpleJson() throws Exception {
        Indexer indexer = search("index1", "type1").indexer();
        JSONObject document = docAsJson("sample-doc-1.json");
        String newId = indexer.index(document);
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.search().index(), indexer.search().type(), newId));
    }

    @Test
    public void testIndexSimpleString() throws Exception {
        Indexer indexer = search("index1", "type1").indexer();
        JSONObject document = docAsJson("sample-doc-1.json");
        String newId = indexer.index(document.toString());
        System.out.println("Got ID: " + newId);
        Assert.assertEquals(document.toString(), getString(indexer.search().index(), indexer.search().type(), newId));
    }

    @Test
    public void testIndexWithRefresh() throws Exception {
        Indexer indexer = search("index1", "type1").indexer();
        Map<String, Object> document = docAsMap("sample-doc-1.json");
        String newId = indexer.index(null, document);
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.search().index(), indexer.search().type(), newId));
    }

    @Test
    public void testIndexWithNoRefresh() throws Exception {
        indexAndTestMap(search("index1", "type1").indexer(), null, "sample-doc-1.json", true);
    }

    @Test
    public void testIndexMapNoId() throws Exception {
        indexAndTestMap(search("index1", "type1").indexer(), null, "sample-doc-1.json");
    }

    @Test(expected = MapperParsingException.class)
    public void testIndexMapWithUnderscoreIdInDoc() throws Exception {
        indexAndTestMap(search("index1", "type1").indexer(), null, "sample-doc-2.json");
    }

    @Test
    public void testIndexMapWithExplicitId() throws Exception {
        String id = UUID.randomUUID().toString();
        String newId = indexAndTestMap(search("index1", "type1").indexer(), id, "sample-doc-1.json");
        Assert.assertEquals(id, newId);
    }

    @Test
    public void testIndexJsonNoId() throws Exception {
        indexAndTestJson(search("index1", "type1").indexer(), null, "sample-doc-1.json");
    }

    @Test(expected = MapperParsingException.class)
    public void testIndexJsonWithUnderscoreIdInDoc() throws Exception {
        indexAndTestJson(search("index1", "type1").indexer(), null, "sample-doc-2.json");
    }

    @Test
    public void testIndexJsonWithExplicitId() throws Exception {
        String id = UUID.randomUUID().toString();
        String newId = indexAndTestJson(search("index1", "type1").indexer(), id, "sample-doc-1.json");
        Assert.assertEquals(id, newId);
    }

    @Test
    public void testIndexStringNoId() throws Exception {
        indexAndTestString(search("index1", "type1").indexer(), null, "sample-doc-1.json");
    }

    @Test(expected = MapperParsingException.class)
    public void testIndexStringWithUnderscoreIdInDoc() throws Exception {
        indexAndTestString(search("index1", "type1").indexer(), null, "sample-doc-2.json");
    }

    @Test
    public void testIndexStringWithExplicitId() throws Exception {
        String id = UUID.randomUUID().toString();
        String newId = indexAndTestString(search("index1", "type1").indexer(), id, "sample-doc-1.json");
        Assert.assertEquals(id, newId);
    }

    @Test
    public void testBulkJsons() throws Exception {
        String index = "index-bulk-jsons";
        Indexer indexer = search(index, "type1").indexer();
        List<JSONObject> documents = generateDocuments(25, false);
        indexer.bulkIndexJson(documents, null);
        testJsons(index, documents);
    }

    @Test
    public void testBulkJsons2() throws Exception {
        String index = "index-bulk-jsons2";
        Indexer indexer = search(index, "type1").indexer();
        List<JSONObject> documents = generateDocuments(25, false);
        indexer.bulkIndexJson(documents);
        testJsons(index, documents);
    }


    @Test(expected = SearchException.class)
    public void testBulkJsonsError() throws Exception {
        String index = "index-bulk-jsonerror";
        Indexer indexer = search(index, "type1").indexer();
        List<JSONObject> documents = generateDocuments(25, true);
        indexer.bulkIndexJson(documents);
    }


    @Test(expected = SearchException.class)
    public void testBulkJsonsError2() throws Exception {
        String index = "index-bulk-jsonerror2";
        Indexer indexer = search(index, "type1").indexer();
        List<JSONObject> documents = generateDocuments(25, true);
        indexer.bulkIndexJson(documents);
    }


    @Test(expected = SearchException.class)
    public void testBulkMapError() throws Exception {
        String index = "index-bulk-maperror";
        Indexer indexer = search(index, "type1").indexer();
        List<JSONObject> documents = generateDocuments(25, true);
        List<Map<String, Object>> maps = documents.stream()
                .map((jsonObject -> jsonObject.toMap()))
                .collect(Collectors.toList());
        indexer.bulkIndex(maps);
    }

    @Test(expected = SearchException.class)
    public void testBulkMapError2() throws Exception {
        String index = "index-bulk-maperror2";
        Indexer indexer = search(index, "type1").indexer();
        List<JSONObject> documents = generateDocuments(25, true);
        List<Map<String, Object>> maps = documents.stream()
                .map((jsonObject -> jsonObject.toMap()))
                .collect(Collectors.toList());
        indexer.bulkIndex(maps, null);
    }

    private void testJsons(String index, List<JSONObject> documents) throws InterruptedException {
        SearchResponse response = waitForIndexedDocs(index, 25);
        Assert.assertEquals(25, response.getHits().getTotalHits());
        response.getHits().forEach((hit) -> {
            int foundCount = 0;
            for (JSONObject doc : documents) {
                if (doc.toString().equals(new JSONObject(hit.getSourceAsString()).toString())) {
                    foundCount++;
                }
            }
            Assert.assertEquals(foundCount, 1);
        });
    }

    private SearchResponse waitForIndexedDocs(String index, int docCount) {
        SearchResponse response = null;
        do {
            response = client().prepareSearch(index)
                    .setSize(docCount)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet();
        } while (response.getHits().getTotalHits() < docCount);
        return response;
    }

    @Test
    public void testBulkMap() throws Exception {
        String index = "index-bulk-map";
        Indexer indexer = search(index, "type1").indexer();
        List<Map<String, Object>> documents = generateDocuments(25, false).stream()
                .map(JSONObject::toMap).collect(Collectors.toList());
        String id = "id";
        indexer.bulkIndex(documents, id);

        SearchResponse response = waitForIndexedDocs(index, 25);
        Assert.assertEquals(25, response.getHits().getTotalHits());
        response.getHits().forEach((hit) -> {
            int foundCount = 0;
            for (Map<String, Object> doc : documents) {
                JSONObject json = new JSONObject(doc);
                JSONObject hitJson = new JSONObject(hit.getSourceAsString());
                if (json.toString().equals(hitJson.toString())) {
                    Assert.assertEquals(json.getString(id), hitJson.getString(id));
                    Assert.assertEquals(json.getString(id), hit.getId());
                    foundCount++;
                }
            }
            Assert.assertEquals(foundCount, 1);
        });
    }


    @Test
    public void testBulkMap2() throws Exception {
        String index = "index-bulk-map2";
        Indexer indexer = search(index, "type1").indexer();
        List<Map<String, Object>> documents = generateDocuments(25, false).stream()
                .map(JSONObject::toMap).collect(Collectors.toList());
        indexer.bulkIndex(documents);

        SearchResponse response = waitForIndexedDocs(index, 25);
        Assert.assertEquals(25, response.getHits().getTotalHits());
        response.getHits().forEach((hit) -> {
            int foundCount = 0;
            for (Map<String, Object> doc : documents) {
                JSONObject json = new JSONObject(doc);
                JSONObject hitJson = new JSONObject(hit.getSourceAsString());
                if (json.toString().equals(hitJson.toString())) {
                    foundCount++;
                }
            }
            Assert.assertEquals(foundCount, 1);
        });
    }

    @Test
    public void testBulkString() throws Exception {
        String index = "index-bulk-string";
        Indexer indexer = search(index, "type1").indexer();
        List<String> documents = generateDocuments(25, false).stream()
                .map(JSONObject::toString).collect(Collectors.toList());
        indexer.bulkIndexJsonStr(documents);

        SearchResponse response = waitForIndexedDocs(index, 25);
        Assert.assertEquals(25, response.getHits().getTotalHits());
        response.getHits().forEach((hit) -> {
            int foundCount = 0;
            for (String doc : documents) {
                JSONObject json = new JSONObject(doc);
                JSONObject hitJson = new JSONObject(hit.getSourceAsString());
                if (json.toString().equals(hitJson.toString())) {
                    foundCount++;
                }
            }
            Assert.assertEquals(foundCount, 1);
        });
    }

    @Test
    public void testBatchIndex() throws Exception {
        String index = "batchindex";
        Indexer indexer = searchTcp(index, "typex").indexer();
        try (IndexBatch batch = indexer.batch()) {
            for (int i = 0; i < 50; i++) {
                batch.add(randomDoc());
                batch.add(randomDoc().toMap());
                batch.add(randomDoc().toString());
            }
        }
        SearchResponse response = waitForIndexedDocs(index, 150);
        Assert.assertEquals(response.getHits().getTotalHits(), 150);
    }

    @Test
    public void testBatchIndexWithSize() throws Exception {
        String index = "batchindex2";
        Indexer indexer = searchTcp(index, "typex").indexer();
        try (IndexBatch batch = indexer.batch(13)) {
            for (int i = 0; i < 50; i++) {
                batch.add(randomDoc());
                batch.add(randomDoc().toMap());
                batch.add(randomDoc().toString());
            }
        }
        SearchResponse response = waitForIndexedDocs(index, 150);
        Assert.assertEquals(response.getHits().getTotalHits(), 150);
    }

    @Test
    public void testBatchIndexWithSizeAndId() throws Exception {
        String index = "batchindex2";
        Indexer indexer = searchTcp(index, "typex").indexer();
        try (IndexBatch batch = indexer.batch(27, "id")) {
            for (int i = 0; i < 50; i++) {
                batch.add(randomDoc());
                batch.add(randomDoc().toMap());
                batch.add(randomDoc().toString());
            }
        }
        SearchResponse response = waitForIndexedDocs(index, 150);
        Assert.assertEquals(response.getHits().getTotalHits(), 150);
    }

    private String indexAndTestMap(Indexer indexer, String id, String docName) throws IOException {
        return indexAndTestMap(indexer, id, docName, false);
    }

    private String indexAndTestMap(Indexer indexer, String id, String docName, boolean refresh) throws IOException {
        Map<String, Object> document = docAsMap(docName);
        String newId = indexer.index(id, document, refresh);
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.search().index(), indexer.search().type(), newId));
        return newId;
    }

    private String indexAndTestJson(Indexer indexer, String id, String docName) throws IOException {
        JSONObject document = docAsJson(docName);
        String newId = indexer.index(id, document);
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.search().index(), indexer.search().type(), newId));
        return newId;
    }

    private String indexAndTestString(Indexer indexer, String id, String docName) throws IOException {
        JSONObject document = docAsJson(docName);
        String newId = indexer.index(id, document.toString());
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.search().index(), indexer.search().type(), newId));
        return newId;
    }
}
