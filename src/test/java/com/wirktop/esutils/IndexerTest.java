package com.wirktop.esutils;

import com.wirktop.esutils.index.IndexBatch;
import com.wirktop.esutils.index.Indexer;
import com.wirktop.esutils.search.Search;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Cosmin Marginean
 */
public class IndexerTest extends TestBase {

    @Test
    public void testIndexDocument() throws Exception {
        Search search = search("indextestindexdocument", "type1");
        Indexer indexer = indexer("indextestindexdocument", "type1");
        String doc = randomDoc();
        indexer.indexDocument(new Document("123", doc));
        Assert.assertEquals(search.getJson("123"), doc);
    }

    @Test
    public void testIndexDocumentWithRefresh() throws Exception {
        Search search = search("indextestindexdocumentrefresh", "type1");
        Indexer indexer = indexer("indextestindexdocumentrefresh", "type1");
        String doc = randomDoc();
        indexer.indexDocument(new Document("345345", doc), true);
        Assert.assertEquals(search.getJson("345345"), doc);
    }

    @Test
    public void testIndexPojo() throws Exception {
        Search search = search("index1", "type1");
        Indexer indexer = indexer("index1", "type1");
        TestPojo document = docAsPojo("pojo1.json", TestPojo.class);
        String newId = indexer.indexObject(document);
        System.out.println("Got ID: " + newId);
        assertSamePojo1(search, document, newId);
    }

    @Test(expected = SearchException.class)
    public void testIndexPojoFail() throws Exception {
        Search search = search("index1", "type1");
        Indexer indexer = indexer("index1", "type1");
        indexer.indexObject("asdasdsa", new TestPojoBroken(), false);
    }

    @Test
    public void testIndexPojoId() throws Exception {
        Search search = search("index1", "type1");
        Indexer indexer = indexer("index1", "type1");
        String myId = UUID.randomUUID().toString();
        TestPojo document = docAsPojo("pojo1.json", TestPojo.class);
        String newId = indexer.indexObject(myId, document);
        System.out.println("Got ID: " + newId);
        Assert.assertEquals(myId, newId);
        assertSamePojo1(search, document, newId);
    }

    @Test
    public void testIndexPojoIdRefresh() throws Exception {
        Search search = search("index1", "type1");
        Indexer indexer = indexer("index1", "type1");
        String myId = UUID.randomUUID().toString();
        TestPojo document = docAsPojo("pojo1.json", TestPojo.class);
        String newId = indexer.indexObject(myId, document, true);
        System.out.println("Got ID: " + newId);
        Assert.assertEquals(myId, newId);
        assertSamePojo1(search, document, newId);
    }

    @Test
    public void testIndexSimpleMap() throws Exception {
        Indexer indexer = indexer("index1", "type1");
        Map<String, Object> document = docAsMap("sample-doc-1.json");
        String newId = indexer.indexObject(document);
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.bucket().getIndex(), indexer.bucket().getType(), newId));
    }

    @Test
    public void testIndexSimpleJson() throws Exception {
        Indexer indexer = indexer("index1", "type1");
        JSONObject document = docAsJson("sample-doc-1.json");
        String newId = indexer.indexJson(document.toString());
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.bucket().getIndex(), indexer.bucket().getType(), newId));
    }

    @Test
    public void testIndexSimpleString() throws Exception {
        Indexer indexer = indexer("index1", "type1");
        JSONObject document = docAsJson("sample-doc-1.json");
        String newId = indexer.indexJson(document.toString());
        System.out.println("Got ID: " + newId);
        Assert.assertEquals(document.toString(), getString(indexer.bucket().getIndex(), indexer.bucket().getType(), newId));
    }

    @Test
    public void testIndexWithRefresh() throws Exception {
        Indexer indexer = indexer("index1", "type1");
        JSONObject document = docAsJson("sample-doc-1.json");
        String newId = indexer.indexJson(null, document.toString());
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.bucket().getIndex(), indexer.bucket().getType(), newId));
    }

    @Test
    public void testIndexStringNoId() throws Exception {
        indexAndTestString(indexer("index1", "type1"), null, "sample-doc-1.json");
    }

    @Test(expected = MapperParsingException.class)
    public void testIndexStringWithUnderscoreIdInDoc() throws Exception {
        indexAndTestString(indexer("index1", "type1"), null, "sample-doc-2.json");
    }

    @Test
    public void testIndexStringWithExplicitId() throws Exception {
        String id = UUID.randomUUID().toString();
        String newId = indexAndTestString(indexer("index1", "type1"), id, "sample-doc-1.json");
        Assert.assertEquals(id, newId);
    }

    @Test
    public void testBulkString() throws Exception {
        String index = "index-bulk-string";
        Indexer indexer = indexer(index, "type1");
        List<String> documents = generateDocuments(25, false);
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
        Indexer indexer = indexerTcp(index, "typex");
        try (IndexBatch batch = indexer.batch()) {
            for (int i = 0; i < 50; i++) {
                batch.add(UUID.randomUUID().toString(), randomDoc());
                batch.addPojo(UUID.randomUUID().toString(), new JSONObject(randomDoc()).toMap());
            }
        }
        SearchResponse response = waitForIndexedDocs(index, 100);
        Assert.assertEquals(response.getHits().getTotalHits(), 100);
    }

    @Test
    public void testBatchIndexWithSize() throws Exception {
        String index = "batchindex2";
        Indexer indexer = indexerTcp(index, "typex");
        try (IndexBatch batch = indexer.batch(13)) {
            for (int i = 0; i < 50; i++) {
                batch.add(UUID.randomUUID().toString(), randomDoc());
                batch.addPojo(UUID.randomUUID().toString(), new JSONObject(randomDoc()).toMap());
            }
        }
        SearchResponse response = waitForIndexedDocs(index, 100);
        Assert.assertEquals(response.getHits().getTotalHits(), 100);
    }


    private String indexAndTestString(Indexer indexer, String id, String docName) throws IOException {
        JSONObject document = docAsJson(docName);
        String newId = indexer.indexJson(id, document.toString());
        System.out.println("Got ID: " + newId);
        assertSame(document, getMap(indexer.bucket().getIndex(), indexer.bucket().getType(), newId));
        return newId;
    }

    public static class TestPojoBroken {
        private Method m;

        public Method getM() {
            throw new RuntimeException("aa");
        }

        public void setM(Method m) {
            this.m = m;
        }
    }
}
