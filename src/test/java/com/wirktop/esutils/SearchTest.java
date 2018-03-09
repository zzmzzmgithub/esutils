package com.wirktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.wirktop.esutils.index.IndexBatch;
import com.wirktop.esutils.index.Indexer;
import com.wirktop.esutils.search.Scroll;
import com.wirktop.esutils.search.Search;
import com.wirktop.esutils.search.SearchIterator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Cosmin Marginean
 */
public class SearchTest extends TestBase {

    @Test(expected = IllegalArgumentException.class)
    public void testNoBucket() throws Exception {
        new ElasticSearchClient(Arrays.asList("localhost:9300"), "x")
                .search(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoIndex() throws Exception {
        new ElasticSearchClient(Arrays.asList("localhost:9300"), "x")
                .search(new DataBucket(null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoClient() throws Exception {
        new Search(null, null);
    }

    @Test
    public void testGetMap() throws Exception {
        Indexer indexer = indexer("test-get-map");
        Search search = search("test-get-map");
        String document = randomDoc();
        String id = indexer.indexJson(document);
        Map<String, Object> map = search.getMap(id);
        assertSame(new JSONObject(map), new JSONObject(document));
    }

    @Test
    public void testGetStr() throws Exception {
        Indexer indexer = indexer("test-get-str");
        Search search = search("test-get-str");
        String document = randomDoc();
        String id = indexer.indexJson(document);
        String jsonDoc = search.getJson(id);
        assertSame(new JSONObject(jsonDoc), new JSONObject(document));
    }

    @Test
    public void testExists() throws Exception {
        Indexer indexer = indexer("test-exists");
        Search search = search("test-exists");
        String document = randomDoc();
        String id = indexer.indexJson(document);
        Assert.assertTrue(search.exists(id));
        Assert.assertFalse(search.exists(id + "x"));
    }

    @Test
    public void testGetPojo() throws Exception {
        Indexer indexer = indexer("test-get-pojo");
        Search search = search("test-get-pojo");
        TestPojo document = docAsPojo("pojo1.json", TestPojo.class);
        String id = indexer.indexObject(document);
        assertSamePojo1(search, document, id);
        assertSame(new JSONObject(pojoToString(document)), new JSONObject(pojoToString(search.get(id, TestPojo.class))));
    }

    @Test
    public void testGetDocument() throws Exception {
        Indexer indexer = indexer("test-get-document");
        Search search = search("test-get-document");
        TestPojo document = docAsPojo("pojo1.json", TestPojo.class);
        String id = indexer.indexObject(document);
        assertSamePojo1(search, document, id);
        Document responseDoc = search.getDocument(id);
        assertSame(new JSONObject(pojoToString(document)), new JSONObject(responseDoc.getSource()));
        Assert.assertTrue(responseDoc.getVersion() > 0);
    }

    @Test
    public void testSearchPojo() throws Exception {
        String index = "test-search-pojo";
        Indexer indexer = indexer(index);
        Search search = search(index);
        int docCount = 243;
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, docCount);
        List<Person> docs = search.search(QueryBuilders.matchAllQuery())
                .map(search.hitToPojo(Person.class))
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }

    @Test
    public void testSearchDocs() throws Exception {
        String index = "test-search-docs";
        Indexer indexer = indexer(index);
        Search search = search(index);
        int docCount = 243;
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, docCount);
        List<Document> docs = search.search(QueryBuilders.matchAllQuery())
                .map(Search.HIT_TO_DOC)
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
        for (Document doc : docs) {
            Assert.assertTrue(doc.getVersion() > 0);
        }
    }

    @Test
    public void testSearchHits() throws Exception {
        String index = "test-search-hits";
        Search search = search(index);
        Indexer indexer = indexer(index);
        int docCount = 119;
        try (IndexBatch batch = indexer.batch()) {
            generateDocuments(docCount, false)
                    .forEach((hit) -> batch.add(UUID.randomUUID().toString(), hit));
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.search(QueryBuilders.matchAllQuery(), 34)
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }

    @Test
    public void testSearchHitsDefaultPageSize() throws Exception {
        String index = "test-search-hits-default-page-size";
        Indexer indexer = indexer(index);
        Search search = search(index);
        int docCount = 361;
        try (IndexBatch batch = indexer.batch()) {
            generateDocuments(docCount, false)
                    .forEach((hit) -> batch.add(UUID.randomUUID().toString(), hit));
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.search(QueryBuilders.matchAllQuery())
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }

    @Test
    public void testMissingDoc() throws Exception {
        Indexer indexer = indexer("test-missing-doc");
        Search search = search("test-missing-doc");
        String id = indexer.indexJson(randomDoc());
        Assert.assertNull(search.get("askjdkjbadfasdf", TestPojo.class));
        Assert.assertNull(search.getJson("askjdkjbadfas12312093df"));
        Assert.assertNull(search.getMap(UUID.randomUUID().toString()));
        Assert.assertNotNull(id);
    }

    @Test
    public void testCount() throws Exception {
        int docCount = 100;
        String index = "test-count";
        Indexer indexer = indexer(index);
        Search search = search(index);
        esClient().admin().createTemplate("aaa", getClass().getResourceAsStream("/templates/template-index.json"));
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, 100);
        Assert.assertEquals(100, search.count());
        Assert.assertEquals(100, search.count(null));
        Assert.assertEquals(100, search.count(QueryBuilders.termQuery("gender", "male")) + search.count(QueryBuilders.termQuery("gender", "female")));
    }

    @Test
    public void testStream() throws Exception {
        int docCount = 100;
        String index = "test-stream";
        Search search = search(index);
        Indexer indexer = indexer(index);
        esClient().admin().createTemplate("aaa", getClass().getResourceAsStream("/templates/template-index.json"));
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, 100);

        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        search.search(QueryBuilders.termQuery("gender", "female"), out1);
        JSONObject r1 = new JSONObject(new String(out1.toByteArray(), StandardCharsets.UTF_8));

        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        search.search(QueryBuilders.termQuery("gender", "male"), out2);
        JSONObject r2 = new JSONObject(new String(out2.toByteArray(), StandardCharsets.UTF_8));

        Assert.assertEquals(100, r1.getJSONObject("hits").getInt("total") + r2.getJSONObject("hits").getInt("total"));
    }

    @Test
    public void testStreamLimit() throws Exception {
        int docCount = 12000;
        String index = "test-stream-limit";
        Search search = search(index);
        Indexer indexer = indexer(index);
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, docCount);
        int count[] = {0};
        search.search(QueryBuilders.matchAllQuery())
                .forEach(hit -> {
                    count[0]++;
                });
        Assert.assertEquals(SearchIterator.MAX_RESULTS, count[0]);
    }

    @Test
    public void testStreamLimit2() throws Exception {
        int docCount = 4597;
        String index = "test-stream-limit2";
        Search search = search(index);
        Indexer indexer = indexer(index);
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, docCount);
        int count[] = {0};
        search.search(QueryBuilders.matchAllQuery())
                .forEach(hit -> {
                    count[0]++;
                });
        Assert.assertEquals(docCount, count[0]);
    }


    @Test
    public void testStreamPojo() throws Exception {
        int docCount = 100;
        String index = "test-streampojo";
        Search search = search(index);
        Indexer indexer = indexer(index);
        esClient().admin().createTemplate("aaa", getClass().getResourceAsStream("/templates/template-index.json"));
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, 100);

        ArrayList<Person> people = new ArrayList<>();
        search.search(QueryBuilders.matchAllQuery())
                .map(search.hitToPojo(Person.class))
                .forEach((people::add));

        Assert.assertEquals(100, people.size());
    }

    @Test
    public void testCustomBucket() throws Exception {
        CustomBucket bucket = new CustomBucket("private-custom-index", "custom1");
        Indexer indexer = esClient().indexer(bucket);
        String document = randomDoc();
        String id = indexer.indexJson(document);
        Map<String, Object> indexedDoc = getMap("custom1---private-custom-index", id);
        Assert.assertNotNull(indexedDoc);
        assertSame(new JSONObject(document), indexedDoc);
    }

    @Test(expected = SearchException.class)
    public void testFailDeserialize() throws Exception {
        DataBucket bucket = new DataBucket("test-fail-deserialize");
        Search search = esClient().search(bucket);
        Indexer indexer = esClient().indexer(bucket);
        indexer.indexJson("a", randomDoc());
        search.get("a", PojoSerialize.class);
    }

    @Test
    public void testScrollFullIndex() throws Exception {
        ElasticSearchClient client = new ElasticSearchClient(client());
        List<Document> docs1 = generateDocuments(100, false)
                .stream()
                .map((docStr) -> new Document(UUID.randomUUID().toString(), docStr))
                .collect(Collectors.toList());
        List<Document> docs2 = generateDocuments(100, false)
                .stream()
                .map((docStr) -> new Document(UUID.randomUUID().toString(), docStr))
                .collect(Collectors.toList());

        String index = "test-scroll-full-index";
        client.indexer(new DataBucket(index))
                .bulkIndex(docs1);
        waitForIndexedDocs(index, 100);

        client.indexer(new DataBucket(index))
                .bulkIndex(docs2);
        waitForIndexedDocs(index, 200);

        long count = Scroll.scrollIndex(esClient(), index).count();
        Assert.assertEquals(200, count);
        Assert.assertEquals(200, client.search(new DataBucket(index)).count());
    }

    @Test
    public void testCustomObjectMapper() throws Exception {
        DataBucket bucket = new DataBucket("test-custom-object-mapper");
        Search search = esClient().search(bucket);
        Indexer indexer = esClient().indexer(bucket);
        PojoSerialize pojo = new PojoSerialize();
        Instant time = pojo.getTime();
        String id = indexer.indexObject(pojo);
        JSONObject defaultSerialized = new JSONObject(search.getJson(id));
        Assert.assertEquals(defaultSerialized.getJSONObject("time").getInt("nano"), time.getNano());
        Assert.assertEquals(defaultSerialized.getJSONObject("time").getInt("epochSecond"), time.getEpochSecond());

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Instant.class, new InstantSerializer());
        module.addDeserializer(Instant.class, new InstantDeserializer());
        objectMapper.registerModule(module);

        DataBucket bucket2 = new DataBucket("test-custom-object-mapper2");
        Search search2 = esClient().search(bucket2);
        esClient().setObjectMapper(objectMapper);
        String newId = esClient().indexer(bucket2).indexObject(pojo);
        String str = search2.getJson(newId);
        Assert.assertEquals("{\"time\":\"" + time.toString() + "\"}", str);
        Assert.assertEquals(time, search2.get(newId, PojoSerialize.class).getTime());
        Assert.assertEquals(time.toString(), new JSONObject(search2.getJson(newId)).getString("time"));
    }

    public static final class PojoSerialize {
        private Instant time = Instant.now();

        public Instant getTime() {
            return time;
        }

        public void setTime(Instant time) {
            this.time = time;
        }
    }

    private static class CustomBucket extends DataBucket {

        private String prefix;

        public CustomBucket(String index, String prefix) {
            super(index);
            this.prefix = prefix;
        }

        @Override
        public String getIndex() {
            String index = super.getIndex();
            String prefix = this.prefix + "---";
            return index.startsWith(prefix) ? index : prefix + index;
        }
    }
}
