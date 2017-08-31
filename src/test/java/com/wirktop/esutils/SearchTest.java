package com.wirktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.wirktop.esutils.index.IndexBatch;
import com.wirktop.esutils.search.Search;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
                .search(new DataBucket(null, "aaa"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoType() throws Exception {
        new ElasticSearchClient(Arrays.asList("localhost:9300"), "x")
                .search(new DataBucket("aaa", null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoClient() throws Exception {
        new Search(null, null);
    }

    @Test
    public void testGetMap() throws Exception {
        Search search = search("test-get-map", "type1");
        String document = randomDoc();
        String id = search.indexer().indexJson(document);
        Map<String, Object> map = search.getMap(id);
        assertSame(new JSONObject(map), new JSONObject(document));
    }

    @Test
    public void testGetStr() throws Exception {
        Search search = search("test-get-str", "type1");
        String document = randomDoc();
        String id = search.indexer().indexJson(document);
        String jsonDoc = search.getStr(id);
        assertSame(new JSONObject(jsonDoc), new JSONObject(document));
    }

    @Test
    public void testGetPojo() throws Exception {
        Search search = search("test-get-pojo", "type1");
        TestPojo document = docAsPojo("pojo1.json", TestPojo.class);
        String id = search.indexer().indexObject(document);
        assertSamePojo1(search, document, id);
        assertSame(new JSONObject(pojoToString(document)), new JSONObject(pojoToString(search.get(id, TestPojo.class))));
    }

    @Test
    public void testScrollHits() throws Exception {
        String index = "test-scroll-hits";
        Search search = search(index, "type1");
        int docCount = 573;
        try (IndexBatch batch = search.indexer().batch()) {
            generateDocuments(docCount, false)
                    .forEach((hit) -> batch.add(UUID.randomUUID().toString(), hit.toString()));
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.scroll(QueryBuilders.matchAllQuery())
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }


    @Test
    public void testScrollNoFilter() throws Exception {
        String index = "test-scroll-nofilter";
        Search search = search(index, "type1");
        int docCount = 573;
        try (IndexBatch batch = search.indexer().batch()) {
            generateDocuments(docCount, false)
                    .forEach((hit) -> batch.add(UUID.randomUUID().toString(), hit.toString()));
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.scroll()
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }

    @Test
    public void testSearchHits() throws Exception {
        String index = "test-search-hits";
        Search search = search(index, "type1");
        int docCount = 573;
        try (IndexBatch batch = search.indexer().batch()) {
            generateDocuments(docCount, false)
                    .forEach((hit) -> batch.add(UUID.randomUUID().toString(), hit.toString()));
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.search(QueryBuilders.matchAllQuery(), 128)
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }


    @Test
    public void testSearchHitsDefaultPageSize() throws Exception {
        String index = "test-search-hits";
        Search search = search(index, "type1");
        int docCount = 573;
        try (IndexBatch batch = search.indexer().batch()) {
            generateDocuments(docCount, false)
                    .forEach((hit) -> batch.add(UUID.randomUUID().toString(), hit.toString()));
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.search(QueryBuilders.matchAllQuery())
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }

    @Test
    public void testMissingDoc() throws Exception {
        Search search = search("test-missing-doc", "type1");
        String id = search.indexer().indexJson(randomDoc());
        Assert.assertNull(search.get("askjdkjbadfasdf", TestPojo.class));
        Assert.assertNull(search.getStr("askjdkjbadfas12312093df"));
        Assert.assertNull(search.getMap(UUID.randomUUID().toString()));
        Assert.assertNotNull(id);
    }

    @Test
    public void testCount() throws Exception {
        int docCount = 100;
        String index = "test-count";
        Search search = search(index, "person");
        esClient().admin().createTemplate("aaa", getClass().getResourceAsStream("/templates/template-index.json"));
        indexStructuredDocs(docCount, search);
        waitForIndexedDocs(index, 100);
        Assert.assertEquals(100, search.count());
        Assert.assertEquals(100, search.count(null));
        Assert.assertEquals(100, search.count(QueryBuilders.termQuery("gender", "male")) + search.count(QueryBuilders.termQuery("gender", "female")));
    }

    @Test
    public void testStream() throws Exception {
        int docCount = 100;
        String index = "test-stream";
        Search search = search(index, "person");
        esClient().admin().createTemplate("aaa", getClass().getResourceAsStream("/templates/template-index.json"));
        indexStructuredDocs(docCount, search);
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
    public void testCustomBucket() throws Exception {
        Search search = esClient().search(new CustomBucket("private-custom-index", "mytype", "custom1"));
        String document = randomDoc();
        String id = search.indexer().indexJson(document);
        Map<String, Object> indexedDoc = getMap("custom1---private-custom-index", "mytype", id);
        Assert.assertNotNull(indexedDoc);
        assertSame(new JSONObject(document), indexedDoc);
    }

    @Test(expected = SearchException.class)
    public void testFailDeserialize() throws Exception {
        Search search = esClient().search(new DataBucket("test-fail-deserialize", "typemapped"));
        search.indexer().indexJson("a", randomDoc());
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
        client.search(new DataBucket(index, "type1"))
                .indexer()
                .bulkIndex(docs1);
        waitForIndexedDocs(index, 100);

        client.search(new DataBucket(index, "type2"))
                .indexer()
                .bulkIndex(docs2);
        waitForIndexedDocs(index, 200);

        long count = client.search(new DataBucket(index, "typex"))
                .scrollFullIndex()
                .count();
        Assert.assertEquals(200, count);
        Assert.assertEquals(100, client.search(new DataBucket(index, "type1")).count());
        Assert.assertEquals(100, client.search(new DataBucket(index, "type2")).count());
    }

    @Test
    public void testCustomObjectMapper() throws Exception {
        Search search = esClient().search(new DataBucket("test-custom-object-mapper", "typemapped"));
        PojoSerialize pojo = new PojoSerialize();
        Instant time = pojo.getTime();
        String id = search.indexer().indexObject(pojo);
        JSONObject defaultSerialized = new JSONObject(search.getStr(id));
        Assert.assertEquals(defaultSerialized.getJSONObject("time").getInt("nano"), time.getNano());
        Assert.assertEquals(defaultSerialized.getJSONObject("time").getInt("epochSecond"), time.getEpochSecond());

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Instant.class, new InstantSerializer());
        module.addDeserializer(Instant.class, new InstantDeserializer());
        objectMapper.registerModule(module);

        Search search2 = esClient().search(new DataBucket("test-custom-object-mapper2", "typemapped"));
        esClient().setObjectMapper(objectMapper);
        String newId = search2.indexer().indexObject(pojo);
        String str = search2.getStr(newId);
        Assert.assertEquals("{\"time\":\"" + time.toString() + "\"}", str);
        Assert.assertEquals(time, search2.get(newId, PojoSerialize.class).getTime());
        Assert.assertEquals(time.toString(), new JSONObject(search2.getStr(newId)).getString("time"));
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

        public CustomBucket(String index, String type, String prefix) {
            super(index, type);
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
