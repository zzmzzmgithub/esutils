package com.wirktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wirktop.esutils.index.IndexBatch;
import com.wirktop.esutils.search.Search;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.glassfish.jersey.client.ClientConfig;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author Cosmin Marginean
 */
public abstract class TestBase {

    private static final String CLUSTER = "wirktop-esutils-test";
    protected static ObjectMapper objectMapper = new ObjectMapper();
    private static TransportClient client;
    private static ElasticSearchClient esClient;

    @BeforeClass
    public static void bootstrap() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", CLUSTER).build();
        client = new PreBuiltTransportClient(settings);
        InetAddress address = InetAddress.getByName("localhost");
        client.addTransportAddress(new InetSocketTransportAddress(address, 9300));
        esClient = new ElasticSearchClient(client());
    }

    @AfterClass
    public static void shutdown() throws IOException {
    }

    public static TransportClient client() {
        return client;
    }

    public static String pojoToString(Object pojo) throws IOException {
        StringWriter writer = new StringWriter();
        objectMapper.writer().writeValue(writer, pojo);
        return writer.toString();
    }

    protected Map<String, Object> getMap(String index, String type, String id) {
        return client.prepareGet()
                .setIndex(index)
                .setType(type)
                .setId(id)
                .execute()
                .actionGet()
                .getSourceAsMap();
    }

    protected String getString(String index, String type, String id) {
        return client.prepareGet()
                .setIndex(index)
                .setType(type)
                .setId(id)
                .execute()
                .actionGet()
                .getSourceAsString();
    }

    public JSONObject docAsJson(String docName) throws IOException {
        String str = IOUtils.toString(getClass().getResourceAsStream("/docs/" + docName), StandardCharsets.UTF_8);
        return new JSONObject(str);
    }

    public Map<String, Object> docAsMap(String docName) throws IOException {
        String str = IOUtils.toString(getClass().getResourceAsStream("/docs/" + docName), StandardCharsets.UTF_8);
        return objectMapper.readValue(str, Map.class);
    }

    public <T> T docAsPojo(String docName, Class<T> pojoClass) throws IOException {
        String str = IOUtils.toString(getClass().getResourceAsStream("/docs/" + docName), StandardCharsets.UTF_8);
        return objectMapper.readValue(str, pojoClass);
    }

    public void assertSame(JSONObject json1, JSONObject json2) {
        System.out.println("Comparing: " + json1.toString());
        System.out.println("     With: " + json2.toString());
        Assert.assertEquals(json1.toString(), json2.toString());
    }

    protected void assertSamePojo1(Search search, TestPojo document, String newId) {
        TestPojo stored = search.get(newId, TestPojo.class);
        Assert.assertEquals(stored, document);
        Assert.assertEquals(stored.getId(), "one");
        Assert.assertEquals(stored.getName(), "John Smith");
        Assert.assertEquals(stored.getAge(), 92);
    }

    public void assertSame(JSONObject json1, Map<String, Object> map) throws IOException {
        assertSame(json1, new JSONObject(pojoToString(map)));
    }

    public void assertSame(Map<String, Object> map1, Map<String, Object> map2) throws IOException {
        assertSame(new JSONObject(pojoToString(map1)), new JSONObject(pojoToString(map2)));
    }

    public Search search(String index, String type) {
        return esClient().search(new DataBucket(index, type));
    }

    public ElasticSearchClient esClient() {
        return esClient;
    }

    public Search searchTcp(String index, String type) {
        return new ElasticSearchClient(Arrays.asList("localhost:9300"), CLUSTER).search(new DataBucket(index, type));
    }

    public List<String> generateDocuments(int count, boolean addError) throws IOException {
        List<String> docs = new ArrayList<>();
        String template = IOUtils.toString(getClass().getResourceAsStream("/docs/bulk-doc-template.json"), StandardCharsets.UTF_8);
        for (int i = 0; i < count; i++) {
            String docStr = template.replaceAll("VAR1", UUID.randomUUID().toString())
                    .replaceAll("VAR2", UUID.randomUUID().toString())
                    .replaceAll("VAR2", UUID.randomUUID().toString());
            docs.add(docStr);
        }
        if (addError) {
            String template2 = IOUtils.toString(getClass().getResourceAsStream("/docs/bulk-doc-template-different.json"), StandardCharsets.UTF_8);
            String docStr = template2.replaceAll("VAR1", UUID.randomUUID().toString())
                    .replaceAll("VAR2", UUID.randomUUID().toString())
                    .replaceAll("VAR2", UUID.randomUUID().toString());
            docs.add(docStr);
        }
        return docs;
    }

    public String randomDoc() throws IOException {
        String template = IOUtils.toString(getClass().getResourceAsStream("/docs/bulk-doc-template.json"), StandardCharsets.UTF_8);
        String docStr = template.replaceAll("VAR1", UUID.randomUUID().toString())
                .replaceAll("VAR2", UUID.randomUUID().toString())
                .replaceAll("VAR2", UUID.randomUUID().toString());
        return docStr;
    }

    protected SearchResponse waitForIndexedDocs(String index, int docCount) {
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

    public Client httpClient() {
        ClientConfig config = new ClientConfig();
        return ClientBuilder.newClient(config);
    }

    public void indexStructuredDocs(int docCount, Search search) throws Exception {
        Random random = new Random();
        try (IndexBatch batch = search.indexer().batch()) {
            JSONObject jsonObject = docAsJson("doc-template-mapped.json");
            for (int i = 0; i < docCount; i++) {
                JSONObject json = new JSONObject(jsonObject.toString());
                json.put("name", UUID.randomUUID().toString());
                json.put("age", Math.abs(random.nextInt()) % 100);
                json.put("gender", i % 2 == 0 ? "male" : "female");
                batch.add(UUID.randomUUID().toString(), json.toString());
            }
        }
    }
}
