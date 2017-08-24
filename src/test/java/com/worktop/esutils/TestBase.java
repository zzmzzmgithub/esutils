package com.worktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Cosmin Marginean
 */
public abstract class TestBase {

    private static final String CLUSTER = "worktop-elasticsearch-test";

    private static TransportClient client;
    private static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public static void bootstrap() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", CLUSTER).build();
        client = new PreBuiltTransportClient(settings);
        InetAddress address = InetAddress.getByName("localhost");
        client.addTransportAddress(new InetSocketTransportAddress(address, 9300));
    }

    @AfterClass
    public static void shutdown() throws IOException {
    }

    public static TransportClient client() {
        return client;
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

    public void assertSame(JSONObject json1, Map<String, Object> map) throws IOException {
        assertSame(json1, new JSONObject(pojoToString(map)));
    }

    public void assertSame(Map<String, Object> map1, Map<String, Object> map2) throws IOException {
        assertSame(new JSONObject(pojoToString(map1)), new JSONObject(pojoToString(map2)));
    }

    private static String pojoToString(Object pojo) throws IOException {
        StringWriter writer = new StringWriter();
        objectMapper.writer().writeValue(writer, pojo);
        return writer.toString();
    }

    public Search search(String index, String type) {
        return Search.clientBuilder()
                .client(client())
                .index(index)
                .type(type)
                .build();
    }

    public Search searchTcp(String index, String type) {
        return Search.transportBuilder()
                .node("localhost:9300")
                .clusterName(CLUSTER)
                .index(index)
                .type(type)
                .build();
    }

    public List<JSONObject> generateDocuments(int count, boolean addError) throws IOException {
        List<JSONObject> docs = new ArrayList<>();
        String template = IOUtils.toString(getClass().getResourceAsStream("/docs/bulk-doc-template.json"), StandardCharsets.UTF_8);
        for (int i = 0; i < count; i++) {
            String docStr = template.replaceAll("VAR1", UUID.randomUUID().toString())
                    .replaceAll("VAR2", UUID.randomUUID().toString())
                    .replaceAll("VAR2", UUID.randomUUID().toString());
            docs.add(new JSONObject(docStr));
        }
        if (addError) {
            String template2 = IOUtils.toString(getClass().getResourceAsStream("/docs/bulk-doc-template-different.json"), StandardCharsets.UTF_8);
            String docStr = template2.replaceAll("VAR1", UUID.randomUUID().toString())
                    .replaceAll("VAR2", UUID.randomUUID().toString())
                    .replaceAll("VAR2", UUID.randomUUID().toString());
            docs.add(new JSONObject(docStr));
        }
        return docs;
    }

    public JSONObject randomDoc() throws IOException {
        String template = IOUtils.toString(getClass().getResourceAsStream("/docs/bulk-doc-template.json"), StandardCharsets.UTF_8);
        String docStr = template.replaceAll("VAR1", UUID.randomUUID().toString())
                .replaceAll("VAR2", UUID.randomUUID().toString())
                .replaceAll("VAR2", UUID.randomUUID().toString());
        return new JSONObject(docStr);
    }
}
