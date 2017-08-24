package com.wirktop.esutils;

import com.wirktop.esutils.admin.Admin;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * @author Cosmin Marginean
 */
public class AdminTest extends TestBase {

    @Test
    public void testCreateTemplate() throws Exception {
        ElasticSearchClient client = new ElasticSearchClient(client());
        String template = "mytemplate";
        client.admin().createTemplate(template, getClass().getResourceAsStream("/templates/template1.json"));
        GetIndexTemplatesResponse response = client().admin().indices().getTemplates(new GetIndexTemplatesRequest(template)).actionGet();
        Assert.assertEquals(response.getIndexTemplates().size(), 1);
        JSONObject storedTemplate = new JSONObject(httpClient().target("http://localhost:9200/_template/mytemplate").request().get().readEntity(String.class));
        JSONObject matchTemplate = new JSONObject(IOUtils.toString(getClass().getResourceAsStream("/templates/template1-match.json"), StandardCharsets.UTF_8));
        assertSame(storedTemplate, matchTemplate);
    }

    @Test
    public void tesCreateTemplateStr() throws Exception {
        ElasticSearchClient client = new ElasticSearchClient(client());
        String template = "mytemplate2";
        client.admin().createTemplate(template, IOUtils.toString(getClass().getResourceAsStream("/templates/template2.json"), StandardCharsets.UTF_8));
        GetIndexTemplatesResponse response = client().admin().indices().getTemplates(new GetIndexTemplatesRequest(template)).actionGet();
        Assert.assertEquals(response.getIndexTemplates().size(), 1);
        JSONObject storedTemplate = new JSONObject(httpClient().target("http://localhost:9200/_template/mytemplate2").request().get().readEntity(String.class));
        JSONObject matchTemplate = new JSONObject(IOUtils.toString(getClass().getResourceAsStream("/templates/template2-match.json"), StandardCharsets.UTF_8));
        assertSame(storedTemplate, matchTemplate);
    }

    @Test
    public void testCreateIndex() throws Exception {
        ElasticSearchClient client = new ElasticSearchClient(client());
        Admin admin = client.admin();
        String index = "createindextest";
        Assert.assertFalse(admin.indexExists(index));
        admin.createIndex(index, 12);
        Assert.assertTrue(admin.indexExists(index));

        JSONObject json = new JSONObject(httpClient().target("http://localhost:9200/" + index).request().get().readEntity(String.class));
        Assert.assertEquals(json.getJSONObject(index).getJSONObject("settings").getJSONObject("index").getString("number_of_shards"), "12");
    }

    @Test
    public void testCreateIndexDefaultShards() throws Exception {
        ElasticSearchClient client = new ElasticSearchClient(client());
        Admin admin = client.admin();
        String index = "createindextest-default-shards";
        Assert.assertFalse(admin.indexExists(index));
        admin.createIndex(index);
        Assert.assertTrue(admin.indexExists(index));

        JSONObject json = new JSONObject(httpClient().target("http://localhost:9200/" + index).request().get().readEntity(String.class));
        Assert.assertEquals(json.getJSONObject(index).getJSONObject("settings").getJSONObject("index").getString("number_of_shards"), "5");
    }


    @Test
    public void testCreateIndexExists() throws Exception {
        ElasticSearchClient client = new ElasticSearchClient(client());
        Admin admin = client.admin();
        String index = "createindextest-exists";
        Assert.assertFalse(admin.indexExists(index));
        admin.createIndex(index);
        Assert.assertTrue(admin.indexExists(index));
        admin.createIndex(index);
        Assert.assertTrue(admin.indexExists(index));

        JSONObject json = new JSONObject(httpClient().target("http://localhost:9200/" + index).request().get().readEntity(String.class));
        Assert.assertEquals(json.getJSONObject(index).getJSONObject("settings").getJSONObject("index").getString("number_of_shards"), "5");
    }
}
