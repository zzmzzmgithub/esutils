package com.wirktop.esutils;

import com.wirktop.esutils.index.Indexer;
import com.wirktop.esutils.search.Search;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Cosmin Marginean
 */
public class AdminTest extends TestBase {

    @Test
    public void testCreateBuckets() throws Exception {
        DataBucket bucket = new DataBucket("index");
        Assert.assertEquals("index", bucket.getIndex());

        AliasWrappedBucket aliasWrappedBucket = new AliasWrappedBucket("wrappedindex");
        Assert.assertEquals("wrappedindex", aliasWrappedBucket.getIndex());

        MyBucket myBucket = new MyBucket("customindex");
        Assert.assertEquals("customindex", myBucket.getIndex());
    }

    @Test
    public void testCreateTemplate() throws Exception {
        ElasticClient client = new ElasticClient(client());
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
        ElasticClient client = new ElasticClient(client());
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
        ElasticClient client = new ElasticClient(client());
        Admin admin = client.admin();
        String index = "createindextest";
        Assert.assertFalse(admin.indexExists(index));
        admin.createIndex(index, 12);
        Assert.assertTrue(admin.indexExists(index));

        JSONObject json = new JSONObject(httpClient().target("http://localhost:9200/" + index).request().get().readEntity(String.class));
        Assert.assertEquals(json.getJSONObject(index).getJSONObject("settings").getJSONObject("index").getString("number_of_shards"), "12");
    }

    @Test
    public void testCreateIndexBucket() throws Exception {
        ElasticClient client = new ElasticClient(client());
        Admin admin = client.admin();
        String index = "createindextest-bucket";
        Assert.assertFalse(admin.indexExists(index));
        new DataBucket(index).createIndex(admin, 7);
        Assert.assertTrue(admin.indexExists(index));

        JSONObject json = new JSONObject(httpClient().target("http://localhost:9200/" + index).request().get().readEntity(String.class));
        Assert.assertEquals(json.getJSONObject(index).getJSONObject("settings").getJSONObject("index").getString("number_of_shards"), "7");
    }

    @Test
    public void testCreateIndexDefaultShards() throws Exception {
        ElasticClient client = new ElasticClient(client());
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
        ElasticClient client = new ElasticClient(client());
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

    @Test
    public void testRemoveIndex() throws Exception {
        ElasticClient client = new ElasticClient(client());
        Admin admin = client.admin();
        String index = "test-remove-index";
        admin.createIndex(index);
        Assert.assertTrue(admin.indexExists(index));
        admin.removeIndex(index);
        Assert.assertFalse(admin.indexExists(index));
    }

    @Test
    public void testCreateAliasWithFilter() throws Exception {
        ElasticClient client = new ElasticClient(client());
        Admin admin = client.admin();
        esClient().admin().createTemplate("index", getClass().getResourceAsStream("/templates/template-index.json"));
        String index = "testcreatealiaswithfilter";
        admin.createIndex(index);
        DataBucket bucket = new DataBucket(index);
        Indexer indexer = client.indexer(bucket);
        indexStructuredDocs(100, indexer);
        waitForIndexedDocs(index, 100);
        admin.createAlias("males-testcreatealiaswithfilter", QueryBuilders.termQuery("gender", "male"), index);
        admin.createAlias("females-testcreatealiaswithfilter", QueryBuilders.termQuery("gender", "female"), index);

        long males = client.search(new DataBucket("males-testcreatealiaswithfilter")).count();
        long females = client.search(new DataBucket("females-testcreatealiaswithfilter")).count();
        Assert.assertTrue(males > 0);
        Assert.assertTrue(females > 0);
        Assert.assertEquals(100, females + males);
    }

    @Test
    public void testRemoveAlias() throws Exception {
        ElasticClient client = new ElasticClient(client());
        Admin admin = client.admin();
        String alias = "aliasnameremovealias";
        String index = "indexnamealiasnameremovealias";
        admin.createIndex(index);
        admin.createAlias(alias, index);
        List<String> indices = admin.indicesForAlias(alias);
        Assert.assertEquals(1, indices.size());
        Assert.assertEquals(index, indices.get(0));
        admin.removeAlias(alias);
        Thread.sleep(1000);
        Assert.assertEquals(0, admin.indicesForAlias(alias).size());
    }

    @Test
    public void testGetShards() throws Exception {
        ElasticClient client = new ElasticClient(client());
        Admin admin = client.admin();
        String index = "indexgetshards";
        admin.createIndex(index, 17);
        Assert.assertEquals(17, admin.getShards(index));
    }

    @Test
    public void testCopyIndex() throws Exception {
        ElasticClient client = new ElasticClient(client());
        Admin admin = client.admin();

        int count = 319;
        List<Document> docs1 = generateDocuments(count, false)
                .stream()
                .map((docStr) -> new Document(UUID.randomUUID().toString(), docStr))
                .collect(Collectors.toList());

        String index = "index-test-copy";

        DataBucket bucket = new DataBucket(index);
        Search search = client.search(bucket);
        Indexer indexer = client.indexer(bucket);
        indexer.bulkIndex(docs1);
        waitForIndexedDocs(index, count);
        Assert.assertEquals(count, search.count());

        String cloneIndex = "index-test-copy-clone";
        admin.copyData(search.bucket().getIndex(), cloneIndex);
        waitForIndexedDocs(cloneIndex, count);
        Assert.assertEquals(count, client.search(bucket).count());
        Assert.assertEquals(319, client.search(new DataBucket(cloneIndex)).count());
    }

    public static final class MyBucket extends DataBucket {

        public MyBucket(String index) {
            super(index);
        }
    }
}
