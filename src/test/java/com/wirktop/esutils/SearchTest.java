package com.wirktop.esutils;

import com.wirktop.esutils.index.IndexBatch;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

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
    public void testInvalidNodes() throws Exception {
        Search.transportBuilder()
                .nodes(Arrays.asList("aaaa2229300"))
                .index("i1")
                .type("t1")
                .clusterName("x")
                .build();
    }

    @Test(expected = SearchException.class)
    public void testInvalidHost() throws Exception {
        Search.transportBuilder()
                .node("blowup:9300")
                .index("i1")
                .type("t1")
                .clusterName("x")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoCluster() throws Exception {
        Search.transportBuilder()
                .node("aaa:300")
                .index("i1")
                .type("t1")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoIndex() throws Exception {
        Search.clientBuilder()
                .client(client())
                .type("t1")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoType() throws Exception {
        Search.clientBuilder()
                .client(client())
                .index("i1")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoClient() throws Exception {
        Search.clientBuilder()
                .index("i1")
                .type("t1")
                .build();
    }

    @Test
    public void testGetJson() throws Exception {
        Search search = search("test-get-json", "type1");
        JSONObject document = randomDoc();
        String id = search.indexer().index(document);
        JSONObject json = search.getJson(id);
        assertSame(json, document);
    }

    @Test
    public void testGetMap() throws Exception {
        Search search = search("test-get-map", "type1");
        JSONObject document = randomDoc();
        String id = search.indexer().index(document);
        Map<String, Object> map = search.getMap(id);
        assertSame(new JSONObject(map), document);
    }

    @Test
    public void testGetStr() throws Exception {
        Search search = search("test-get-str", "type1");
        JSONObject document = randomDoc();
        String id = search.indexer().index(document);
        String jsonDoc = search.getStr(id);
        assertSame(new JSONObject(jsonDoc), document);
    }

    @Test
    public void testGetPojo() throws Exception {
        Search search = search("test-get-pojo", "type1");
        JSONObject document = randomDoc();
        String id = search.indexer().index(docAsPojo("pojo1.json", TestPojo.class));
        TestPojo pojo = search.get(id, TestPojo.class);
        assertSame(new JSONObject(pojoToString(pojo)), document);
    }

    @Test
    public void testScrollHits() throws Exception {
        String index = "test-scroll-hits";
        Search search = search(index, "type1");
        int docCount = 573;
        try (IndexBatch batch = search.indexer().batch()) {
            generateDocuments(docCount, false)
                    .forEach(batch::add);
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.scroll(QueryBuilders.matchAllQuery())
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }

    @Test
    public void testMissingDoc() throws Exception {
        Search search = search("test-missing-doc", "type1");
        Assert.assertNull(search.get("askjdkjbadfasdf", TestPojo.class));
        Assert.assertNull(search.getStr("askjdkjbadfas12312093df"));
        Assert.assertNull(search.getJson(UUID.randomUUID().toString()));
        Assert.assertNull(search.getMap(UUID.randomUUID().toString()));
    }
}
