package com.wirktop.esutils;

import com.wirktop.esutils.index.IndexBatch;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
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
        assertSame(search.getJson(id), document);
    }

    @Test
    public void testGetMap() throws Exception {
        Search search = search("test-get-map", "type1");
        JSONObject document = randomDoc();
        String id = search.indexer().index(document);
        assertSame(new JSONObject(search.getMap(id)), document);
    }

    @Test
    public void testGetStr() throws Exception {
        Search search = search("test-get-str", "type1");
        JSONObject document = randomDoc();
        String id = search.indexer().index(document);
        assertSame(new JSONObject(search.getStr(id)), document);
    }

    @Test
    public void testGetPojo() throws Exception {
        Search search = search("test-get-str", "type1");
        JSONObject document = randomDoc();
        String id = search.indexer().index(document);
        assertSame(new JSONObject(search.getStr(id)), document);
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
}
