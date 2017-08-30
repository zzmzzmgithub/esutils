package com.wirktop.esutils;

import com.wirktop.esutils.search.Search;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * @author Cosmin Marginean
 */
public class ElasticSearchClientTest extends TestBase {


    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNodes() throws Exception {
        new ElasticSearchClient(Arrays.asList("aaaa2229300"), "x");
    }

    @Test(expected = SearchException.class)
    public void testInvalidHost() throws Exception {
        new ElasticSearchClient(Arrays.asList("blowup:9300"), "x");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoCluster() throws Exception {
        new ElasticSearchClient(Arrays.asList("blowup:9300"), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoClient() throws Exception {
        new ElasticSearchClient(null);
    }

    @Test
    public void testAliasWrapped() throws Exception {
        ElasticSearchClient client = esClient();
        String indexBaseName = "aliaswrapped";
        AliasWrappedBucket bucket = client.aliasWrapped(new DataBucket(indexBaseName, "mytype"));
        bucket.createIndex(client.admin(), 7);
        Assert.assertTrue(client.admin().indexExists("aliaswrapped_000000000001"));
        Assert.assertTrue(client.admin().aliasExists(indexBaseName));
        Assert.assertFalse(client.admin().indexExists(indexBaseName));
        Collection<String> indexesForAlias = client.admin().indexesForAlias(indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), "aliaswrapped_000000000001");

        Search search = client.search(bucket);
        JSONObject document = randomDoc();
        String newId = search.indexer().index(document);
        Map<String, Object> storedDoc = search.getMap(newId);
        assertSame(document, storedDoc);
        Assert.assertFalse(client.admin().indexExists(indexBaseName));

        bucket.wipe();

        Assert.assertFalse(client.admin().indexExists("aliaswrapped_000000000001"));
        Assert.assertTrue(client.admin().indexExists("aliaswrapped_000000000002"));
        Assert.assertTrue(client.admin().aliasExists(indexBaseName));
        Assert.assertFalse(client.admin().indexExists(indexBaseName));

        indexesForAlias = client.admin().indexesForAlias(indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), "aliaswrapped_000000000002");
        Assert.assertNull(search.getMap(newId));
    }
}
