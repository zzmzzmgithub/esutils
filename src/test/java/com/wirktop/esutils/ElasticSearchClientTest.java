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
        AliasWrappedBucket bucket = new AliasWrappedBucket(indexBaseName, "mytype");
        Admin admin = client.admin();
        admin.createIndex(bucket);
        Assert.assertTrue(admin.indexExists(indexBaseName + "_000000000001"));
        Assert.assertTrue(admin.aliasExists(indexBaseName));
        Assert.assertFalse(admin.indexExists(indexBaseName));
        Collection<String> indexesForAlias = admin.indexesForAlias(indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), indexBaseName + "_000000000001");

        Search search = client.search(bucket);
        JSONObject document = randomDoc();
        String newId = search.indexer().index(document);
        Map<String, Object> storedDoc = search.getMap(newId);
        assertSame(document, storedDoc);
        Assert.assertFalse(admin.indexExists(indexBaseName));

        admin.wipe(bucket);

        Assert.assertFalse(admin.indexExists(indexBaseName + "_000000000001"));
        Assert.assertTrue(admin.indexExists(indexBaseName + "_000000000002"));
        Assert.assertTrue(admin.aliasExists(indexBaseName));
        Assert.assertFalse(admin.indexExists(indexBaseName));

        indexesForAlias = admin.indexesForAlias(indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), indexBaseName + "_000000000002");
        Assert.assertNull(search.getMap(newId));
    }

    @Test
    public void testAliasWrappedCustom() throws Exception {
        ElasticSearchClient client = esClient();
        String indexBaseName = "aliaswrappedcustom";
        String prefixyz = "prefixyz";
        String fullPrefix = prefixyz + "---";
        AliasWrappedBucket bucket = new CustomBucket(new AliasWrappedBucket(indexBaseName, "mytype"), prefixyz);
        Admin admin = client.admin();
        admin.createIndex(bucket);
        Assert.assertTrue(admin.indexExists(fullPrefix + indexBaseName + "_000000000001"));
        Assert.assertTrue(admin.aliasExists(fullPrefix + indexBaseName));
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));
        Collection<String> indexesForAlias = admin.indexesForAlias(fullPrefix + indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), fullPrefix + indexBaseName + "_000000000001");

        Search search = client.search(bucket);
        JSONObject document = randomDoc();
        String newId = search.indexer().index(document);
        Map<String, Object> storedDoc = search.getMap(newId);
        assertSame(document, storedDoc);
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));

        admin.wipe(bucket);

        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName + "_000000000001"));
        Assert.assertTrue(admin.indexExists(fullPrefix + indexBaseName + "_000000000002"));
        Assert.assertTrue(admin.aliasExists(fullPrefix + indexBaseName));
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));

        indexesForAlias = admin.indexesForAlias(fullPrefix + indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), fullPrefix + indexBaseName + "_000000000002");
        Assert.assertNull(search.getMap(newId));
    }

    private static class CustomBucket extends AliasWrappedBucket {

        private String prefix;

        public CustomBucket(AliasWrappedBucket bucket, String prefix) {
            super(bucket.getIndex(), bucket.getType());
            this.prefix = prefix;
        }

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
