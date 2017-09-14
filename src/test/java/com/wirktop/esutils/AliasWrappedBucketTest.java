package com.wirktop.esutils;

import com.wirktop.esutils.search.Search;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

/**
 * @author Cosmin Marginean
 */
public class AliasWrappedBucketTest extends TestBase {

    @Test
    public void testAliasWrapped() throws Exception {
        ElasticSearchClient client = esClient();
        String indexBaseName = "aliaswrapped";
        AliasWrappedBucket bucket = esClient().admin().aliasWrappedBucket(indexBaseName, "mytype");
        Admin admin = client.admin();
        bucket.createIndex();
        String index1 = bucket.actualIndex();
        Assert.assertTrue(admin.indexExists(index1));
        Assert.assertTrue(admin.aliasExists(indexBaseName));
        Assert.assertFalse(admin.indexExists(indexBaseName));
        Collection<String> indexesForAlias = admin.indexesForAlias(indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), index1);

        Search search = client.search(bucket);
        String document = randomDoc();
        String newId = search.indexer().indexJson(document.toString());
        Map<String, Object> storedDoc = search.getMap(newId);
        assertSame(new JSONObject(document), storedDoc);
        Assert.assertFalse(admin.indexExists(indexBaseName));

        bucket.wipe(0);

        Assert.assertFalse(admin.indexExists(index1));
        Assert.assertTrue(admin.indexExists(bucket.actualIndex()));
        Assert.assertTrue(admin.aliasExists(indexBaseName));
        Assert.assertFalse(admin.indexExists(indexBaseName));

        indexesForAlias = admin.indexesForAlias(indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), bucket.actualIndex());
        Assert.assertNull(search.getMap(newId));
    }

    @Test
    public void testRefresh() throws Exception {
        ElasticSearchClient client = esClient();
        String indexBaseName = "aliaswrappedrefresh";
        AliasWrappedBucket bucket = esClient().admin().aliasWrappedBucket(indexBaseName, "mytype");
        Admin admin = client.admin();
        bucket.createIndex();

        Search search = client.search(bucket);
        indexStructuredDocs(267, search);
        waitForIndexedDocs(indexBaseName, 267);
        bucket.refresh(5);
        Thread.sleep(1000);
        Assert.assertEquals(267, search.count());
    }

    @Test
    public void testCustom() throws Exception {
        ElasticSearchClient client = esClient();
        String indexBaseName = "aliaswrappedcustom";
        String prefixyz = "prefixyz";
        String fullPrefix = prefixyz + "---";
        AliasWrappedBucket bucket = new CustomBucket(esClient().admin(), esClient().admin().aliasWrappedBucket(indexBaseName, "mytype"), prefixyz);
        Admin admin = client.admin();
        bucket.createIndex();
        String index1 = bucket.actualIndex();
        Assert.assertTrue(admin.indexExists(index1));
        Assert.assertTrue(admin.aliasExists(fullPrefix + indexBaseName));
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));
        Collection<String> indexesForAlias = admin.indexesForAlias(fullPrefix + indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), index1);

        Search search = client.search(bucket);
        String document = randomDoc();
        String newId = search.indexer().indexJson(document);
        Map<String, Object> storedDoc = search.getMap(newId);
        assertSame(new JSONObject(document), storedDoc);
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));

        bucket.wipe();

        Assert.assertFalse(admin.indexExists(index1));
        Assert.assertTrue(admin.indexExists(bucket.actualIndex()));
        Assert.assertTrue(admin.aliasExists(fullPrefix + indexBaseName));
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));

        indexesForAlias = admin.indexesForAlias(fullPrefix + indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), bucket.actualIndex());
        Assert.assertNull(search.getMap(newId));
    }

    @Test
    public void testDataMove() throws Exception {
        AliasWrappedBucket bucket = esClient().admin().aliasWrappedBucket("test-alias-move-to-new-wrapped-index", "notyourtype");
        bucket.createIndex();
        String index = bucket.actualIndex();
        String document = super.randomDoc();
        Search search = esClient().search(bucket);
        search.indexer().indexJson(document);
        waitForIndexedDocs(index, 1);
        Assert.assertEquals(1, search.count());

        DataBucket newBucket = bucket.createNewIndex();
        esClient().search(newBucket).indexer().indexJson(randomDoc());
        esClient().search(newBucket).indexer().indexJson(randomDoc());
        esClient().search(newBucket).indexer().indexJson(randomDoc());
        waitForIndexedDocs(newBucket.getIndex(), 3);
        AliasWrappedBucket newWrappedBucket = bucket.moveTo(newBucket, true);
        Assert.assertFalse(esClient().admin().indexExists(index));
        Assert.assertEquals(3, esClient().search(newWrappedBucket).count());
    }

    private static class CustomBucket extends AliasWrappedBucket {

        private String prefix;

        protected CustomBucket(Admin admin, AliasWrappedBucket bucket, String prefix) {
            super(admin, bucket.getIndex(), bucket.getType());
            this.prefix = prefix;
        }

        protected CustomBucket(Admin admin, String index, String type, String prefix) {
            super(admin, index, type);
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
