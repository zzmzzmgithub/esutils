package com.wirktop.esutils;

import com.wirktop.esutils.index.Indexer;
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
        AliasWrappedBucket bucket = new AliasWrappedBucket(indexBaseName, "mytype");
        Admin admin = client.admin();
        bucket.createIndex(admin);
        String index1 = bucket.actualIndex(admin);
        Assert.assertTrue(admin.indexExists(index1));
        Assert.assertTrue(admin.aliasExists(indexBaseName));
        Assert.assertFalse(admin.indexExists(indexBaseName));
        Collection<String> indexesForAlias = admin.indexesForAlias(indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), index1);

        Search search = client.search(bucket);
        Indexer indexer = client.indexer(bucket);
        String document = randomDoc();
        String newId = indexer.indexJson(document.toString());
        Map<String, Object> storedDoc = search.getMap(newId);
        assertSame(new JSONObject(document), storedDoc);
        Assert.assertFalse(admin.indexExists(indexBaseName));

        bucket.wipe(admin, 0);

        Assert.assertFalse(admin.indexExists(index1));
        Assert.assertTrue(admin.indexExists(bucket.actualIndex(admin)));
        Assert.assertTrue(admin.aliasExists(indexBaseName));
        Assert.assertFalse(admin.indexExists(indexBaseName));

        indexesForAlias = admin.indexesForAlias(indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), bucket.actualIndex(admin));
        Assert.assertNull(search.getMap(newId));
    }

    @Test
    public void testRefresh() throws Exception {
        ElasticSearchClient client = esClient();
        String indexBaseName = "aliaswrappedrefresh";
        AliasWrappedBucket bucket = new AliasWrappedBucket(indexBaseName, "mytype");
        Admin admin = client.admin();
        bucket.createIndex(admin);

        Search search = client.search(bucket);
        Indexer indexer = client.indexer(bucket);
        indexStructuredDocs(267, indexer);
        waitForIndexedDocs(indexBaseName, 267);
        bucket.refresh(admin);
        Thread.sleep(1000);
        Assert.assertEquals(267, search.count());
    }

    @Test
    public void testCustom() throws Exception {
        ElasticSearchClient client = esClient();
        String indexBaseName = "aliaswrappedcustom";
        String prefixyz = "prefixyz";
        String fullPrefix = prefixyz + "---";
        AliasWrappedBucket bucket = new CustomBucket(esClient().admin(), new AliasWrappedBucket(indexBaseName, "mytype"), prefixyz);
        Admin admin = client.admin();
        bucket.createIndex(admin);
        String index1 = bucket.actualIndex(admin);
        Assert.assertTrue(admin.indexExists(index1));
        Assert.assertTrue(admin.aliasExists(fullPrefix + indexBaseName));
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));
        Collection<String> indexesForAlias = admin.indexesForAlias(fullPrefix + indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), index1);

        Search search = client.search(bucket);
        Indexer indexer = client.indexer(bucket);
        String document = randomDoc();
        String newId = indexer.indexJson(document);
        Map<String, Object> storedDoc = search.getMap(newId);
        assertSame(new JSONObject(document), storedDoc);
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));

        bucket.wipe(admin);

        Assert.assertFalse(admin.indexExists(index1));
        Assert.assertTrue(admin.indexExists(bucket.actualIndex(admin)));
        Assert.assertTrue(admin.aliasExists(fullPrefix + indexBaseName));
        Assert.assertFalse(admin.indexExists(fullPrefix + indexBaseName));

        indexesForAlias = admin.indexesForAlias(fullPrefix + indexBaseName);
        Assert.assertEquals(indexesForAlias.size(), 1);
        Assert.assertEquals(indexesForAlias.iterator().next(), bucket.actualIndex(admin));
        Assert.assertNull(search.getMap(newId));
    }

    @Test
    public void testDataMove() throws Exception {
        AliasWrappedBucket bucket = new AliasWrappedBucket("test-alias-move-to-new-wrapped-index", "notyourtype");
        Admin admin = esClient().admin();
        bucket.createIndex(admin);
        String index = bucket.actualIndex(admin);
        String document = super.randomDoc();
        Search search = esClient().search(bucket);
        Indexer indexer = esClient().indexer(bucket);
        indexer.indexJson(document);
        waitForIndexedDocs(index, 1);
        Assert.assertEquals(1, search.count());

        DataBucket newBucket = bucket.createNewIndex(admin);
        Indexer newIndexer = esClient().indexer(newBucket);
        newIndexer.indexJson(randomDoc());
        newIndexer.indexJson(randomDoc());
        newIndexer.indexJson(randomDoc());
        waitForIndexedDocs(newBucket.getIndex(), 3);
        AliasWrappedBucket newWrappedBucket = bucket.wrap(admin, newBucket, true);
        Assert.assertFalse(esClient().admin().indexExists(index));
        Assert.assertEquals(3, esClient().search(newWrappedBucket).count());
    }

    private static class CustomBucket extends AliasWrappedBucket {

        private String prefix;

        protected CustomBucket(Admin admin, AliasWrappedBucket bucket, String prefix) {
            super(bucket.getIndex(), bucket.getType());
            this.prefix = prefix;
        }

        protected CustomBucket(Admin admin, String index, String type, String prefix) {
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
