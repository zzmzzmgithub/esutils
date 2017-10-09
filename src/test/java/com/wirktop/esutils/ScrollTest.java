package com.wirktop.esutils;

import com.wirktop.esutils.index.IndexBatch;
import com.wirktop.esutils.index.Indexer;
import com.wirktop.esutils.search.Search;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Cosmin Marginean
 */
public class ScrollTest extends TestBase {

    @Test
    public void testScrollHits() throws Exception {
        String index = "test-scroll-hits";
        Indexer indexer = indexer(index, "type1");
        Search search = search(index, "type1");
        int docCount = 267;
        try (IndexBatch batch = indexer.batch()) {
            generateDocuments(docCount, false)
                    .forEach((hit) -> batch.add(UUID.randomUUID().toString(), hit.toString()));
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.scroll().scroll(QueryBuilders.matchAllQuery())
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }

    @Test
    public void testScrollPojo() throws Exception {
        String index = "test-scroll-pojo";
        Indexer indexer = indexer(index, "type1");
        Search search = search(index, "type1");
        int docCount = 194;
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, docCount);
        List<Person> docs = search.scroll().scroll(QueryBuilders.matchAllQuery(), Person.class)
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }

    @Test
    public void testScrollDocs() throws Exception {
        String index = "test-scroll-docs";
        Indexer indexer = indexer(index, "type1");
        Search search = search(index, "type1");
        int docCount = 194;
        indexStructuredDocs(docCount, indexer);
        waitForIndexedDocs(index, docCount);
        List<Document> docs = search.scroll().scrollDocs(QueryBuilders.matchAllQuery())
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
        for (Document doc : docs) {
            Assert.assertTrue(doc.getVersion() > 0);
        }
    }

    @Test
    public void testScrollNoFilter() throws Exception {
        String index = "test-scroll-nofilter";
        Indexer indexer = indexer(index, "type1");
        Search search = search(index, "type1");
        int docCount = 215;
        try (IndexBatch batch = indexer.batch()) {
            generateDocuments(docCount, false)
                    .forEach((hit) -> batch.add(UUID.randomUUID().toString(), hit));
        }
        waitForIndexedDocs(index, docCount);
        List<SearchHit> docs = search.scroll().scroll()
                .collect(Collectors.toList());
        Assert.assertEquals(docs.size(), docCount);
    }


}
