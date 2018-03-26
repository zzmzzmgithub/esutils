package com.wirktop.esutils.search;

import com.wirktop.esutils.ElasticSearchClient;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;

/**
 * @author Cosmin Marginean
 */
public class ScrollIterator implements Iterator<SearchHit> {

    protected static final TimeValue DEFAULT_KEEPALIVE = TimeValue.timeValueMinutes(30);
    protected static final int DEFAULT_PAGE_SIZE = 200;

    private ElasticSearchClient esClient;
    private String scrollId;
    private SearchResponse currentResponse;
    private long currentIndex;
    private long totalHitCount;
    private int pageSize;

    public ScrollIterator(ElasticSearchClient esClient, SearchRequestBuilder request, QueryBuilder query, boolean fetchSource, int pageSize, TimeValue keepAlive) {
        this.esClient = esClient;
        this.pageSize = pageSize > 0 ? pageSize : DEFAULT_PAGE_SIZE;

        request.setScroll(keepAlive)
                .setSize(pageSize)
                .setFetchSource(fetchSource);
        if (query != null) {
            request.setQuery(query);
        }
        SearchResponse response = request.execute().actionGet();
        totalHitCount = response.getHits().getTotalHits();
        if (totalHitCount > 0) {
            currentIndex = 0;
            scrollId = response.getScrollId();
            currentResponse = response;
        }
    }

    @Override
    public boolean hasNext() {
        return totalHitCount > 0 && currentIndex < totalHitCount;
    }

    @Override
    public SearchHit next() {
        if (!hasNext()) {
            throw new IllegalStateException("No next element");
        }
        long arrayIndex = currentIndex % pageSize;
        SearchHit hit = currentResponse.getHits().getHits()[(int) arrayIndex];
        currentIndex++;
        if (currentIndex % pageSize == 0) {
            currentResponse = esClient.getClient().prepareSearchScroll(scrollId)
                    .setScroll(DEFAULT_KEEPALIVE)
                    .execute()
                    .actionGet();
            totalHitCount = currentResponse.getHits().getTotalHits();
        }
        return hit;
    }
}
