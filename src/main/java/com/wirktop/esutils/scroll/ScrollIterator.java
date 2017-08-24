package com.wirktop.esutils.scroll;

import com.wirktop.esutils.Search;
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

    public static final TimeValue DEFAULT_KEEPALIVE = TimeValue.timeValueMinutes(30);
    public static final int DEFAULT_PAGE_SIZE = 200;

    private Search search;
    private String scrollId;
    private SearchResponse currentResponse;
    private long currentIndex;
    private long totalHitCount;
    private int pageSize;

    public ScrollIterator(Search search, QueryBuilder query, boolean fetchSource, int pageSize, TimeValue keepAlive) {
        this.search = search;
        this.pageSize = pageSize;

        SearchRequestBuilder request = search.searchRequest();
        request.setScroll(keepAlive)
                .setSize(pageSize)
                .setFetchSource(fetchSource)
                .setQuery(query);
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
        return currentIndex < totalHitCount;
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
            currentResponse = search.client()
                    .prepareSearchScroll(scrollId)
                    .setScroll(DEFAULT_KEEPALIVE)
                    .execute()
                    .actionGet();
        }
        return hit;
    }
}
