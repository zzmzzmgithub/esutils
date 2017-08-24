package com.wirktop.esutils.search;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;

/**
 * @author Cosmin Marginean
 */
public class SearchIterator implements Iterator<SearchHit> {

    public static final int DEFAULT_PAGE_SIZE = 10;

    private Search search;
    private QueryBuilder query;
    private SearchResponse currentResponse;
    private int currentIndex;
    private long totalHitCount;
    private int pageSize;

    protected SearchIterator(Search search, QueryBuilder query, int pageSize) {
        this.search = search;
        this.query = query;
        this.pageSize = pageSize;

        currentIndex = 0;
        doRequest();
        totalHitCount = currentResponse.getHits().getTotalHits();
        if (totalHitCount > 0) {
            currentIndex = 0;
        }
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
            doRequest();
        }
        return hit;
    }

    private void doRequest() {
        SearchRequestBuilder request = search.searchRequest();
        request.setQuery(query);
        request.setFrom(currentIndex);
        request.setSize(pageSize);
        currentResponse = request.execute().actionGet();
    }

    @Override
    public boolean hasNext() {
        return currentIndex < totalHitCount;
    }
}
