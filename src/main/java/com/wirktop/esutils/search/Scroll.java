package com.wirktop.esutils.search;

import com.wirktop.esutils.DataBucket;
import com.wirktop.esutils.ElasticSearchClient;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Cosmin Marginean
 */
public class Scroll {

    public static final TimeValue DEFAULT_KEEPALIVE = TimeValue.timeValueMinutes(10);
    public static final int DEFAULT_PAGE_SIZE = 100;

    private final ElasticSearchClient esClient;
    private final DataBucket bucket;

    public Scroll(ElasticSearchClient esClient, DataBucket bucket) {
        if (esClient == null) {
            throw new IllegalArgumentException("client argument cannot be null");
        }
        if (bucket == null) {
            throw new IllegalArgumentException("bucket argument cannot be null");
        }
        this.esClient = esClient;
        this.bucket = bucket;
    }

    public static Stream<SearchHit> scrollIndex(ElasticSearchClient esClient, String index) {
        return scrollIndex(esClient, index, SearchIterator.DEFAULT_PAGE_SIZE);
    }

    public static Stream<SearchHit> scrollIndex(ElasticSearchClient esClient, String index, int pageSize) {
        SearchRequestBuilder request = esClient.getClient().prepareSearch(index);
        ScrollIterator iterator = new ScrollIterator(esClient, request, null, true, pageSize, DEFAULT_KEEPALIVE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public Stream<SearchHit> scroll(QueryBuilder query) {
        return scroll(query, DEFAULT_PAGE_SIZE, true, DEFAULT_KEEPALIVE);
    }

    public Stream<SearchHit> scroll(QueryBuilder query, int pageSize, boolean fetchSource, TimeValue keepAlive) {
        ScrollIterator iterator = new ScrollIterator(esClient, searchRequest(), query, fetchSource, pageSize, keepAlive);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public SearchRequestBuilder searchRequest() {
        return esClient.getClient()
                .prepareSearch(bucket.getIndex())
                .setVersion(true);
    }
}
