package com.wirktop.esutils.search;

import com.wirktop.esutils.DataBucket;
import com.wirktop.esutils.ElasticSearchClient;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Cosmin Marginean
 */
public class Scroll {

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
        ScrollIterator iterator = new ScrollIterator(esClient, request, null, true, pageSize, ScrollIterator.DEFAULT_KEEPALIVE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public Stream<SearchHit> scroll(QueryBuilder query) {
        return scroll(query, ScrollIterator.DEFAULT_PAGE_SIZE, true);
    }

    public Stream<SearchHit> scroll(QueryBuilder query, int pageSize, boolean fetchSource) {
        ScrollIterator iterator = new ScrollIterator(esClient, searchRequest(), query, fetchSource, pageSize, ScrollIterator.DEFAULT_KEEPALIVE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public SearchRequestBuilder searchRequest() {
        return esClient.getClient()
                .prepareSearch(bucket.getIndex())
                .setTypes(bucket.getType())
                .setVersion(true);
    }
}
