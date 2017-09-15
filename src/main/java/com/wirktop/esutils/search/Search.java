package com.wirktop.esutils.search;

import com.wirktop.esutils.DataBucket;
import com.wirktop.esutils.ElasticSearchClient;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Cosmin Marginean
 */
public class Search {

    private ElasticSearchClient esClient;
    private DataBucket bucket;

    public Search(ElasticSearchClient esClient, DataBucket bucket) {
        if (esClient == null) {
            throw new IllegalArgumentException("client argument cannot be null");
        }
        if (bucket == null) {
            throw new IllegalArgumentException("bucket argument cannot be null");
        }
        this.esClient = esClient;
        this.bucket = bucket;
    }

    public Map<String, Object> getMap(String id) {
        GetResponse response = get(id);
        return response.isExists()
                ? response.getSourceAsMap()
                : null;
    }

    public String getJson(String id) {
        GetResponse response = get(id);
        return response.isExists()
                ? response.getSourceAsString()
                : null;
    }

    public <T> T get(String id, Class<T> docClass) {
        GetResponse response = get(id);
        return response.isExists()
                ? esClient.json().toPojo(response.getSourceAsString(), docClass)
                : null;
    }

    private GetResponse get(String id) {
        GetRequestBuilder getRequest = esClient.getClient()
                .prepareGet(bucket.getIndex(), bucket.getType(), id);
        return getRequest.execute().actionGet();
    }

    public void search(QueryBuilder query, OutputStream outputStream) {
        search(searchRequest().setQuery(query), outputStream);
    }

    public void search(SearchRequestBuilder request, OutputStream outputStream) {
        SearchResponse response = request.execute().actionGet();
        PrintStream printStream = new PrintStream(outputStream);
        printStream.print(response.toString());
        printStream.flush();
    }

    public long count() {
        return count(null);
    }

    public long count(QueryBuilder filter) {
        SearchRequestBuilder builder = searchRequest().setSize(0);
        if (filter != null) {
            builder = builder.setPostFilter(filter);
        }
        SearchResponse response = builder.execute().actionGet();
        return response.getHits().getTotalHits();
    }

    public SearchRequestBuilder searchRequest() {
        return esClient.getClient()
                .prepareSearch(bucket.getIndex())
                .setTypes(bucket.getType());
    }

    public <T> Stream<T> scroll(Class<T> pojoClass) {
        return scroll(null, pojoClass);
    }

    public Stream<SearchHit> scroll() {
        return scroll((QueryBuilder) null);
    }

    public <T> Stream<T> scroll(QueryBuilder query, Class<T> pojoClass) {
        return scroll(query)
                .map((hit) -> esClient.json().toPojo(hit.getSourceAsString(), pojoClass));
    }

    public Stream<SearchHit> scroll(QueryBuilder query) {
        ScrollIterator iterator = new ScrollIterator(esClient, searchRequest(), query, true, ScrollIterator.DEFAULT_PAGE_SIZE, ScrollIterator.DEFAULT_KEEPALIVE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public static Stream<SearchHit> scrollIndex(ElasticSearchClient esClient, String index) {
        SearchRequestBuilder request = esClient.getClient().prepareSearch(index);
        ScrollIterator iterator = new ScrollIterator(esClient, request, null, true, ScrollIterator.DEFAULT_PAGE_SIZE, ScrollIterator.DEFAULT_KEEPALIVE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public <T> Stream<T> search(Class<T> pojoClass) {
        return search(null, pojoClass);
    }

    public <T> Stream<T> search(QueryBuilder query, Class<T> pojoClass) {
        return search(query)
                .map((hit) -> esClient.json().toPojo(hit.getSourceAsString(), pojoClass));
    }

    public Stream<SearchHit> search(QueryBuilder query) {
        return search(query, SearchIterator.DEFAULT_PAGE_SIZE);
    }

    public Stream<SearchHit> search(QueryBuilder query, int pageSize) {
        SearchIterator iterator = new SearchIterator(this, query, pageSize);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public DataBucket bucket() {
        return bucket;
    }

    public ElasticSearchClient esClient() {
        return esClient;
    }
}
