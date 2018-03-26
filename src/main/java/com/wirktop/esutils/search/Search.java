package com.wirktop.esutils.search;

import com.wirktop.esutils.DataBucket;
import com.wirktop.esutils.Document;
import com.wirktop.esutils.ElasticSearchClient;
import com.wirktop.esutils.index.Indexer;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Cosmin Marginean
 */
public class Search {

    public static final Function<SearchHit, Document> HIT_TO_DOC = hit -> new Document(hit.getId(), hit.getVersion(), hit.getSourceAsString());

    private final ElasticSearchClient esClient;
    private final DataBucket bucket;
    private final Scroll scroll;

    public Search(ElasticSearchClient esClient, DataBucket bucket) {
        if (esClient == null) {
            throw new IllegalArgumentException("client argument cannot be null");
        }
        if (bucket == null) {
            throw new IllegalArgumentException("bucket argument cannot be null");
        }
        this.esClient = esClient;
        this.bucket = bucket;
        scroll = new Scroll(esClient, bucket);
    }

    public Map<String, Object> getMap(String id) {
        GetResponse response = get(id);
        return response.isExists()
                ? response.getSourceAsMap()
                : null;
    }

    public boolean exists(String id) {
        return get(id).isExists();
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

    public Document getDocument(String id) {
        GetResponse response = get(id);
        return response.isExists()
                ? new Document(id, response.getVersion(), response.getSourceAsString())
                : null;
    }

    private GetResponse get(String id) {
        GetRequestBuilder getRequest = esClient.getClient()
                .prepareGet(bucket.getIndex(), Indexer.DEFAULTTYPE, id);
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
                .setTypes(Indexer.DEFAULTTYPE)
                .setVersion(true);
    }

    public Stream<SearchHit> search(QueryBuilder query) {
        return search(query, SearchIterator.DEFAULT_PAGE_SIZE, null);
    }

    public Stream<SearchHit> search(QueryBuilder query, int pageSize) {
        return search(query, pageSize, null);
    }

    public Stream<SearchHit> search(QueryBuilder query, SortBuilder sort) {
        return search(query, SearchIterator.DEFAULT_PAGE_SIZE, sort);
    }

    public Stream<SearchHit> search(QueryBuilder query, int pageSize, SortBuilder sort) {
        SearchIterator iterator = new SearchIterator(this, query, pageSize, sort);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public <T> Function<SearchHit, T> hitToPojo(Class<T> pojoClass) {
        return (hit) -> esClient.json().toPojo(hit.getSourceAsString(), pojoClass);
    }

    public SearchResponse search(SearchRequestBuilder request) {
        return request.execute().actionGet();
    }

    public DataBucket bucket() {
        return bucket;
    }

    public ElasticSearchClient esClient() {
        return esClient;
    }

    public Scroll scroll() {
        return scroll;
    }

    public Indexer indexer() {
        return new Indexer(esClient, bucket);
    }

    public boolean indexExists() {
        return esClient.admin().indexExists(bucket.getIndex());
    }

    public ExplainResponse explain(String id, QueryBuilder query) {
        return esClient.getClient()
                .prepareExplain(bucket.getIndex(), Indexer.DEFAULTTYPE, id)
                .setQuery(query)
                .get();
    }
}
