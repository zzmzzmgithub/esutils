package com.wirktop.esutils.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wirktop.esutils.DataBucket;
import com.wirktop.esutils.SearchException;
import com.wirktop.esutils.index.Indexer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private static final Logger log = LoggerFactory.getLogger(Search.class);

    private Client client;
    private DataBucket bucket;
    private ObjectMapper objectMapper = new ObjectMapper();
    private Indexer indexer;

    public Search(Client client, DataBucket bucket) {
        if (client == null) {
            throw new IllegalArgumentException("client argument cannot be null");
        }
        if (bucket == null) {
            throw new IllegalArgumentException("location argument cannot be null");
        }
        this.client = client;
        this.bucket = bucket;
        this.indexer = new Indexer(this);
    }

    public JSONObject getJson(String id) {
        GetResponse response = get(id);
        if (response.isExists()) {
            return new JSONObject(response.getSourceAsString());
        }
        return null;
    }

    public Map<String, Object> getMap(String id) {
        GetResponse response = get(id);
        if (response.isExists()) {
            return response.getSourceAsMap();
        }
        return null;
    }

    public String getStr(String id) {
        GetResponse response = get(id);
        if (response.isExists()) {
            return response.getSourceAsString();
        }
        return null;
    }

    public <T> T get(String id, Class<T> docClass) {
        try {
            GetResponse response = get(id);
            if (response.isExists()) {
                return objectMapper.readValue(response.getSourceAsString(), docClass);
            }
            return null;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    private GetResponse get(String id) {
        GetRequestBuilder getRequest = client.prepareGet(bucket.getIndex(), bucket.getType(), id);
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
        try {
            SearchRequestBuilder builder = searchRequest().setSize(0);
            if (filter != null) {
                builder = builder.setPostFilter(filter);
            }
            SearchResponse response = builder.execute().actionGet();
            return response.getHits().getTotalHits();
        } catch (ElasticsearchException e) {
            log.error(e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    public Indexer indexer() {
        return indexer;
    }

    public Client client() {
        return client;
    }

    protected SearchRequestBuilder searchRequest() {
        return client.prepareSearch(bucket.getIndex())
                .setTypes(bucket.getType());
    }

    public Stream<SearchHit> scroll(QueryBuilder query) {
        ScrollIterator iterator = new ScrollIterator(this, query, true, ScrollIterator.DEFAULT_PAGE_SIZE, ScrollIterator.DEFAULT_KEEPALIVE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public Stream<SearchHit> search(QueryBuilder query) {
        return search(query, SearchIterator.DEFAULT_PAGE_SIZE);
    }

    public Stream<SearchHit> search(QueryBuilder query, int pageSize) {
        SearchIterator iterator = new SearchIterator(this, query, pageSize);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public DataBucket bucket() {
        return bucket;
    }
}
