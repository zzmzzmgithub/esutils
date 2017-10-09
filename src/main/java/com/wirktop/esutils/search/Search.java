package com.wirktop.esutils.search;

import com.wirktop.esutils.DataBucket;
import com.wirktop.esutils.Document;
import com.wirktop.esutils.ElasticSearchClient;
import com.wirktop.esutils.index.Indexer;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

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

    protected static final Function<SearchHit, Document> HIT_TO_DOCUMENT = (hit) -> new Document(hit.getId(), hit.getVersion(), hit.getSourceAsString());
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

    public Document getDocument(String id) {
        GetResponse response = get(id);
        return response.isExists()
                ? new Document(id, response.getVersion(), response.getSourceAsString())
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
                .setTypes(bucket.getType())
                .setVersion(true);
    }

    public <T> Stream<T> search(Class<T> pojoClass) {
        return search(null, pojoClass, SearchIterator.DEFAULT_PAGE_SIZE);
    }

    public <T> Stream<T> search(Class<T> pojoClass, int pageSize) {
        return search(null, pojoClass, pageSize);
    }

    public <T> Stream<T> search(QueryBuilder query, Class<T> pojoClass) {
        return search(query, pojoClass, SearchIterator.DEFAULT_PAGE_SIZE);
    }

    public <T> Stream<T> search(QueryBuilder query, Class<T> pojoClass, int pageSize) {
        return search(query, pageSize)
                .map((hit) -> esClient.json().toPojo(hit.getSourceAsString(), pojoClass));
    }

    public Stream<Document> searchDocs(QueryBuilder query) {
        return searchDocs(query, SearchIterator.DEFAULT_PAGE_SIZE);
    }

    public Stream<Document> searchDocs(QueryBuilder query, int pageSize) {
        return search(query, pageSize).map(HIT_TO_DOCUMENT);
    }

    public Stream<SearchHit> search(QueryBuilder query) {
        return search(query, SearchIterator.DEFAULT_PAGE_SIZE);
    }

    public Stream<SearchHit> search(QueryBuilder query, int pageSize) {
        SearchIterator iterator = new SearchIterator(this, query, pageSize);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public SearchResponse search(SearchRequestBuilder request) {
        return request.execute().actionGet();
    }

    public void suggest(String suggestField, String text, int size, OutputStream outputStream) {
        suggest(suggestField, text, size, outputStream, null, null);
    }

    public void suggest(String suggestField, String text, int size, OutputStream outputStream, String contextName, String contextValue) {
        CompletionSuggestionBuilder completion = new CompletionSuggestionBuilder(suggestField)
                .prefix(text);
        if (size > 0) {
            completion.size(size);
        }
        if (contextName != null) {
            completion.contexts(new CompletionSuggestionBuilder.Contexts2x().addCategory(contextName, contextValue));
        }

        SuggestBuilder suggestions = new SuggestBuilder()
                .addSuggestion("suggestions", completion);
        SearchRequestBuilder request = esClient.getClient().prepareSearch(bucket.getIndex())
                .suggest(suggestions);
        search(request, outputStream);
    }

    public DataBucket bucket() {
        return bucket;
    }

    public ElasticSearchClient esClient() {
        return esClient;
    }

    public Scroll scroll() {
        return new Scroll(esClient, bucket);
    }

    public Indexer indexer() {
        return new Indexer(esClient, bucket);
    }

    public boolean indexExists() {
        return esClient.admin().indexExists(bucket.getIndex());
    }
}
