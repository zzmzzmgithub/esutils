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

    public Stream<SearchHit> search(QueryBuilder query) {
        return search(query, SearchIterator.DEFAULT_PAGE_SIZE);
    }

    public <T> Function<SearchHit, T> hitToPojo(Class<T> pojoClass) {
        return (hit) -> esClient.json().toPojo(hit.getSourceAsString(), pojoClass);
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
        return scroll;
    }

    public Indexer indexer() {
        return new Indexer(esClient, bucket);
    }

    public boolean indexExists() {
        return esClient.admin().indexExists(bucket.getIndex());
    }
}
