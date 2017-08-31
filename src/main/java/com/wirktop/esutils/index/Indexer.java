package com.wirktop.esutils.index;

import com.wirktop.esutils.DataBucket;
import com.wirktop.esutils.Document;
import com.wirktop.esutils.Json;
import com.wirktop.esutils.SearchException;
import com.wirktop.esutils.search.Search;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Collection;

/**
 * @author Cosmin Marginean
 */
public class Indexer {

    private static final int DEFAULT_BATCH_SIZE = 100;
    private Search search;

    public Indexer(Search search) {
        this.search = search;
    }

    public String indexObject(Object doc) {
        return indexObject(null, doc, false);
    }

    public String indexObject(String id, Object doc) {
        return indexObject(id, doc, false);
    }

    public String indexObject(String id, Object doc, boolean refresh) {
        return indexJson(id, json().toString(doc), refresh);
    }

    public String indexJson(String jsonDocument) {
        return indexJson(null, jsonDocument, false);
    }

    public String indexJson(String id, String jsonDocument) {
        return indexJson(id, jsonDocument, false);
    }

    public String indexJson(String id, String jsonDocument, boolean refresh) {
        return indexDocument(new Document(id, jsonDocument), refresh);
    }

    public String indexDocument(Document document) {
        return indexDocument(document, false);
    }

    public String indexDocument(Document document, boolean waitRefresh) {
        IndexRequestBuilder request = indexRequest(document.getId(), waitRefresh);
        request = request.setSource(document.getSource(), XContentType.JSON);
        IndexResponse response = request.execute().actionGet();
        return response.getId();
    }

    public void bulkIndex(Collection<Document> documents) {
        if (documents != null) {
            BulkRequestBuilder request = search.esClient().getClient().prepareBulk();
            for (Document document : documents) {
                IndexRequestBuilder indexRequest = prepareIndex();
                if (document.getId() != null) {
                    indexRequest.setId(document.getId());
                }
                indexRequest = indexRequest.setSource(document.getSource(), XContentType.JSON);
                request.add(indexRequest);
            }
            doBulkIndex(request);
        }
    }

    private IndexRequestBuilder indexRequest(String id, boolean waitRefresh) {
        IndexRequestBuilder request = prepareIndex();
        if (id != null) {
            request = request.setId(id);
        }
        if (waitRefresh) {
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        }
        return request;
    }

    public void bulkIndexJsonStr(Collection<String> documents) {
        if (documents != null) {
            BulkRequestBuilder request = search.esClient().getClient().prepareBulk();
            for (String document : documents) {
                IndexRequestBuilder indexRequest = prepareIndex();
                indexRequest = indexRequest.setSource(document, XContentType.JSON);
                request.add(indexRequest);
            }
            doBulkIndex(request);
        }
    }

    public IndexBatch batch() {
        return batch(DEFAULT_BATCH_SIZE);
    }

    public IndexBatch batch(int size) {
        return new IndexBatch(this, json(), size);
    }

    private void doBulkIndex(BulkRequestBuilder request) {
        BulkResponse response = request.execute().actionGet();
        if (response.hasFailures()) {
            throw new SearchException("Could not index all documents. Error message is: " + response.buildFailureMessage());
        }
    }

    private IndexRequestBuilder prepareIndex() {
        return search.esClient().getClient().prepareIndex(search.bucket().getIndex(), search.bucket().getType());
    }

    private Json json() {
        return search.esClient().json();
    }

    public DataBucket bucket() {
        return search.bucket();
    }
}
