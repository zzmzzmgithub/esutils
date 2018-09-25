package com.wirktop.esutils.index;

import com.wirktop.esutils.*;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author Cosmin Marginean
 */
public class Indexer {

    private static final int DEFAULT_BATCH_SIZE = 100;
    public static final String DEFAULTTYPE = "defaulttype";

    private final ElasticSearchClient esClient;
    private final DataBucket bucket;

    public Indexer(ElasticSearchClient esClient, DataBucket bucket) {
        this.esClient = esClient;
        this.bucket = bucket;
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

    public void updateScriptRetry(String id, String painlessScript, Map<String, Object> params, int retryCount) {
        UpdateRequest request = updateRequest(id)
                .script(new Script(ScriptType.INLINE, "painless", painlessScript, params));
        if (retryCount > 0) {
            request = request.retryOnConflict(retryCount);
        }
        update(request);
    }

    public void updateScript(String id, String painlessScript, Map<String, Object> params) {
        updateScript(id, painlessScript, params, 0L);
    }

    public void updateScript(String id, String painlessScript, Map<String, Object> params, long version) {
        UpdateRequest request = updateRequest(id)
                .script(new Script(ScriptType.INLINE, "painless", painlessScript, params));
        if (version > 0) {
            request = request.version(version);
        }
        update(request);
    }


    public void updateDoc(String id, String docJsonStr, long version) {
        UpdateRequest request = updateRequest(id)
                .doc(docJsonStr, XContentType.JSON);
        if (version > 0) {
            request = request.version(version);
        }
        update(request);
    }

    public void updateField(String id, String field, Object value) {
        updateField(id, field, value, 0);
    }

    public void updateField(String id, String field, Object value, long version) {
        UpdateRequest request = updateRequest(id)
                .doc(Collections.singletonMap(field, value));
        if (version > 0) {
            request = request.version(version);
        }
        update(request);
    }

    public UpdateRequest updateRequest(String id) {
        return new UpdateRequest(bucket.getIndex(), DEFAULTTYPE, id);
    }

    public void update(UpdateRequest request) {
        esClient.getClient().update(request).actionGet();
    }

    public long delete(QueryBuilder filter) {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(esClient.getClient())
                .filter(filter)
                .source(bucket.getIndex())
                .get();
        return response.getDeleted();
    }

    public void delete(String id) {
        delete(id, false);
    }

    public void delete(String id, boolean refresh) {
        DeleteRequestBuilder request = esClient.getClient().prepareDelete(bucket.getIndex(), DEFAULTTYPE, id);
        if (refresh) {
            request = request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        }
        request.execute().actionGet();
    }

    public String indexDocument(Document document) {
        return indexDocument(document, false);
    }

    public String indexDocument(Document document, boolean waitRefresh) {
        IndexRequestBuilder request = indexRequest(document.getId(), waitRefresh);
        if (document.getVersion() > 0) {
            request.setVersion(document.getVersion());
        }
        request = request.setSource(document.getSource(), XContentType.JSON);
        IndexResponse response = request.execute().actionGet();
        return response.getId();
    }

    public void bulkIndex(Collection<Document> documents) {
        if (documents != null) {
            BulkRequestBuilder request = esClient.getClient().prepareBulk();
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
            request = request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        }
        return request;
    }

    public void bulkIndexJsonStr(Collection<String> documents) {
        if (documents != null) {
            BulkRequestBuilder request = esClient.getClient().prepareBulk();
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
        return esClient.getClient().prepareIndex(bucket.getIndex(), DEFAULTTYPE);
    }

    private Json json() {
        return esClient.json();
    }

    public DataBucket bucket() {
        return bucket;
    }
}
