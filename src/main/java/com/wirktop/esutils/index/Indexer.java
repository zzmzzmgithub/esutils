package com.wirktop.esutils.index;

import com.wirktop.esutils.DataBucket;
import com.wirktop.esutils.SearchException;
import com.wirktop.esutils.search.Search;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Cosmin Marginean
 */
public class Indexer {

    public static final int DEFAULT_BATCH_SIZE = 100;
    private static final Logger log = LoggerFactory.getLogger(Indexer.class);
    private Search search;

    public Indexer(Search search) {
        this.search = search;
    }

    public String index(String id, Map<String, Object> document) {
        return index(id, document, false);
    }

    public String index(Object pojo) {
        return index(null, pojo, false);
    }

    public String index(String id, Object pojo) {
        return index(id, pojo, false);
    }

    public String index(String id, Object pojo, boolean refresh) {
        try {
            StringWriter writer = new StringWriter();
            search.getObjectMapper().writer().writeValue(writer, pojo);
            return index(id, writer.toString(), refresh);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    public String index(Map<String, Object> document) {
        return index(null, document, false);
    }

    public String index(String id, Map<String, Object> document, boolean waitRefresh) {
        IndexRequestBuilder request = indexRequest(id, waitRefresh);
        request = request.setSource(document);
        IndexResponse response = request.execute().actionGet();
        return response.getId();
    }

    public String index(JSONObject document) {
        return index(null, document, false);
    }

    public String index(String id, JSONObject document) {
        return index(id, document, false);
    }

    public String index(String id, JSONObject document, boolean waitRefresh) {
        return index(id, document.toString(), waitRefresh);
    }

    public String index(String source) {
        return index(null, source, false);
    }

    public String index(String id, String source) {
        return index(id, source, false);
    }

    public String index(String id, String source, boolean waitRefresh) {
        IndexRequestBuilder request = indexRequest(id, waitRefresh);
        request = request.setSource(source, XContentType.JSON);
        IndexResponse response = request.execute().actionGet();
        return response.getId();
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

    public void bulkIndexJson(Collection<JSONObject> documents) {
        bulkIndexJson(documents, null);
    }

    public void bulkIndexJson(Collection<JSONObject> documents, String idField) {
        List<Map<String, Object>> mapDocs = documents.stream()
                .map((json) -> json.toMap())
                .collect(Collectors.toList());
        bulkIndex(mapDocs, idField);
    }

    public void bulkIndex(Collection<Map<String, Object>> documents) {
        bulkIndex(documents, null);
    }

    public void bulkIndex(Collection<Map<String, Object>> documents, String idField) {
        if (documents != null) {
            BulkRequestBuilder request = search.client().prepareBulk();
            for (Map<String, Object> document : documents) {
                IndexRequestBuilder indexRequest = prepareIndex();
                if (idField != null && document.containsKey(idField) && document.get(idField) instanceof String) {
                    indexRequest.setId((String) document.get(idField));
                }
                indexRequest = indexRequest.setSource(document);
                request.add(indexRequest);
            }
            doBulkIndex(request);
        }
    }

    public void bulkIndexJsonStr(Collection<String> documents) {
        if (documents != null) {
            BulkRequestBuilder request = search.client().prepareBulk();
            for (String document : documents) {
                IndexRequestBuilder indexRequest = prepareIndex();
                indexRequest = indexRequest.setSource(document, XContentType.JSON);
                request.add(indexRequest);
            }
            doBulkIndex(request);
        }
    }

    public IndexBatch batch() {
        return new IndexBatch(this, DEFAULT_BATCH_SIZE, null);
    }

    public IndexBatch batch(int size) {
        return new IndexBatch(this, size, null);
    }

    public IndexBatch batch(int size, String idField) {
        return new IndexBatch(this, size, idField);
    }

    private void doBulkIndex(BulkRequestBuilder request) {
        BulkResponse response = request.execute().actionGet();
        if (response.hasFailures()) {
            throw new SearchException("Could not index all documents. Error message is: " + response.buildFailureMessage());
        }
    }

    private IndexRequestBuilder prepareIndex() {
        return search.client().prepareIndex(search.bucket().getIndex(), search.bucket().getType());
    }

    public Search search() {
        return search;
    }

    public DataBucket bucket() {
        return search.bucket();
    }
}
