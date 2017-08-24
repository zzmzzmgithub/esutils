package com.worktop.esutils.index;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Cosmin Marginean
 */
public class IndexBatch implements AutoCloseable {

    private int size;
    protected Indexer indexer;
    protected String idField;
    protected List<Map<String, Object>> documents = new ArrayList<>();

    public IndexBatch(Indexer indexer, int size, String idField) {
        this.indexer = indexer;
        this.size = size;
        this.idField = idField;
    }

    public void add(JSONObject document) {
        add(document.toMap());
    }

    public void add(String jsonDocStr) {
        add(new JSONObject(jsonDocStr).toMap());
    }

    public void add(Map<String, Object> document) {
        documents.add(document);
        if (documents.size() == size) {
            bulkIndex();
        }
    }

    @Override
    public void close() throws Exception {
        if (documents.size() > 0) {
            bulkIndex();
        }
    }

    private void bulkIndex() {
        indexer.bulkIndex(documents, idField);
        documents.clear();
    }
}
