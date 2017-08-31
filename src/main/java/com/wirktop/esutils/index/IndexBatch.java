package com.wirktop.esutils.index;

import com.wirktop.esutils.Document;
import com.wirktop.esutils.Json;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Cosmin Marginean
 */
public class IndexBatch implements AutoCloseable {

    private Indexer indexer;
    private Json json;
    private List<Document> documents = new ArrayList<>();
    private int size;

    protected IndexBatch(Indexer indexer, Json json, int size) {
        this.indexer = indexer;
        this.json = json;
        this.size = size;
    }

    public void addPojo(String id, Object pojo) {
        add(id, json.toString(pojo));
    }

    public void add(String id, String documentJson) {
        documents.add(new Document(id, documentJson));
        if (documents.size() == size) {
            bulkIndex();
        }
    }

    @Override
    public void close() {
        if (documents.size() > 0) {
            bulkIndex();
        }
    }

    private void bulkIndex() {
        indexer.bulkIndex(documents);
        documents.clear();
    }
}
