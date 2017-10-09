package com.wirktop.esutils.document;

/**
 * @author Cosmin Marginean
 */
public class TypedDocument<T> {

    private String id;
    private long version;
    private T source;

    public TypedDocument() {
    }

    public TypedDocument(String id, long version, T source) {
        this.id = id;
        this.version = version;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public T getSource() {
        return source;
    }

    public void setSource(T source) {
        this.source = source;
    }
}
