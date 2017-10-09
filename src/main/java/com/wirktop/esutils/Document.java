package com.wirktop.esutils;

/**
 * @author Cosmin Marginean
 */
public class Document {

    private String id;
    private long version;
    private String source;

    public Document(String id, String source) {
        this.id = id;
        this.source = source;
    }

    public Document(String id, long version, String source) {
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

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
