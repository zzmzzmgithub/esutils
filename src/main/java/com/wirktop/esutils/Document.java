package com.wirktop.esutils;

import java.util.Map;

/**
 * @author Cosmin Marginean
 */
public class Document {

    private String id;
    private String source;

    public Document() {
    }

    public Document(String id, String source) {
        this.id = id;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
