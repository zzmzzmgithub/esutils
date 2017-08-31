package com.wirktop.esutils;

/**
 * @author Cosmin Marginean
 */
public class DataBucket {

    private String index;
    private String type;

    public DataBucket(String index, String type) {
        if (index == null) {
            throw new IllegalArgumentException("index argument cannot be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("type argument cannot be null");
        }

        this.index = index;
        this.type = type;
    }

    protected void createIndex(Admin admin) {
        createIndex(admin, 0);
    }

    protected void createIndex(Admin admin, int shards) {
        admin.createIndex(index, shards);
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }
}
