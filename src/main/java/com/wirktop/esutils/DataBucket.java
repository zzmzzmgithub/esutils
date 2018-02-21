package com.wirktop.esutils;

/**
 * @author Cosmin Marginean
 */
public class DataBucket {

    private String index;

    public DataBucket(String index) {
        if (index == null) {
            throw new IllegalArgumentException("index argument cannot be null");
        }
        this.index = index;
    }

    public void createIndex(Admin admin) {
        createIndex(admin, 0);
    }

    public void createIndex(Admin admin, int shards) {
        admin.createIndex(getIndex(), shards);
    }

    public String getIndex() {
        return index;
    }
}
