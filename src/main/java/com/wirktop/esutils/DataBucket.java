package com.wirktop.esutils;

/**
 * @author Cosmin Marginean
 */
public class DataBucket {

    private String index;
    private String type;
    private Admin admin;

    protected DataBucket(Admin admin, String index, String type) {
        if (admin == null) {
            throw new IllegalArgumentException("admin argument cannot be null");
        }
        if (index == null) {
            throw new IllegalArgumentException("index argument cannot be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("type argument cannot be null");
        }

        this.admin = admin;
        this.index = index;
        this.type = type;
    }

    protected Admin getAdmin() {
        return admin;
    }

    protected void createIndex() {
        createIndex(0);
    }

    protected void createIndex(int shards) {
        admin.createIndex(index, shards);
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }
}
