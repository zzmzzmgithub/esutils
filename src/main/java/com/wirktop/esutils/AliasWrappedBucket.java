package com.wirktop.esutils;

import java.util.Collection;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * @author Cosmin Marginean
 */
public class AliasWrappedBucket extends DataBucket {

    public AliasWrappedBucket(String index, String type) {
        super(index, type);
    }

    private static String uuid() {
        return UUID.randomUUID().toString().toLowerCase().replaceAll("-", "");
    }

    @Override
    public void createIndex(Admin admin, int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex(admin);
        if (crtIndex == null || !admin.indexExists(crtIndex)) {
            String nextIndex = newIndexName();
            admin.createIndex(nextIndex, shards);
            admin.createAlias(alias, nextIndex);
        }
    }

    public void wipe(Admin admin) {
        wipe(admin, 0);
    }

    public void wipe(Admin admin, int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex(admin);
        String nextIndex = newIndexName();
        admin.createIndex(nextIndex, shards);
        admin.moveAlias(alias, crtIndex, nextIndex);
        admin.removeIndex(crtIndex);
    }

    public void refresh(Admin admin) {
        refresh(admin, 0);
    }

    public void refresh(Admin admin, int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex(admin);
        String nextIndex = newIndexName();
        admin.createIndex(nextIndex, shards);
        admin.copyData(crtIndex, nextIndex);
        admin.moveAlias(alias, crtIndex, nextIndex);
        admin.removeIndex(crtIndex);
    }

    public DataBucket createNewIndex(Admin admin) {
        return createNewIndex(admin, 0);
    }

    public DataBucket createNewIndex(Admin admin, int shards) {
        String index = newIndexName();
        DataBucket dataBucket = new DataBucket(index, getType());
        admin.createIndex(index, shards);
        return dataBucket;
    }

    public AliasWrappedBucket wrap(Admin admin, DataBucket dataBucket, boolean deleteCurrentIndex) {
        String oldIndex = actualIndex(admin);
        admin.moveAlias(getIndex(), oldIndex, dataBucket.getIndex());
        if (deleteCurrentIndex) {
            admin.removeIndex(oldIndex);
        }
        return this;
    }

    public String actualIndex(Admin admin) {
        Collection<String> indices = admin.indexesForAlias(getIndex());
        Pattern pattern = Pattern.compile(getIndex() + "\\..{32}");
        for (String index : indices) {
            if (pattern.matcher(index).matches()) {
                return index;
            }
        }
        return null;
    }

    private String newIndexName() {
        return getIndex() + "." + uuid();
    }
}
