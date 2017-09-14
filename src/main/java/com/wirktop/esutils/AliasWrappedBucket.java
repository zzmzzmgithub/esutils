package com.wirktop.esutils;

import java.util.Collection;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * @author Cosmin Marginean
 */
public class AliasWrappedBucket extends DataBucket {

    protected AliasWrappedBucket(Admin admin, String index, String type) {
        super(admin, index, type);
    }

    @Override
    protected void createIndex(int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex();
        if (crtIndex == null || !getAdmin().indexExists(crtIndex)) {
            String nextIndex = newIndexName();
            getAdmin().createIndex(nextIndex, shards);
            getAdmin().createAlias(alias, nextIndex);
        }
    }

    protected void wipe() {
        wipe(0);
    }

    public void wipe(int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex();
        String nextIndex = newIndexName();
        getAdmin().createIndex(nextIndex, shards);
        getAdmin().moveAlias(alias, crtIndex, nextIndex);
        getAdmin().removeIndex(crtIndex);
    }

    public void refresh(int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex();
        String nextIndex = newIndexName();
        getAdmin().createIndex(nextIndex, shards);
        getAdmin().copyData(crtIndex, nextIndex);
        getAdmin().moveAlias(alias, crtIndex, nextIndex);
        getAdmin().removeIndex(crtIndex);
    }

    public DataBucket createNewIndex() {
        return createNewIndex(0);
    }

    public DataBucket createNewIndex(int shards) {
        String index = newIndexName();
        DataBucket dataBucket = new DataBucket(getAdmin(), index, getType());
        getAdmin().createIndex(index, shards);
        return dataBucket;
    }

    public AliasWrappedBucket wrap(DataBucket dataBucket, boolean deleteCurrentIndex) {
        String oldIndex = actualIndex();
        getAdmin().moveAlias(getIndex(), oldIndex, dataBucket.getIndex());
        if (deleteCurrentIndex) {
            getAdmin().removeIndex(oldIndex);
        }
        return this;
    }

    public String actualIndex() {
        Collection<String> indices = getAdmin().indexesForAlias(getIndex());
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

    private static String uuid() {
        return UUID.randomUUID().toString().toLowerCase().replaceAll("-", "");
    }
}
