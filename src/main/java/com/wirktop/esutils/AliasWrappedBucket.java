package com.wirktop.esutils;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * @author Cosmin Marginean
 */
public class AliasWrappedBucket extends DataBucket {

    public AliasWrappedBucket(String index, String type) {
        super(index, type);
    }

    @Override
    protected void createIndex(Admin admin, int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex(admin, alias);
        if (crtIndex == null || !admin.indexExists(crtIndex)) {
            String nextIndex = nextIndexVersion(admin, alias);
            admin.createIndex(nextIndex, shards);
            admin.createAlias(alias, nextIndex);
        }
    }

    protected void wipe(Admin admin, int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex(admin, alias);
        String nextIndex = nextIndexVersion(admin, alias);
        admin.createIndex(nextIndex, shards);
        admin.moveAlias(alias, crtIndex, nextIndex);
        admin.removeIndex(crtIndex);
    }

    protected void refresh(Admin admin, int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex(admin, alias);
        String nextIndex = nextIndexVersion(admin, alias);
        admin.createIndex(nextIndex, shards);
        admin.copyData(crtIndex, nextIndex);
        admin.moveAlias(alias, crtIndex, nextIndex);
        admin.removeIndex(crtIndex);
    }

    private String actualIndex(Admin admin, String aliasWrapper) {
        Collection<String> indices = admin.indexesForAlias(aliasWrapper);
        Pattern pattern = Pattern.compile(aliasWrapper + "_\\d{12}");
        for (String index : indices) {
            if (pattern.matcher(index).matches()) {
                return index;
            }
        }
        return null;
    }

    private String nextIndexVersion(Admin admin, String aliasWrapper) {
        Collection<String> indices = admin.indexesForAlias(aliasWrapper);
        long crtVersion = 0;
        for (String index : indices) {
            if (index.startsWith(aliasWrapper)) {
                crtVersion = Long.parseLong(index.substring(index.lastIndexOf("_") + 1));
                break;
            }
        }
        return aliasWrapper + "_" + String.format("%012d", crtVersion + 1);
    }
}
