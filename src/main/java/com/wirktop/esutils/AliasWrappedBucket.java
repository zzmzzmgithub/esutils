package com.wirktop.esutils;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * @author Cosmin Marginean
 */
public class AliasWrappedBucket extends DataBucket {

    private Admin admin;

    public AliasWrappedBucket(String index, String type, Admin admin) {
        super(index, type);
        this.admin = admin;
    }

    @Override
    public void createIndex(Admin admin, int shards) {
        String alias = getIndex();
        String crtIndex = actualIndex(alias);
        if (crtIndex == null || !admin.indexExists(crtIndex)) {
            String nextIndex = nextIndexVersion(alias);
            admin.createIndex(nextIndex, shards);
            admin.createAlias(alias, nextIndex);
        }
    }

    public void wipe() {
        String alias = getIndex();
        String crtIndex = actualIndex(alias);
        String nextIndex = nextIndexVersion(alias);
        admin.removeAlias(alias);
        if (crtIndex != null && admin.indexExists(crtIndex)) {
            admin.removeIndex(crtIndex);
        }

        admin.createIndex(nextIndex);
        admin.removeAlias(alias);
        admin.createAlias(alias, nextIndex);
    }

    private String actualIndex(String aliasWrapper) {
        Collection<String> indices = admin.indexesForAlias(aliasWrapper);
        Pattern pattern = Pattern.compile(aliasWrapper + "_\\d{12}");
        for (String index : indices) {
            if (pattern.matcher(index).matches()) {
                return index;
            }
        }
        return null;
    }

    private String nextIndexVersion(String aliasWrapper) {
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
