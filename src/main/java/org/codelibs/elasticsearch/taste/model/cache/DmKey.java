package org.codelibs.elasticsearch.taste.model.cache;

public class DmKey {
    public static final int PREFERENCES_FROM_USER = 1;

    public static final int ITEMIDS_FROM_USER = 2;

    public static final int PREFERENCES_FROM_ITEM = 3;

    public static final int PREFERENCE_VALUE = 4;

    public static final int PREFERENCE_TIME = 5;

    public static final int EXISTS_USER_ID = 6;

    public static final int EXISTS_ITEM_ID = 7;

    public static final int NUM_USERS_FOR_ITEM = 8;

    public static final int NUM_USERS_FOR_ITEMS = 9;

    private static final ThreadLocal<DmKey> localDmKey = new ThreadLocal<>();

    private int type;

    private long id1;

    private long id2;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id1 ^ id1 >>> 32);
        result = prime * result + (int) (id2 ^ id2 >>> 32);
        result = prime * result + type;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DmKey other = (DmKey) obj;
        if (id1 != other.id1) {
            return false;
        }
        if (id2 != other.id2) {
            return false;
        }
        if (type != other.type) {
            return false;
        }
        return true;
    }

    public static DmKey key(final int type, final long id1) {
        return key(type, id1, 0);
    }

    public static DmKey key(final int type, final long id1, final long id2) {
        DmKey dmKey = localDmKey.get();
        if (dmKey == null) {
            dmKey = new DmKey();
            localDmKey.set(dmKey);
        }
        dmKey.type = type;
        dmKey.id1 = id1;
        dmKey.id2 = id2;
        return dmKey;
    }

    public static DmKey create(final int type, final long id1) {
        return create(type, id1, 0);
    }

    public static DmKey create(final int type, final long id1, final long id2) {
        final DmKey dmKey = new DmKey();
        dmKey.type = type;
        dmKey.id1 = id1;
        dmKey.id2 = id2;
        return dmKey;
    }

}
