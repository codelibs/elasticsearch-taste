package org.codelibs.elasticsearch.taste.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.recommender.SimilarUser;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;

import com.google.common.cache.Cache;

public class UserWriter extends ObjectWriter {

    protected String targetIdField;

    protected String userIdField = TasteConstants.USER_ID_FIELD;

    protected String valueField = TasteConstants.VALUE_FIELD;

    protected String usersField = TasteConstants.USERS_FILED;

    protected boolean verbose = false;

    protected String userIndex;

    protected String userType;

    protected Cache<Long, Map<String, Object>> cache;

    public UserWriter(final Client client, final String index,
            final String type, final String targetIdField, final int capacity) {
        super(client, index, type, capacity);
        this.targetIdField = targetIdField;
    }

    public void write(final long userID,
            final List<SimilarUser> mostSimilarUsers) {
        final Map<String, Object> rootObj = new HashMap<>();
        rootObj.put(targetIdField, userID);
        if (verbose) {
            final GetResponse response = client
                    .prepareGet(userIndex, userType, Long.toString(userID))
                    .execute().actionGet();
            if (response.isExists()) {
                final Map<String, Object> map = response.getSourceAsMap();
                map.remove(targetIdField);
                rootObj.putAll(map);
            }
        }
        final List<Map<String, Object>> userList = new ArrayList<>();
        for (final SimilarUser similarUser : mostSimilarUsers) {
            final Map<String, Object> user = new HashMap<>();
            user.put(userIdField, similarUser.getUserID());
            user.put(valueField, similarUser.getSimilarity());
            if (verbose) {
                final Map<String, Object> map = getUserMap(similarUser
                        .getUserID());
                if (map != null) {
                    user.putAll(map);
                }
            }
            userList.add(user);
        }
        rootObj.put(usersField, userList);

        write(rootObj);

    }

    protected Map<String, Object> getUserMap(final long userID) {
        if (cache != null) {
            final Map<String, Object> map = cache.getIfPresent(userID);
            if (map != null) {
                return map;
            }
        }
        final GetResponse response = client
                .prepareGet(userIndex, userType, Long.toString(userID))
                .execute().actionGet();
        if (response.isExists()) {
            final Map<String, Object> map = response.getSourceAsMap();
            map.remove(userIdField);
            map.remove(valueField);
            if (cache != null) {
                cache.put(userID, map);
            }
            return map;
        }
        return null;
    }

    public void setUserIdField(final String userIdField) {
        this.userIdField = userIdField;
    }

    public void setValueField(final String valueField) {
        this.valueField = valueField;
    }

    public void setUsersField(final String usersField) {
        this.usersField = usersField;
    }

    public void setUserIndex(final String itemIndex) {
        userIndex = itemIndex;
    }

    public void setUserType(final String itemType) {
        userType = itemType;
    }

    public void setCache(final Cache<Long, Map<String, Object>> cache) {
        this.cache = cache;
    }

    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

}
