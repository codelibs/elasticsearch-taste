package org.codelibs.elasticsearch.taste.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveArrayIterator;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.model.cache.DmKey;
import org.codelibs.elasticsearch.taste.model.cache.DmValue;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

public class ElasticsearchDataModel implements DataModel {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_SCROLL_SIZE = 1000;

    private static final int DEFAULT_MAX_PREFERENCE_SIZE = 10000;

    private static final ESLogger logger = Loggers
            .getLogger(ElasticsearchDataModel.class);

    protected Client client;

    protected String preferenceIndex;

    protected String userIndex;

    protected String itemIndex;

    protected String preferenceType = TasteConstants.PREFERENCE_TYPE;

    protected String userType = TasteConstants.USER_TYPE;

    protected String itemType = TasteConstants.ITEM_TYPE;

    protected String userIdField = TasteConstants.USER_ID_FIELD;

    protected String itemIdField = TasteConstants.ITEM_ID_FIELD;

    protected String valueField = TasteConstants.VALUE_FIELD;

    protected String timestampField = TasteConstants.TIMESTAMP_FIELD;

    protected Scroll scrollKeepAlive = new Scroll(TimeValue.timeValueMinutes(1));

    protected int scrollSize = DEFAULT_SCROLL_SIZE;

    protected int maxPreferenceSize = DEFAULT_MAX_PREFERENCE_SIZE;

    protected volatile long[] userIDs;

    protected volatile long[] itemIDs;

    protected volatile Stats stats;

    protected Date lastAccessed = new Date();

    protected QueryBuilder userQueryBuilder = QueryBuilders.matchAllQuery();

    protected QueryBuilder itemQueryBuilder = QueryBuilders.matchAllQuery();

    protected Cache<DmKey, DmValue> cache;

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        cache.cleanUp();
    }

    public void setMaxCacheWeight(final long weight) {
        final Weigher<DmKey, DmValue> weigher = (key, value) -> 24 + value
                .getSize();
        cache = CacheBuilder.newBuilder().maximumWeight(weight)
                .weigher(weigher).build();
    }

    @Override
    public LongPrimitiveIterator getUserIDs() {
        if (userIDs == null) {
            loadUserIDs();
        }
        return new LongPrimitiveArrayIterator(userIDs);
    }

    protected boolean existsUserID(final long userID) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.EXISTS_USER_ID, userID));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        if (userIDs == null) {
            loadUserIDs();
        }

        // use elasticsearch?
        boolean exists = false;
        for (final long id : userIDs) {
            if (id == userID) {
                exists = true;
                break;
            }
        }

        if (cache != null) {
            cache.put(DmKey.create(DmKey.EXISTS_USER_ID, userID), new DmValue(
                    exists, 16));
        }
        return exists;
    }

    @Override
    public PreferenceArray getPreferencesFromUser(final long userID) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.PREFERENCES_FROM_USER, userID));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        final SearchResponse response = getPreferenceSearchResponse(
                userIdField, userID, itemIdField, valueField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > maxPreferenceSize) {
            logger.warn("UserID {} has {} items over {}.", userID, totalHits,
                    maxPreferenceSize);
            totalHits = maxPreferenceSize;
        }

        long oldId = -1;
        final int size = (int) totalHits;
        final List<Preference> prefList = new ArrayList<>(size);
        for (final SearchHit hit : response.getHits()) {
            final long itemID = getLongValue(hit, itemIdField);
            if (itemID != oldId && existsItemID(itemID)) {
                final float value = getFloatValue(hit, valueField);
                prefList.add(new GenericPreference(userID, itemID, value));
                oldId = itemID;
            }
        }

        final PreferenceArray preferenceArray = new GenericUserPreferenceArray(
                prefList);

        if (cache != null) {
            cache.put(DmKey.create(DmKey.PREFERENCES_FROM_USER, userID),
                    new DmValue(preferenceArray, size * 4 * 8 + 100));
        }
        return preferenceArray;
    }

    @Override
    public FastIDSet getItemIDsFromUser(final long userID) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.ITEMIDS_FROM_USER, userID));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        final SearchResponse response = getPreferenceSearchResponse(
                userIdField, userID, itemIdField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > maxPreferenceSize) {
            logger.warn("UserID {} has {} items over {}.", userID, totalHits,
                    maxPreferenceSize);
            totalHits = maxPreferenceSize;
        }

        final int size = (int) totalHits;
        final FastIDSet result = new FastIDSet(size);
        for (final SearchHit hit : response.getHits()) {
            final long itemID = getLongValue(hit, itemIdField);
            if (existsItemID(itemID)) {
                result.add(itemID);
            }
        }

        if (cache != null) {
            cache.put(DmKey.create(DmKey.ITEMIDS_FROM_USER, userID),
                    new DmValue(result, size * 8 + 100));
        }
        return result;
    }

    @Override
    public LongPrimitiveIterator getItemIDs() {
        if (itemIDs == null) {
            loadItemIDs();
        }
        return new LongPrimitiveArrayIterator(itemIDs);
    }

    protected boolean existsItemID(final long itemID) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.EXISTS_ITEM_ID, itemID));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        if (itemIDs == null) {
            loadItemIDs();
        }

        // use elasticsearch?
        boolean exists = false;
        for (final long id : itemIDs) {
            if (id == itemID) {
                exists = true;
                break;
            }
        }

        if (cache != null) {
            cache.put(DmKey.create(DmKey.EXISTS_ITEM_ID, itemID), new DmValue(
                    exists, 16));
        }
        return exists;
    }

    @Override
    public PreferenceArray getPreferencesForItem(final long itemID) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.PREFERENCES_FROM_ITEM, itemID));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        final SearchResponse response = getPreferenceSearchResponse(
                itemIdField, itemID, userIdField, valueField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > maxPreferenceSize) {
            logger.warn("ItemID {} has {} users over {}.", itemID, totalHits,
                    maxPreferenceSize);
            totalHits = maxPreferenceSize;
        }

        long oldId = -1;
        final int size = (int) totalHits;
        final List<Preference> prefList = new ArrayList<>(size);
        for (final SearchHit hit : response.getHits()) {
            final long userID = getLongValue(hit, userIdField);
            if (userID != oldId && existsUserID(userID)) {
                final float value = getFloatValue(hit, valueField);
                prefList.add(new GenericPreference(userID, itemID, value));
                oldId = userID;
            }
        }

        final PreferenceArray preferenceArray = new GenericItemPreferenceArray(
                prefList);

        if (cache != null) {
            cache.put(DmKey.create(DmKey.PREFERENCES_FROM_ITEM, itemID),
                    new DmValue(preferenceArray, size * 4 * 8 + 100));
        }
        return preferenceArray;
    }

    @Override
    public Float getPreferenceValue(final long userID, final long itemID) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.PREFERENCE_VALUE, userID, itemID));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        SearchResponse response;
        try {
            response = client.prepareSearch(preferenceIndex)
                    .setTypes(preferenceType)
                    .setQuery(QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery(itemIdField, itemID))
                            .must(QueryBuilders.termQuery(userIdField, userID))
                            .filter(getLastAccessedFilterQuery()))
                    .addFields(valueField)
                    .addSort(timestampField, SortOrder.DESC).setSize(1)
                    .execute().actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to get the preference by ("
                    + userID + "," + itemID + ")", e);
        }

        final SearchHits hits = response.getHits();
        final long totalHits = hits.getTotalHits();
        if (totalHits == 0) {
            return null;
        } else if (totalHits > 1) {
            logger.warn(
                    "ItemID {} of UserID {} has {} preferences. Use the latest value.",
                    itemID, userID, totalHits);
        }

        final SearchHit[] searchHits = hits.getHits();
        if (searchHits.length > 0) {
            final SearchHitField result = searchHits[0].field(valueField);
            if (result != null) {
                final Number value = result.getValue();
                final float floatValue = value.floatValue();
                if (cache != null) {
                    cache.put(DmKey.create(DmKey.PREFERENCE_VALUE, userID,
                            itemID), new DmValue(floatValue, 16));
                }
                return floatValue;
            }
        }

        return null;
    }

    @Override
    public Long getPreferenceTime(final long userID, final long itemID) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.PREFERENCE_TIME, userID, itemID));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        SearchResponse response;
        try {
            response = client
                    .prepareSearch(preferenceIndex)
                    .setTypes(preferenceType)
                    .setQuery(QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery(itemIdField, itemID))
                            .must(QueryBuilders.termQuery(userIdField, userID))
                            .filter(getLastAccessedFilterQuery()))
                    .addFields(timestampField)
                    .addSort(timestampField, SortOrder.DESC).setSize(1)
                    .execute().actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to get the timestamp by ("
                    + userID + "," + itemID + ")", e);
        }

        final SearchHits hits = response.getHits();
        final long totalHits = hits.getTotalHits();
        if (totalHits == 0) {
            return null;
        } else if (totalHits > 1) {
            logger.warn(
                    "ItemID {} of UserID {} has {} preferences. Use the latest value.",
                    itemID, userID, totalHits);
        }

        final SearchHit[] searchHits = hits.getHits();
        if (searchHits.length > 0) {
            final SearchHitField result = searchHits[0].field(timestampField);
            if (result != null) {
                final Date date = result.getValue();
                final long time = date.getTime();
                if (cache != null) {
                    cache.put(
                            DmKey.create(DmKey.PREFERENCE_TIME, userID, itemID),
                            new DmValue(time, 16));
                }
                return time;
            }
        }

        return null;
    }

    @Override
    public int getNumItems() {
        if (itemIDs == null) {
            loadItemIDs();
        }
        return itemIDs.length;
    }

    @Override
    public int getNumUsers() {
        if (userIDs == null) {
            loadUserIDs();
        }
        return userIDs.length;
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.NUM_USERS_FOR_ITEM, itemID));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        final PreferenceArray array = getPreferencesForItem(itemID);
        final int length = array.length();
        if (cache != null) {
            cache.put(DmKey.create(DmKey.NUM_USERS_FOR_ITEM, itemID),
                    new DmValue(length, 16));
        }
        return length;
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID1,
            final long itemID2) {
        if (cache != null) {
            final DmValue dmValue = cache.getIfPresent(DmKey.key(
                    DmKey.NUM_USERS_FOR_ITEMS, itemID1, itemID2));
            if (dmValue != null) {
                return dmValue.getValue();
            }
        }

        int count = 0;
        int pos = 0;
        final PreferenceArray array1 = getPreferencesForItem(itemID1);
        final PreferenceArray array2 = getPreferencesForItem(itemID2);
        final int length1 = array1.length();
        final int length2 = array2.length();
        for (int index1 = 0; index1 < length1; index1++) {
            final long userID1 = array1.getUserID(index1);
            for (int index2 = pos; index2 < length2; index2++) {
                final long userID2 = array2.getUserID(index2);
                if (userID1 == userID2) {
                    count++;
                    pos = index2 + 1;
                    continue;
                } else if (userID1 < userID2) {
                    pos = index2;
                    break;
                }
            }
        }

        if (cache != null) {
            cache.put(
                    DmKey.create(DmKey.NUM_USERS_FOR_ITEMS, itemID1, itemID2),
                    new DmValue(count, 16));
        }
        return count;
    }

    @Override
    public void setPreference(final long userID, final long itemID,
            final float value) {
        createUserID(userID);
        createItemID(itemID);

        final Map<String, Object> source = new HashMap<>();
        source.put(userIdField, userID);
        source.put(itemIdField, itemID);
        source.put(valueField, value);
        source.put(timestampField, new Date());
        try {
            client.prepareIndex(preferenceIndex, preferenceType)
                    .setSource(source).setRefresh(true).execute().actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to set (" + userID + "," + itemID
                    + "," + value + ")", e);
        }
    }

    private void createUserID(final long userID) {
        final GetResponse getResponse = client
                .prepareGet(userIndex, userType, Long.toString(userID))
                .setRefresh(true).execute().actionGet();
        if (!getResponse.isExists()) {
            final Map<String, Object> source = new HashMap<>();
            source.put("system_id", Long.toString(userID));
            source.put(userIdField, userID);
            source.put(timestampField, new Date());
            final IndexResponse response = client
                    .prepareIndex(userIndex, userType, Long.toString(userID))
                    .setSource(source).setRefresh(true).execute().actionGet();
            if (!response.isCreated()) {
                throw new TasteException("Failed to create " + source);
            }
        }
    }

    private void createItemID(final long itemID) {
        final GetResponse getResponse = client
                .prepareGet(itemIndex, itemType, Long.toString(itemID))
                .setRefresh(true).execute().actionGet();
        if (!getResponse.isExists()) {
            final Map<String, Object> source = new HashMap<>();
            source.put("system_id", Long.toString(itemID));
            source.put(itemIdField, itemID);
            source.put(timestampField, new Date());
            final IndexResponse response = client
                    .prepareIndex(itemIndex, itemType, Long.toString(itemID))
                    .setSource(source).setRefresh(true).execute().actionGet();
            if (!response.isCreated()) {
                throw new TasteException("Failed to create " + source);
            }
        }
    }

    @Override
    public void removePreference(final long userID, final long itemID) {
        SearchResponse response = null;
        try {
            while (true) {
                if (response == null) {
                    response = client.prepareSearch(preferenceIndex)
                            .setTypes(preferenceType).setScroll(scrollKeepAlive)
                            .setQuery(QueryBuilders.boolQuery()
                                    .must(QueryBuilders.termQuery(userIdField,
                                            userID))
                                    .must(QueryBuilders.termQuery(itemIdField,
                                            itemID))
                                    .filter(getLastAccessedFilterQuery()))
                            .addFields("_id").setSize(scrollSize).execute()
                            .actionGet();
                } else {
                    response = client
                            .prepareSearchScroll(response.getScrollId())
                            .setScroll(scrollKeepAlive).execute().actionGet();
                }
                final int size = response.getHits().getHits().length;
                if (size == 0) {
                    break;
                }

                final BulkRequestBuilder bulkRequest = client.prepareBulk();
                for (final SearchHit hit : response.getHits()) {
                    bulkRequest.add(client.prepareDelete(hit.getIndex(),
                            hit.getType(), hit.getId()));
                }
                bulkRequest.execute().actionGet();
            }
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to remove the preference by ("
                    + userID + "," + itemID + ")", e);
        }
    }

    @Override
    public boolean hasPreferenceValues() {
        return true;
    }

    @Override
    public float getMaxPreference() {
        if (stats == null) {
            loadValueStats();
        }

        return (float) stats.getMax();
    }

    @Override
    public float getMinPreference() {
        if (stats == null) {
            loadValueStats();
        }

        return (float) stats.getMin();
    }

    protected SearchResponse getPreferenceSearchResponse(
            final String targetField, final long targetID,
            final String... resultFields) {
        try {
            return client
                    .prepareSearch(preferenceIndex)
                    .setTypes(preferenceType)
                    .setQuery(QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery(targetField,
                                    targetID))
                            .filter(getLastAccessedFilterQuery()))
                    .addFields(resultFields)
                    .addSort(resultFields[0], SortOrder.ASC)
                    .addSort(timestampField, SortOrder.DESC)
                    .setSize(maxPreferenceSize).execute().actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to get the preference by "
                    + targetField + ":" + targetID, e);
        }
    }

    protected long getLongValue(final SearchHit hit, final String field) {
        final SearchHitField result = hit.field(field);
        if (result == null) {
            throw new TasteException(field + " is not found.");
        }
        final Number longValue = result.getValue();
        if (longValue == null) {
            throw new TasteException("The result of " + field + " is null.");
        }
        return longValue.longValue();
    }

    protected float getFloatValue(final SearchHit hit, final String field) {
        final SearchHitField result = hit.field(field);
        if (result == null) {
            throw new TasteException(field + " is not found.");
        }
        final Number floatValue = result.getValue();
        if (floatValue == null) {
            throw new TasteException("The result of " + field + " is null.");
        }
        return floatValue.floatValue();
    }

    protected synchronized void loadUserIDs() {
        if (userIDs != null) {
            return;
        }

        SearchResponse response = null;
        int size = 0;
        long[] ids = new long[size];
        int index = 0;
        try {
            while (true) {
                if (response == null) {
                    response = client.prepareSearch(userIndex)
                            .setTypes(userType)
                            .setScroll(scrollKeepAlive)
                            .setQuery(QueryBuilders.boolQuery()
                                    .must(userQueryBuilder)
                                    .filter(getLastAccessedFilterQuery()))
                            .addFields(userIdField).setSize(scrollSize)
                            .execute().actionGet();
                    long totalHits = response.getHits().getTotalHits();
                    if (totalHits > Integer.MAX_VALUE) {
                        logger.warn("The number of users is {} > {}.", totalHits,
                                Integer.MAX_VALUE);
                        totalHits = Integer.MAX_VALUE;
                    }
                    size = (int)totalHits;
                    ids = new long[size];
                } else {
                    response = client
                            .prepareSearchScroll(response.getScrollId())
                            .setScroll(scrollKeepAlive).execute().actionGet();
                }
                if (response.getHits().getHits().length == 0) {
                    break;
                }
                for (final SearchHit hit : response.getHits()) {
                    ids[index] = getLongValue(hit, userIdField);
                    index++;
                }
            }
        } catch (final ElasticsearchException e) {
            throw new TasteException(
                    "Failed to scroll the results by userIDs.", e);
        }

        if (index != size) {
            throw new TasteException("The total size " + size
                    + " and the result " + index + " are not matched");
        }
        Arrays.sort(ids);
        userIDs = ids;
    }

    protected synchronized void loadItemIDs() {
        if (itemIDs != null) {
            return;
        }

        SearchResponse response = null;
        int size = 0;
        long[] ids = new long[size];
        int index = 0;
        try {
            while (true) {
                if (response == null) {
                    response = client.prepareSearch(itemIndex)
                            .setTypes(itemType)
                            .setScroll(scrollKeepAlive)
                            .setQuery(QueryBuilders.boolQuery()
                                    .must(itemQueryBuilder)
                                    .filter(getLastAccessedFilterQuery()))
                            .addFields(itemIdField).setSize(scrollSize)
                            .execute().actionGet();
                    long totalHits = response.getHits().getTotalHits();
                    if (totalHits > Integer.MAX_VALUE) {
                        logger.warn("The number of items is {} > {}.",
                                totalHits, Integer.MAX_VALUE);
                        totalHits = Integer.MAX_VALUE;
                    }
                    size = (int) totalHits;
                    ids = new long[size];
                } else {
                    response = client
                            .prepareSearchScroll(response.getScrollId())
                            .setScroll(scrollKeepAlive).execute().actionGet();
                }
                if (response.getHits().getHits().length == 0) {
                    break;
                }
                for (final SearchHit hit : response.getHits()) {
                    ids[index] = getLongValue(hit, itemIdField);
                    index++;
                }
            }
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to scroll the result by itemIDs.",
                    e);
        }

        if (index != size) {
            throw new TasteException("The total size " + size
                    + " and the result " + index + " are not matched");
        }
        Arrays.sort(ids);
        itemIDs = ids;
    }

    private RangeQueryBuilder getLastAccessedFilterQuery() {
        return QueryBuilders.rangeQuery(timestampField).to(lastAccessed);
    }

    protected synchronized void loadValueStats() {
        if (stats != null) {
            return;
        }
        // TODO join userQueryBuilder and itemQueryBuilder
        final SearchResponse response = client
                .prepareSearch(preferenceIndex)
                .setTypes(preferenceType)
                .setQuery(getLastAccessedFilterQuery())
                .setSize(0)
                .addAggregation(
                        AggregationBuilders.stats(valueField).field(valueField))
                .execute().actionGet();
        final Aggregations aggregations = response.getAggregations();
        stats = aggregations.get(valueField);
    }

    public Date getLastAccessed() {
        return lastAccessed;
    }

    public void setLastAccessed(final Date lastAccessed) {
        this.lastAccessed = lastAccessed;
    }

    public void setClient(final Client client) {
        this.client = client;
    }

    public void setPreferenceIndex(final String preferenceIndex) {
        this.preferenceIndex = preferenceIndex;
    }

    public void setUserIndex(final String userIndex) {
        this.userIndex = userIndex;
    }

    public void setItemIndex(final String itemIndex) {
        this.itemIndex = itemIndex;
    }

    public void setPreferenceType(final String preferenceType) {
        this.preferenceType = preferenceType;
    }

    public void setUserType(final String userType) {
        this.userType = userType;
    }

    public void setItemType(final String itemType) {
        this.itemType = itemType;
    }

    public void setUserIdField(final String userIDField) {
        userIdField = userIDField;
    }

    public void setItemIdField(final String itemIDField) {
        itemIdField = itemIDField;
    }

    public void setValueField(final String valueField) {
        this.valueField = valueField;
    }

    public void setTimestampField(final String timestampField) {
        this.timestampField = timestampField;
    }

    public void setScrollKeepAlive(final Scroll scrollKeepAlive) {
        this.scrollKeepAlive = scrollKeepAlive;
    }

    public void setScrollSize(final int scrollSize) {
        this.scrollSize = scrollSize;
    }

    public void setUserQueryBuilder(final QueryBuilder userQueryBuilder) {
        this.userQueryBuilder = userQueryBuilder;
    }

    public void setItemQueryBuilder(final QueryBuilder itemQueryBuilder) {
        this.itemQueryBuilder = itemQueryBuilder;
    }

    public void setMaxPreferenceSize(final int maxPreferenceSize) {
        this.maxPreferenceSize = maxPreferenceSize;
    }
}
