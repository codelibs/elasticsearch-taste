package org.codelibs.elasticsearch.taste.model;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveArrayIterator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericItemPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.codelibs.elasticsearch.taste.TasteConstants;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.sort.SortOrder;

public class ElasticsearchDataModel implements DataModel {

    private static final long serialVersionUID = 1L;

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

    protected int scrollSize = 1000;

    protected volatile long[] userIDs;

    protected volatile long[] itemIDs;

    protected volatile Stats stats;

    protected Date lastAccessed = new Date();

    protected QueryBuilder userQueryBuilder = QueryBuilders.matchAllQuery();

    protected QueryBuilder itemQueryBuilder = QueryBuilders.matchAllQuery();

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        // TODO
    }

    @Override
    public LongPrimitiveIterator getUserIDs() throws TasteException {
        if (userIDs == null) {
            loadUserIDs();
        }
        return new LongPrimitiveArrayIterator(userIDs);
    }

    @Override
    public PreferenceArray getPreferencesFromUser(final long userID)
            throws TasteException {
        SearchResponse response = getPreferenceSearchResponse(userIdField,
                userID, itemIdField, valueField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("UserID {} has {} items over {}.", userID, totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        final int size = (int) totalHits;
        final PreferenceArray preferenceArray = new GenericUserPreferenceArray(
                size);
        preferenceArray.setUserID(0, userID);
        int index = 0;
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (final SearchHit hit : response.getHits()) {
                    preferenceArray.setItemID(index,
                            getLongValue(hit, itemIdField));
                    preferenceArray.setValue(index,
                            getFloatValue(hit, valueField));
                    index++;
                }

                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
        } catch (final ElasticsearchException e) {
            throw new TasteException(
                    "Failed to scroll the result by " + userID, e);
        }

        if (index != size) {
            throw new TasteException("The total size " + size
                    + " and the result " + index + " are not matched");
        }
        return preferenceArray;
    }

    @Override
    public FastIDSet getItemIDsFromUser(final long userID)
            throws TasteException {
        SearchResponse response = getPreferenceSearchResponse(userIdField,
                userID, itemIdField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("UserID {} has {} items over {}.", userID, totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        final FastIDSet result = new FastIDSet((int) totalHits);
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (final SearchHit hit : response.getHits()) {
                    result.add(getLongValue(hit, itemIdField));
                }

                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
        } catch (final ElasticsearchException e) {
            throw new TasteException(
                    "Failed to scroll the result by " + userID, e);
        }

        return result;
    }

    @Override
    public LongPrimitiveIterator getItemIDs() throws TasteException {
        if (itemIDs == null) {
            loadItemIDs();
        }
        return new LongPrimitiveArrayIterator(itemIDs);
    }

    @Override
    public PreferenceArray getPreferencesForItem(final long itemID)
            throws TasteException {
        SearchResponse response = getPreferenceSearchResponse(itemIdField,
                itemID, userIdField, valueField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("ItemID {} has {} users over {}.", itemID, totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        final int size = (int) totalHits;
        final PreferenceArray preferenceArray = new GenericItemPreferenceArray(
                size);
        preferenceArray.setItemID(0, itemID);
        int index = 0;
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (final SearchHit hit : response.getHits()) {
                    preferenceArray.setUserID(index,
                            getLongValue(hit, userIdField));
                    preferenceArray.setValue(index,
                            getFloatValue(hit, valueField));
                    index++;
                }

                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
        } catch (final ElasticsearchException e) {
            throw new TasteException(
                    "Failed to scroll the result by " + itemID, e);
        }

        if (index != size) {
            throw new TasteException("The total size " + size
                    + " and the result " + index + " are not matched");
        }
        return preferenceArray;
    }

    @Override
    public Float getPreferenceValue(final long userID, final long itemID)
            throws TasteException {
        SearchResponse response;
        try {
            response = client
                    .prepareSearch(preferenceIndex)
                    .setTypes(preferenceType)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.termQuery(
                                                    itemIdField, itemID))
                                            .must(QueryBuilders.termQuery(
                                                    userIdField, userID)),
                                    getLastAccessedFilterQuery()))
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
                return value.floatValue();
            }
        }

        return null;
    }

    @Override
    public Long getPreferenceTime(final long userID, final long itemID)
            throws TasteException {
        SearchResponse response;
        try {
            response = client
                    .prepareSearch(preferenceIndex)
                    .setTypes(preferenceType)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.termQuery(
                                                    itemIdField, itemID))
                                            .must(QueryBuilders.termQuery(
                                                    userIdField, userID)),
                                    getLastAccessedFilterQuery()))
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
                return date.getTime();
            }
        }

        return null;
    }

    @Override
    public int getNumItems() throws TasteException {
        if (itemIDs == null) {
            loadItemIDs();
        }
        return itemIDs.length;
    }

    @Override
    public int getNumUsers() throws TasteException {
        if (userIDs == null) {
            loadUserIDs();
        }
        return userIDs.length;
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID)
            throws TasteException {
        return getNumByQuery(userIndex, userType,
                QueryBuilders.termQuery(itemIdField, itemID));
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID1,
            final long itemID2) throws TasteException {
        return getNumByQuery(
                userIndex,
                userType,
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(itemIdField, itemID1))
                        .must(QueryBuilders.termQuery(itemIdField, itemID2)));
    }

    @Override
    public void setPreference(final long userID, final long itemID,
            final float value) throws TasteException {
        final Map<String, Object> source = new HashMap<String, Object>();
        source.put(userIdField, userID);
        source.put(itemIdField, itemID);
        source.put(valueField, value);
        source.put(timestampField, "now");
        try {
            client.prepareIndex(preferenceIndex, preferenceType)
                    .setSource(source).setRefresh(true).execute().actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to set (" + userID + "," + itemID
                    + "," + value + ")", e);
        }

        // TODO add user and item
    }

    @Override
    public void removePreference(final long userID, final long itemID)
            throws TasteException {
        DeleteByQueryResponse response;
        try {
            response = client
                    .prepareDeleteByQuery(preferenceIndex)
                    .setTypes(preferenceType)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.termQuery(
                                                    userIdField, userID))
                                            .must(QueryBuilders.termQuery(
                                                    itemIdField, itemID)),
                                    getLastAccessedFilterQuery())).execute()
                    .actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to remove the preference by ("
                    + userID + "," + itemID + ")", e);
        }
        for (final IndexDeleteByQueryResponse res : response) {
            final int totalShards = res.getTotalShards();
            final int successfulShards = res.getSuccessfulShards();
            if (totalShards != successfulShards) {
                throw new TasteException(totalShards - successfulShards
                        + " shards are failed.");
            }
            final ShardOperationFailedException[] failures = res.getFailures();
            if (failures.length > 0) {
                final StringBuilder buf = new StringBuilder();
                for (final ShardOperationFailedException failure : failures) {
                    buf.append('\n').append(failure.toString());
                }
                throw new TasteException("Search Operation Failed: "
                        + buf.toString());
            }
        }

        // TODO add user and item
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
            final String... resultFields) throws TasteException {
        try {
            return client
                    .prepareSearch(preferenceIndex)
                    .setTypes(preferenceType)
                    .setSearchType(SearchType.SCAN)
                    .setScroll(scrollKeepAlive)
                    .setQuery(
                            QueryBuilders.filteredQuery(QueryBuilders
                                    .termQuery(targetField, targetID),
                                    getLastAccessedFilterQuery()))
                    .addFields(resultFields)
                    .addSort(resultFields[0], SortOrder.ASC)
                    .setSize(scrollSize).execute().actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to get the preference by "
                    + targetField + ":" + targetID, e);
        }
    }

    protected long getLongValue(final SearchHit hit, final String field)
            throws TasteException {
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

    protected float getFloatValue(final SearchHit hit, final String field)
            throws TasteException {
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

    protected synchronized void loadUserIDs() throws TasteException {
        if (userIDs != null) {
            return;
        }

        SearchResponse response;
        try {
            response = client
                    .prepareSearch(userIndex)
                    .setTypes(userType)
                    .setSearchType(SearchType.SCAN)
                    .setScroll(scrollKeepAlive)
                    .setQuery(
                            QueryBuilders.filteredQuery(userQueryBuilder,
                                    getLastAccessedFilterQuery()))
                    .addFields(userIdField).addSort(userIdField, SortOrder.ASC)
                    .setSize(scrollSize).execute().actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to load userIDs.", e);
        }

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("The number of users is {} > {}.", totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        final int size = (int) totalHits;
        final long[] ids = new long[size];
        int index = 0;
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (final SearchHit hit : response.getHits()) {
                    ids[index] = getLongValue(hit, userIdField);
                    index++;
                }

                if (response.getHits().getHits().length == 0) {
                    break;
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
        userIDs = ids;
    }

    protected synchronized void loadItemIDs() throws TasteException {
        if (itemIDs != null) {
            return;
        }

        SearchResponse response;
        try {
            response = client
                    .prepareSearch(itemIndex)
                    .setTypes(itemType)
                    .setSearchType(SearchType.SCAN)
                    .setScroll(scrollKeepAlive)
                    .setQuery(
                            QueryBuilders.filteredQuery(itemQueryBuilder,
                                    getLastAccessedFilterQuery()))
                    .addFields(itemIdField).addSort(itemIdField, SortOrder.ASC)
                    .setSize(scrollSize).execute().actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to load itemIDs.", e);
        }

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("The number of items is {} > {}.", totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        final int size = (int) totalHits;
        final long[] ids = new long[size];
        int index = 0;
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (final SearchHit hit : response.getHits()) {
                    ids[index] = getLongValue(hit, itemIdField);
                    index++;
                }

                if (response.getHits().getHits().length == 0) {
                    break;
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
        itemIDs = ids;
    }

    private RangeFilterBuilder getLastAccessedFilterQuery() {
        return FilterBuilders.rangeFilter(timestampField).to(lastAccessed);
    }

    protected int getNumByQuery(final String index, final String type,
            final QueryBuilder query) throws TasteException {
        CountResponse response;
        try {
            response = client
                    .prepareCount(index)
                    .setTypes(type)
                    .setQuery(
                            QueryBuilders.filteredQuery(query,
                                    getLastAccessedFilterQuery())).execute()
                    .actionGet();
        } catch (final ElasticsearchException e) {
            throw new TasteException("Failed to count by " + query, e);
        }
        final int totalShards = response.getTotalShards();
        final int successfulShards = response.getSuccessfulShards();
        if (totalShards != successfulShards) {
            throw new TasteException(totalShards - successfulShards
                    + " shards are failed.");
        }
        final ShardOperationFailedException[] failures = response
                .getShardFailures();
        if (failures.length > 0) {
            final StringBuilder buf = new StringBuilder();
            for (final ShardOperationFailedException failure : failures) {
                buf.append('\n').append(failure.toString());
            }
            throw new TasteException("Search Operation Failed: "
                    + buf.toString());
        }
        final long count = response.getCount();
        if (count > Integer.MAX_VALUE) {
            throw new TasteException("The number of results is " + count
                    + " > " + Integer.MAX_VALUE);
        }
        return (int) count;
    }

    protected synchronized void loadValueStats() {
        if (stats != null) {
            return;
        }
        // TODO join userQueryBuilder and itemQueryBuilder
        final SearchResponse response = client
                .prepareSearch(preferenceIndex)
                .setTypes(preferenceType)
                .setQuery(
                        QueryBuilders.filteredQuery(
                                QueryBuilders.matchAllQuery(),
                                getLastAccessedFilterQuery()))
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
}
