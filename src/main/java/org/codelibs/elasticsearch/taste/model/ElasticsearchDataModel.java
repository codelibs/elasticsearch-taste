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

    protected String index;

    protected String preferenceType = "preference";

    protected String userType = "user";

    protected String itemType = "item";

    protected String userIDField = "userid";

    protected String itemIDField = "itemid";

    protected String valueField = "value";

    protected String timestampField = "@timestamp";

    protected Scroll scrollKeepAlive = new Scroll(TimeValue.timeValueMinutes(1));

    protected int scrollSize = 1000;

    protected volatile long[] userIDs;

    protected volatile long[] itemIDs;

    protected volatile Stats stats;

    protected Date lastAccessed = new Date();

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
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
    public PreferenceArray getPreferencesFromUser(long userID)
            throws TasteException {
        SearchResponse response = getPreferenceSearchResponse(userIDField,
                userID, itemIDField, valueField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("UserID {} has {} items over {}.", userID, totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        int size = (int) totalHits;
        PreferenceArray preferenceArray = new GenericUserPreferenceArray(size);
        preferenceArray.setUserID(0, userID);
        int index = 0;
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (SearchHit hit : response.getHits()) {
                    preferenceArray.setItemID(index,
                            getLongValue(hit, itemIDField));
                    preferenceArray.setValue(index,
                            getFloatValue(hit, valueField));
                    index++;
                }

                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
        } catch (ElasticsearchException e) {
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
    public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
        SearchResponse response = getPreferenceSearchResponse(userIDField,
                userID, itemIDField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("UserID {} has {} items over {}.", userID, totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        FastIDSet result = new FastIDSet((int) totalHits);
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (SearchHit hit : response.getHits()) {
                    result.add(getLongValue(hit, itemIDField));
                }

                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
        } catch (ElasticsearchException e) {
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
    public PreferenceArray getPreferencesForItem(long itemID)
            throws TasteException {
        SearchResponse response = getPreferenceSearchResponse(itemIDField,
                itemID, userIDField, valueField);

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("ItemID {} has {} users over {}.", itemID, totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        int size = (int) totalHits;
        PreferenceArray preferenceArray = new GenericItemPreferenceArray(size);
        preferenceArray.setItemID(0, itemID);
        int index = 0;
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (SearchHit hit : response.getHits()) {
                    preferenceArray.setUserID(index,
                            getLongValue(hit, userIDField));
                    preferenceArray.setValue(index,
                            getFloatValue(hit, valueField));
                    index++;
                }

                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
        } catch (ElasticsearchException e) {
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
    public Float getPreferenceValue(long userID, long itemID)
            throws TasteException {
        SearchResponse response;
        try {
            response = client
                    .prepareSearch(index)
                    .setTypes(preferenceType)
                    .setSearchType(SearchType.SCAN)
                    .setScroll(scrollKeepAlive)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.termQuery(
                                                    itemIDField, itemID))
                                            .must(QueryBuilders.termQuery(
                                                    userIDField, userID)),
                                    getLastAccessedFilterQuery()))
                    .addFields(valueField)
                    .addSort(timestampField, SortOrder.DESC).setSize(1)
                    .execute().actionGet();
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to get the preference by ("
                    + userID + "," + itemID + ")", e);
        }

        SearchHits hits = response.getHits();
        long totalHits = hits.getTotalHits();
        if (totalHits == 0) {
            return null;
        } else if (totalHits > 1) {
            logger.warn(
                    "ItemID {} of UserID {} has {} preferences. Use the latest value.",
                    itemID, userID, totalHits);
        }

        SearchHit[] searchHits = hits.getHits();
        if (searchHits.length > 0) {
            SearchHitField result = searchHits[0].field(valueField);
            if (result != null) {
                return result.getValue();
            }
        }

        return null;
    }

    @Override
    public Long getPreferenceTime(long userID, long itemID)
            throws TasteException {
        SearchResponse response;
        try {
            response = client
                    .prepareSearch(index)
                    .setTypes(preferenceType)
                    .setSearchType(SearchType.SCAN)
                    .setScroll(scrollKeepAlive)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.termQuery(
                                                    itemIDField, itemID))
                                            .must(QueryBuilders.termQuery(
                                                    userIDField, userID)),
                                    getLastAccessedFilterQuery()))
                    .addFields(timestampField)
                    .addSort(timestampField, SortOrder.DESC).setSize(1)
                    .execute().actionGet();
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to get the timestamp by ("
                    + userID + "," + itemID + ")", e);
        }

        SearchHits hits = response.getHits();
        long totalHits = hits.getTotalHits();
        if (totalHits == 0) {
            return null;
        } else if (totalHits > 1) {
            logger.warn(
                    "ItemID {} of UserID {} has {} preferences. Use the latest value.",
                    itemID, userID, totalHits);
        }

        SearchHit[] searchHits = hits.getHits();
        if (searchHits.length > 0) {
            SearchHitField result = searchHits[0].field(timestampField);
            if (result != null) {
                Date date = result.getValue();
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
    public int getNumUsersWithPreferenceFor(long itemID) throws TasteException {
        return getNumByQuery(userType,
                QueryBuilders.termQuery(itemIDField, itemID));
    }

    @Override
    public int getNumUsersWithPreferenceFor(long itemID1, long itemID2)
            throws TasteException {
        return getNumByQuery(
                userType,
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(itemIDField, itemID1))
                        .must(QueryBuilders.termQuery(itemIDField, itemID2)));
    }

    @Override
    public void setPreference(long userID, long itemID, float value)
            throws TasteException {
        Map<String, Object> source = new HashMap<String, Object>();
        source.put(userIDField, userID);
        source.put(itemIDField, itemID);
        source.put(valueField, value);
        source.put(timestampField, "now");
        try {
            client.prepareIndex(index, preferenceType).setSource(source)
                    .setRefresh(true).execute().actionGet();
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to set (" + userID + "," + itemID
                    + "," + value + ")", e);
        }
    }

    @Override
    public void removePreference(long userID, long itemID)
            throws TasteException {
        DeleteByQueryResponse response;
        try {
            response = client
                    .prepareDeleteByQuery(index)
                    .setTypes(preferenceType)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.termQuery(
                                                    userIDField, userID))
                                            .must(QueryBuilders.termQuery(
                                                    itemIDField, itemID)),
                                    getLastAccessedFilterQuery())).execute()
                    .actionGet();
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to remove the preference by ("
                    + userID + "," + itemID + ")", e);
        }
        for (IndexDeleteByQueryResponse res : response) {
            int totalShards = res.getTotalShards();
            int successfulShards = res.getSuccessfulShards();
            if (totalShards != successfulShards) {
                throw new TasteException((totalShards - successfulShards)
                        + " shards are failed.");
            }
            ShardOperationFailedException[] failures = res.getFailures();
            if (failures.length > 0) {
                StringBuilder buf = new StringBuilder();
                for (ShardOperationFailedException failure : failures) {
                    buf.append('\n').append(failure.toString());
                }
                throw new TasteException("Search Operation Failed: "
                        + buf.toString());
            }
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

    protected SearchResponse getPreferenceSearchResponse(String targetField,
            long targetID, String... resultFields) throws TasteException {
        try {
            return client
                    .prepareSearch(index)
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
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to get the preference by "
                    + targetField + ":" + targetID, e);
        }
    }

    protected long getLongValue(SearchHit hit, String field)
            throws TasteException {
        SearchHitField result = hit.field(field);
        if (result == null) {
            throw new TasteException(field + " is not found.");
        }
        Long longValue = result.getValue();
        if (longValue == null) {
            throw new TasteException("The result of " + field + " is null.");
        }
        return longValue.longValue();
    }

    protected float getFloatValue(SearchHit hit, String field)
            throws TasteException {
        SearchHitField result = hit.field(field);
        if (result == null) {
            throw new TasteException(field + " is not found.");
        }
        Float floatValue = result.getValue();
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
                    .prepareSearch(index)
                    .setTypes(userType)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.matchAllQuery(),
                                    getLastAccessedFilterQuery()))
                    .addFields(userIDField).addSort(userIDField, SortOrder.ASC)
                    .setSize(scrollSize).execute().actionGet();
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to load userIDs.", e);
        }

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("The number of users is {} > {}.", totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        int size = (int) totalHits;
        long[] ids = new long[size];
        int index = 0;
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (SearchHit hit : response.getHits()) {
                    ids[index] = getLongValue(hit, userIDField);
                    index++;
                }

                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
        } catch (ElasticsearchException e) {
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
        if (userIDs != null) {
            return;
        }

        SearchResponse response;
        try {
            response = client
                    .prepareSearch(index)
                    .setTypes(itemType)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.matchAllQuery(),
                                    getLastAccessedFilterQuery()))
                    .addFields(itemIDField).addSort(itemIDField, SortOrder.ASC)
                    .setSize(scrollSize).execute().actionGet();
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to load itemIDs.", e);
        }

        long totalHits = response.getHits().getTotalHits();
        if (totalHits > Integer.MAX_VALUE) {
            logger.warn("The number of items is {} > {}.", totalHits,
                    Integer.MAX_VALUE);
            totalHits = Integer.MAX_VALUE;
        }

        int size = (int) totalHits;
        long[] ids = new long[size];
        int index = 0;
        try {
            while (true) {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(scrollKeepAlive).execute().actionGet();
                for (SearchHit hit : response.getHits()) {
                    ids[index] = getLongValue(hit, itemIDField);
                    index++;
                }

                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to scroll the result by itemIDs.",
                    e);
        }

        if (index != size) {
            throw new TasteException("The total size " + size
                    + " and the result " + index + " are not matched");
        }
        userIDs = ids;
    }

    private RangeFilterBuilder getLastAccessedFilterQuery() {
        return FilterBuilders.rangeFilter(timestampField).to(lastAccessed);
    }

    protected int getNumByQuery(String type, QueryBuilder query)
            throws TasteException {
        CountResponse response;
        try {
            response = client
                    .prepareCount(index)
                    .setTypes(type)
                    .setQuery(
                            QueryBuilders.filteredQuery(query,
                                    getLastAccessedFilterQuery())).execute()
                    .actionGet();
        } catch (ElasticsearchException e) {
            throw new TasteException("Failed to count by " + query, e);
        }
        int totalShards = response.getTotalShards();
        int successfulShards = response.getSuccessfulShards();
        if (totalShards != successfulShards) {
            throw new TasteException((totalShards - successfulShards)
                    + " shards are failed.");
        }
        ShardOperationFailedException[] failures = response.getShardFailures();
        if (failures.length > 0) {
            StringBuilder buf = new StringBuilder();
            for (ShardOperationFailedException failure : failures) {
                buf.append('\n').append(failure.toString());
            }
            throw new TasteException("Search Operation Failed: "
                    + buf.toString());
        }
        long count = response.getCount();
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
        SearchResponse response = client
                .prepareSearch(index)
                .setTypes(preferenceType)
                .setQuery(
                        QueryBuilders.filteredQuery(
                                QueryBuilders.matchAllQuery(),
                                getLastAccessedFilterQuery()))
                .setSize(0)
                .addAggregation(
                        AggregationBuilders.stats(valueField).field(valueField))
                .execute().actionGet();
        Aggregations aggregations = response.getAggregations();
        stats = aggregations.get(valueField);
    }
}
