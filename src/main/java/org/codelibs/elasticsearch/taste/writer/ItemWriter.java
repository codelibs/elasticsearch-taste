package org.codelibs.elasticsearch.taste.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.codelibs.elasticsearch.taste.TasteConstants;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.Cache;

public class ItemWriter extends ObjectWriter {

    protected String targetIdField;

    protected String itemIdField = TasteConstants.ITEM_ID_FIELD;

    protected String valueField = TasteConstants.VALUE_FIELD;

    protected String itemsField = TasteConstants.ITEMS_FILED;

    protected boolean verbose = false;

    protected String itemIndex;

    protected String itemType;

    protected Cache<Long, Map<String, Object>> cache;

    public ItemWriter(final Client client, final String index,
            final String type, final String targetIdField) {
        super(client, index, type);
        this.targetIdField = targetIdField;
    }

    public void write(final long userID,
            final List<RecommendedItem> recommendedItems) {
        final Map<String, Object> rootObj = new HashMap<>();
        rootObj.put(targetIdField, userID);
        if (verbose) {
            final GetResponse response = client
                    .prepareGet(index, type, Long.toString(userID)).execute()
                    .actionGet();
            if (response.isExists()) {
                final Map<String, Object> map = response.getSourceAsMap();
                map.remove(targetIdField);
                rootObj.putAll(map);
            }
        }
        final List<Map<String, Object>> itemList = new ArrayList<>();
        for (final RecommendedItem recommendedItem : recommendedItems) {
            final Map<String, Object> item = new HashMap<>();
            item.put(itemIdField, recommendedItem.getItemID());
            item.put(valueField, recommendedItem.getValue());
            if (verbose) {
                final Map<String, Object> map = getItemMap(recommendedItem
                        .getItemID());
                if (map != null) {
                    item.putAll(map);
                }
            }
            itemList.add(item);
        }
        rootObj.put(itemsField, itemList);

        write(rootObj);

    }

    protected Map<String, Object> getItemMap(final long itemID) {
        if (cache != null) {
            final Map<String, Object> map = cache.getIfPresent(itemID);
            if (map != null) {
                return map;
            }
        }
        final GetResponse response = client
                .prepareGet(itemIndex, itemType, Long.toString(itemID))
                .execute().actionGet();
        if (response.isExists()) {
            final Map<String, Object> map = response.getSourceAsMap();
            map.remove(itemIdField);
            map.remove(valueField);
            cache.put(itemID, map);
            return map;
        }
        return null;
    }

    public void setItemIdField(final String itemIdField) {
        this.itemIdField = itemIdField;
    }

    public void setValueField(final String valueField) {
        this.valueField = valueField;
    }

    public void setItemsField(final String itemsField) {
        this.itemsField = itemsField;
    }

    public void setItemIndex(final String itemIndex) {
        this.itemIndex = itemIndex;
    }

    public void setItemType(final String itemType) {
        this.itemType = itemType;
    }

    public void setCache(final Cache<Long, Map<String, Object>> cache) {
        this.cache = cache;
    }

    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

}
