package org.codelibs.elasticsearch.taste.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.codelibs.elasticsearch.taste.TasteConstants;
import org.elasticsearch.client.Client;

public class ItemWriter extends ObjectWriter {

    protected String targetIdField;

    protected String itemIdField = TasteConstants.ITEM_ID_FIELD;

    protected String valueField = TasteConstants.VALUE_FIELD;

    protected String itemsField = TasteConstants.ITEMS_FILED;

    public ItemWriter(final Client client, final String index,
            final String type, final String targetIdField) {
        super(client, index, type);
        this.targetIdField = targetIdField;
    }

    public void write(final long userID,
            final List<RecommendedItem> recommendedItems) {
        final Map<String, Object> rootObj = new HashMap<>();
        rootObj.put(targetIdField, userID);
        final List<Map<String, Object>> itemList = new ArrayList<>();
        for (final RecommendedItem recommendedItem : recommendedItems) {
            final Map<String, Object> item = new HashMap<>();
            item.put(itemIdField, recommendedItem.getItemID());
            item.put(valueField, recommendedItem.getValue());
            itemList.add(item);
        }
        rootObj.put(itemsField, itemList);

        write(rootObj);

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

}
