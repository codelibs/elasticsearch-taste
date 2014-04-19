package org.codelibs.elasticsearch.taste.similarity.precompute;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.codelibs.elasticsearch.taste.TasteConstants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class SimilarItemsWriter implements Closeable {

    private static final ESLogger logger = Loggers
            .getLogger(SimilarItemsWriter.class);

    protected Client client;

    protected String index;

    protected String type = TasteConstants.ITEM_SIMILARITY_TYPE;

    protected String itemIDField = TasteConstants.ITEM_ID_FIELD;

    protected String valueField = TasteConstants.VALUE_FIELD;

    protected String itemsField = TasteConstants.ITEMS_FILED;

    public SimilarItemsWriter(final Client client, final String index) {
        this.client = client;
        this.index = index;
    }

    @Override
    public void close() throws IOException {
        // nothing
    }

    public void write(final long itemID,
            final List<RecommendedItem> recommendedItems) {
        final Map<String, Object> rootObj = new HashMap<>();
        rootObj.put(itemIDField, itemID);
        final List<Map<String, Object>> itemList = new ArrayList<>();
        for (final RecommendedItem recommendedItem : recommendedItems) {
            final Map<String, Object> item = new HashMap<>();
            item.put(itemIDField, recommendedItem.getItemID());
            item.put(valueField, recommendedItem.getValue());
            itemList.add(item);
        }
        rootObj.put(itemsField, itemList);

        client.prepareIndex(index, type, Long.toString(itemID))
                .setSource(rootObj)
                .execute(new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse response) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Response: ", response);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        logger.error("Failed to write " + rootObj, e);
                    }
                });
    }

    public void setType(final String type) {
        this.type = type;
    }

    public void setItemIDField(final String itemIDField) {
        this.itemIDField = itemIDField;
    }

    public void setValueField(final String valueField) {
        this.valueField = valueField;
    }

    public void setItemsField(final String itemsField) {
        this.itemsField = itemsField;
    }

}
