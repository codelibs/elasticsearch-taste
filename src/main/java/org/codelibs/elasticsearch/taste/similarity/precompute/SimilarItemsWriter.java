package org.codelibs.elasticsearch.taste.similarity.precompute;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class SimilarItemsWriter implements Closeable {

    private static final ESLogger logger = Loggers
            .getLogger(SimilarItemsWriter.class);

    protected Client client;

    protected String index;

    protected String type = TasteConstants.ITEM_SIMILARITY_TYPE;

    protected String itemIdField = TasteConstants.ITEM_ID_FIELD;

    protected String valueField = TasteConstants.VALUE_FIELD;

    protected String itemsField = TasteConstants.ITEMS_FILED;

    protected String timestampField = TasteConstants.TIMESTAMP_FIELD;

    public SimilarItemsWriter(final Client client, final String index) {
        this.client = client;
        this.index = index;
    }

    public void open() {
        final GetMappingsResponse response = client.admin().indices()
                .prepareGetMappings(index).setTypes(type).execute().actionGet();
        if (response.mappings().isEmpty()) {
            try {
                final XContentBuilder builder = XContentFactory.jsonBuilder()//
                        .startObject()//
                        .startObject(type)//
                        .startObject("properties")//

                        // @timestamp
                        .startObject(timestampField)//
                        .field("type", "date")//
                        .field("format", "dateOptionalTime")//
                        .endObject()//

                        // item_id
                        .startObject(itemIdField)//
                        .field("type", "long")//
                        .endObject()//

                        // items
                        .startObject(itemsField)//
                        .startObject("properties")//

                        // item_id
                        .startObject(itemIdField)//
                        .field("type", "long")//
                        .endObject()//

                        // value
                        .startObject(valueField)//
                        .field("type", "double")//
                        .endObject()//

                        .endObject()//
                        .endObject()//

                        .endObject()//
                        .endObject()//
                        .endObject();

                final PutMappingResponse putMappingResponse = client.admin()
                        .indices().preparePutMapping(index).setType(type)
                        .setSource(builder).execute().actionGet();
                if (!putMappingResponse.isAcknowledged()) {
                    throw new TasteSystemException(
                            "Failed to create a mapping of" + index + "/"
                                    + type);
                }
            } catch (final IOException e) {
                throw new TasteSystemException("Failed to create a mapping of"
                        + index + "/" + type, e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // nothing
    }

    public void write(final long itemId,
            final List<RecommendedItem> recommendedItems) {
        final Map<String, Object> rootObj = new HashMap<>();
        rootObj.put(itemIdField, itemId);
        final List<Map<String, Object>> itemList = new ArrayList<>();
        for (final RecommendedItem recommendedItem : recommendedItems) {
            final Map<String, Object> item = new HashMap<>();
            item.put(itemIdField, recommendedItem.getItemID());
            item.put(valueField, recommendedItem.getValue());
            itemList.add(item);
        }
        rootObj.put(itemsField, itemList);
        rootObj.put(timestampField, new Date());

        client.prepareIndex(index, type, Long.toString(itemId))
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

    public void setItemIdField(final String itemIdField) {
        this.itemIdField = itemIdField;
    }

    public void setValueField(final String valueField) {
        this.valueField = valueField;
    }

    public void setItemsField(final String itemsField) {
        this.itemsField = itemsField;
    }

    public void setTimestampField(final String timestampField) {
        this.timestampField = timestampField;
    }

}
