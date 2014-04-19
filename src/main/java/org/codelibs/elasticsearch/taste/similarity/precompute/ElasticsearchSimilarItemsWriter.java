package org.codelibs.elasticsearch.taste.similarity.precompute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.similarity.precompute.SimilarItem;
import org.apache.mahout.cf.taste.similarity.precompute.SimilarItems;
import org.apache.mahout.cf.taste.similarity.precompute.SimilarItemsWriter;
import org.codelibs.elasticsearch.taste.TasteConstants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticsearchSimilarItemsWriter implements SimilarItemsWriter {

    private static final ESLogger logger = Loggers
            .getLogger(ElasticsearchSimilarItemsWriter.class);

    protected Client client;

    protected String index;

    protected String similarityType = TasteConstants.ITEM_SIMILARITY_TYPE;

    protected String userIDField = TasteConstants.USER_ID_FIELD;

    protected String itemIDField = TasteConstants.ITEM_ID_FIELD;

    protected String valueField = TasteConstants.VALUE_FIELD;

    public ElasticsearchSimilarItemsWriter(final Client client,
            final String index) {
        this.client = client;
        this.index = index;
    }

    @Override
    public void open() throws IOException {
        // nothing
    }

    @Override
    public void close() throws IOException {
        // nothing
    }

    @Override
    public void add(final SimilarItems similarItems) throws IOException {
        final Map<String, Object> rootObj = new HashMap<>();
        rootObj.put(TasteConstants.ITEM_ID_FIELD, similarItems.getItemID());
        final List<Map<String, Object>> itemList = new ArrayList<>();
        for (final SimilarItem similarItem : similarItems.getSimilarItems()) {
            final Map<String, Object> item = new HashMap<>();
            item.put(TasteConstants.ITEM_ID_FIELD, similarItem.getItemID());
            item.put(TasteConstants.VALUE_FIELD, similarItem.getSimilarity());
            itemList.add(item);
        }
        rootObj.put("items", itemList);

        client.prepareIndex(index, similarityType,
                Long.toString(similarItems.getItemID())).setSource(rootObj)
                .execute(new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse response) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Create Response: ", response);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        logger.error("Failed to write " + rootObj, e);
                    }
                });
    }

}
