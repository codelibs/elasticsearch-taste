package org.codelibs.elasticsearch.taste.river.handler;

import java.io.IOException;
import java.util.Map;

import org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.recommender.ItemBasedRecommenderBuilder;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.codelibs.elasticsearch.taste.writer.ItemWriter;
import org.codelibs.elasticsearch.util.SettingsUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.river.RiverSettings;

public class RmdItemsFromItemHandler extends RmdItemsHandler {
    public RmdItemsFromItemHandler(final RiverSettings settings,
            final Client client, final TasteService tasteService) {
        super(settings, client, tasteService);
    }

    @Override
    public void execute() {
        final int numOfItems = SettingsUtils.get(rootSettings, "num_of_items",
                10);
        final int maxDuration = SettingsUtils.get(rootSettings, "max_duration",
                0);
        final int degreeOfParallelism = getDegreeOfParallelism();

        final Map<String, Object> indexInfoSettings = SettingsUtils.get(
                rootSettings, "index_info");
        final IndexInfo indexInfo = new IndexInfo(indexInfoSettings);

        final Map<String, Object> modelInfoSettings = SettingsUtils.get(
                rootSettings, "data_model");
        final ElasticsearchDataModel dataModel = createDataModel(client,
                indexInfo, modelInfoSettings);

        waitForClusterStatus(indexInfo.getUserIndex(),
                indexInfo.getItemIndex(), indexInfo.getPreferenceIndex(),
                indexInfo.getItemSimilarityIndex());

        final ItemBasedRecommenderBuilder recommenderBuilder = new ItemBasedRecommenderBuilder(
                indexInfo, rootSettings);

        final ItemWriter writer = createSimilarItemsWriter(indexInfo,
                rootSettings);

        tasteService.compute(dataModel, recommenderBuilder, writer, numOfItems,
                degreeOfParallelism, maxDuration);
    }

    protected ItemWriter createSimilarItemsWriter(final IndexInfo indexInfo,
            final Map<String, Object> rootSettings) {
        final ItemWriter writer = new ItemWriter(client,
                indexInfo.getItemSimilarityIndex(),
                indexInfo.getItemSimilarityType(), indexInfo.getItemIdField());
        writer.setItemIdField(indexInfo.getItemIdField());
        writer.setItemsField(indexInfo.getItemsField());
        writer.setValueField(indexInfo.getValueField());
        writer.setTimestampField(indexInfo.getTimestampField());
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(indexInfo.getItemSimilarityType())//
                    .startObject("properties")//

                    // @timestamp
                    .startObject(indexInfo.getTimestampField())//
                    .field("type", "date")//
                    .field("format", "dateOptionalTime")//
                    .endObject()//

                    // user_id
                    .startObject(indexInfo.getItemIdField())//
                    .field("type", "long")//
                    .endObject()//

                    // items
                    .startObject(indexInfo.getItemsField())//
                    .startObject("properties")//

                    // item_id
                    .startObject(indexInfo.getItemIdField())//
                    .field("type", "long")//
                    .endObject()//

                    // value
                    .startObject(indexInfo.getValueField())//
                    .field("type", "double")//
                    .endObject()//

                    .endObject()//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            writer.setMapping(builder);
        } catch (final IOException e) {
            logger.info("Failed to create a mapping {}/{}.", e,
                    indexInfo.getReportIndex(), indexInfo.getReportType());
        }

        final Map<String, Object> writerSettings = SettingsUtils.get(
                rootSettings, "writer");
        final boolean verbose = SettingsUtils.get(writerSettings, "verbose",
                false);
        if (verbose) {
            writer.setVerbose(verbose);
            final int maxCacheSize = SettingsUtils.get(writerSettings,
                    "cache_size", 1000);
            final Cache<Long, Map<String, Object>> cache = CacheBuilder
                    .newBuilder().maximumSize(maxCacheSize).build();
            writer.setCache(cache);
        }

        writer.open();

        return writer;
    }
}
