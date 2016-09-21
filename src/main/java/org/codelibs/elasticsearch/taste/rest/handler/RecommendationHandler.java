package org.codelibs.elasticsearch.taste.rest.handler;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.codelibs.elasticsearch.taste.util.SettingsUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.threadpool.ThreadPool;

public abstract class RecommendationHandler extends ActionHandler {

    protected TasteService tasteService;

    public RecommendationHandler(final Settings settings,
            final Map<String, Object> sourceMap, final Client client, final ThreadPool pool,
            final TasteService tasteService) {
        super(settings, sourceMap, client, pool);
        this.tasteService = tasteService;
    }

    /* (non-Javadoc)
     * @see org.codelibs.elasticsearch.taste.river.handler.ActionHandler#execute()
     */
    @Override
    public abstract void execute();

    protected ElasticsearchDataModel createDataModel(final Client client,
            final IndexInfo indexInfo,
            final Map<String, Object> modelInfoSettings) {
        if (StringUtils.isBlank(indexInfo.getUserIndex())) {
            throw new TasteException("User Index is blank.");
        }
        if (StringUtils.isBlank(indexInfo.getPreferenceIndex())) {
            throw new TasteException("Preference Index is blank.");
        }
        if (StringUtils.isBlank(indexInfo.getItemIndex())) {
            throw new TasteException("Item Index is blank.");
        }

        final String className = SettingsUtils
                .get(modelInfoSettings, "class",
                        "org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel");

        try {
            final Class<?> clazz = Class.forName(className);
            final ElasticsearchDataModel model = (ElasticsearchDataModel) clazz
                    .newInstance();
            model.setClient(client);
            model.setPreferenceIndex(indexInfo.getPreferenceIndex());
            model.setPreferenceType(indexInfo.getPreferenceType());
            model.setUserIndex(indexInfo.getUserIndex());
            model.setUserType(indexInfo.getUserType());
            model.setItemIndex(indexInfo.getItemIndex());
            model.setItemType(indexInfo.getItemType());
            model.setUserIdField(indexInfo.getUserIdField());
            model.setItemIdField(indexInfo.getItemIdField());
            model.setValueField(indexInfo.getValueField());
            model.setTimestampField(indexInfo.getTimestampField());

            final Map<String, Object> scrollSettings = SettingsUtils.get(
                    modelInfoSettings, "scroll");
            model.setScrollSize(((Number) SettingsUtils.get(scrollSettings,
                    "size", 1000)).intValue());
            model.setScrollKeepAlive(new Scroll(TimeValue
                    .timeValueSeconds(((Number) SettingsUtils.get(
                            scrollSettings, "keep_alive", 60L)).longValue())));

            final Map<String, Object> querySettings = SettingsUtils.get(
                    modelInfoSettings, "query");
            final String userQuery = SettingsUtils.get(querySettings, "user");
            if (StringUtils.isNotBlank(userQuery)) {
                model.setUserQueryBuilder(QueryBuilders.queryStringQuery(userQuery));
            }
            final String itemQuery = SettingsUtils.get(querySettings, "item");
            if (StringUtils.isNotBlank(itemQuery)) {
                model.setUserQueryBuilder(QueryBuilders.queryStringQuery(itemQuery));
            }

            final Map<String, Object> cacheSettings = SettingsUtils.get(
                    modelInfoSettings, "cache");
            final Object weight = SettingsUtils.get(cacheSettings, "weight");
            if (weight instanceof Number) {
                model.setMaxCacheWeight(((Integer) weight).longValue());
            } else {
                final long weightSize = parseWeight(weight.toString());
                if (weightSize > 0) {
                    model.setMaxCacheWeight(weightSize);
                }
            }

            final Object maxPreferenceSize = SettingsUtils
                    .get(modelInfoSettings, "max_preference_size");
            if (maxPreferenceSize instanceof Number) {
                int size = ((Number) maxPreferenceSize).intValue();
                if (size > 0) {
                    model.setMaxPreferenceSize(size);
                }
            }

            return model;
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException e) {
            throw new TasteException("Could not create an instance of "
                    + className);
        }
    }

    protected void waitFor(final ExecutorService executorService,
            final int maxDuration) {
        executorService.shutdown();
        boolean succeeded = false;
        try {
            if (maxDuration == 0) {
                succeeded = executorService.awaitTermination(Long.MAX_VALUE,
                        TimeUnit.NANOSECONDS);
            } else {
                succeeded = executorService.awaitTermination(maxDuration,
                        TimeUnit.MINUTES);
            }
            if (!succeeded) {
                logger.warn(
                        "Unable to complete the computation in {} minutes!",
                        maxDuration);
            }
        } catch (final InterruptedException e) {
            logger.warn("Interrupted a executor.", e);
        }
        if (!succeeded) {
            executorService.shutdownNow();
        }
    }

    private long parseWeight(final String value) {
        if (StringUtils.isBlank(value)) {
            return 0;
        }
        try {
            final char lastChar = value.charAt(value.length() - 1);
            if (lastChar == 'g' || lastChar == 'G') {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1000 * 1000 * 1000;
            } else if (lastChar == 'm' || lastChar == 'M') {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1000 * 1000;
            } else if (lastChar == 'k' || lastChar == 'K') {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1000;
            }
            return Long.parseLong(value);
        } catch (final Exception e) {
            logger.warn("Failed to parse a weight: {}", e, value);
        }
        return 0;
    }
}
