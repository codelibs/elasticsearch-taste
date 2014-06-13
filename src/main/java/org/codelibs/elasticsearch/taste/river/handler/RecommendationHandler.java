package org.codelibs.elasticsearch.taste.river.handler;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;

import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.codelibs.elasticsearch.util.lang.StringUtils;
import org.codelibs.elasticsearch.util.settings.SettingsUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.Scroll;

public abstract class RecommendationHandler extends ActionHandler {

    protected TasteService tasteService;

    public RecommendationHandler(final RiverSettings settings,
            final Client client, final TasteService tasteService) {
        super(settings, client);
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
            throw new TasteSystemException("User Index is blank.");
        }
        if (StringUtils.isBlank(indexInfo.getPreferenceIndex())) {
            throw new TasteSystemException("Preference Index is blank.");
        }
        if (StringUtils.isBlank(indexInfo.getItemIndex())) {
            throw new TasteSystemException("Item Index is blank.");
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
                model.setUserQueryBuilder(QueryBuilders.queryString(userQuery));
            }
            final String itemQuery = SettingsUtils.get(querySettings, "item");
            if (StringUtils.isNotBlank(itemQuery)) {
                model.setUserQueryBuilder(QueryBuilders.queryString(itemQuery));
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
            return model;
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException e) {
            throw new TasteSystemException("Could not create an instance of "
                    + className);
        }
    }

    protected void waitForTasks(final ForkJoinTask<?>[] tasks,
            final int maxDuration) {
        final long endTime = maxDuration > 0 ? System.currentTimeMillis()
                + maxDuration * 60 * 1000 : 0;
        try {
            Arrays.stream(tasks).forEach(
                    task -> {
                        try {
                            if (endTime > 0) {
                                task.get(endTime - System.currentTimeMillis(),
                                        TimeUnit.MILLISECONDS);
                            } else {
                                task.get();
                            }
                        } catch (final Exception e) {
                            throw new TasteSystemException(
                                    "Failed to process tasks.", e);
                        }
                    });
        } catch (final Exception e) {
            logger.warn("Unable to complete the computation.", e);
            Arrays.stream(tasks).forEach(task -> {
                if (!task.isCancelled()) {
                    task.cancel(true);
                }
            });
        }
    }

    private long parseWeight(final String value) {
        if (StringUtils.isBlank(value)) {
            return 0;
        }
        try {
            final char lastChar = value.charAt(value.length() - 1);
            if (lastChar == 'g' || lastChar == 'G') {
                return Long.parseLong(value.substring(0, value.length() - 2));
            } else if (lastChar == 'm' || lastChar == 'M') {
                return Long.parseLong(value.substring(0, value.length() - 2));
            } else if (lastChar == 'k' || lastChar == 'K') {
                return Long.parseLong(value.substring(0, value.length() - 2));
            }
            return Long.parseLong(value);
        } catch (final Exception e) {
            logger.warn("Failed to parse a weight: {}", e, value);
        }
        return 0;
    }
}
