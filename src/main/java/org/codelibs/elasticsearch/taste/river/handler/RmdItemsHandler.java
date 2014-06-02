package org.codelibs.elasticsearch.taste.river.handler;

import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.codelibs.elasticsearch.util.SettingsUtils;
import org.codelibs.elasticsearch.util.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.Scroll;

public abstract class RmdItemsHandler extends ActionHandler {

    protected TasteService tasteService;

    public RmdItemsHandler(final RiverSettings settings, final Client client,
            final TasteService tasteService) {
        super(settings, client);
        this.tasteService = tasteService;
    }

    /* (non-Javadoc)
     * @see org.codelibs.elasticsearch.taste.river.handler.ActionHandler#execute()
     */
    @Override
    public abstract void execute();

    protected void waitForClusterStatus(final String... indices) {
        final ClusterHealthResponse response = client.admin().cluster()
                .prepareHealth(indices).setWaitForYellowStatus().execute()
                .actionGet();
        final List<String> failures = response.getAllValidationFailures();
        if (!failures.isEmpty()) {
            throw new TasteSystemException("Cluster is not available: "
                    + failures.toString());
        }
    }

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
            model.setScrollSize(SettingsUtils.get(scrollSettings, "size", 1000));
            model.setScrollKeepAlive(new Scroll(TimeValue
                    .timeValueSeconds(SettingsUtils.get(scrollSettings,
                            "keep_alive", 60))));

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

    protected int getDegreeOfParallelism() {
        int degreeOfParallelism = Runtime.getRuntime().availableProcessors() - 1;
        if (degreeOfParallelism < 1) {
            degreeOfParallelism = 1;
        }
        return degreeOfParallelism;
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
