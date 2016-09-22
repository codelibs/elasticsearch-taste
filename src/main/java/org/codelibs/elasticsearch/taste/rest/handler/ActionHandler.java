package org.codelibs.elasticsearch.taste.rest.handler;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.codelibs.elasticsearch.taste.util.SettingsUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;

public abstract class ActionHandler {
    protected Client client;

    protected final ESLogger logger;

    protected Map<String, Object> rootSettings;

    protected Settings settings;

    protected ThreadPool pool;

    public ActionHandler(final Settings settings,
            final Map<String, Object> sourceMap, final Client client, final ThreadPool pool) {
        this.client = client;
        this.settings = settings;
        rootSettings = sourceMap;
        this.pool = pool;
        logger = Loggers.getLogger(getClass(), settings);
    }

    public abstract void execute();

    public abstract void close();

    protected int getNumOfThreads() {
        return SettingsUtils.get(rootSettings, "num_of_threads", Runtime
                .getRuntime().availableProcessors());
    }

    protected long[] getTargetIDs(final String index, final String type,
            final String fieldName, final String targetName) {
        final Map<String, Object> userSettings = SettingsUtils.get(
                rootSettings, targetName);
        final String userQuery = SettingsUtils.get(userSettings, "query");
        if (StringUtils.isBlank(userQuery)) {
            return null;
        }
        final Number size = SettingsUtils.get(userSettings, "size", 1000);
        final Number keepAlive = SettingsUtils.get(userSettings, "keep_alive",
                60000); //1min

        int count = 0;
        long[] targetIDs = null;
        SearchResponse response = null;
        while (true) {
            if (response == null) {
                response = client.prepareSearch(index).setTypes(type)
                        .setScroll(new TimeValue(keepAlive.longValue()))
                        .setQuery(QueryBuilders.queryStringQuery(userQuery))
                        .addField(fieldName).setSize(size.intValue()).execute()
                        .actionGet();
            } else {
                response = client.prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(keepAlive.longValue()))
                        .execute().actionGet();
            }
            final SearchHits hits = response.getHits();
            if (targetIDs == null) {
                targetIDs = new long[(int) hits.getTotalHits()];
                if (logger.isDebugEnabled()) {
                    logger.debug("{} users are found by {}",
                            hits.getTotalHits(), userQuery);
                }
            }

            if (hits.getHits().length == 0) {
                break;
            }

            for (final SearchHit hit : hits) {
                final SearchHitField searchHitField = hit.getFields().get(
                        fieldName);
                final Number value = searchHitField.getValue();
                targetIDs[count] = value.longValue();
                count++;
            }
        }
        return targetIDs;
    }
}