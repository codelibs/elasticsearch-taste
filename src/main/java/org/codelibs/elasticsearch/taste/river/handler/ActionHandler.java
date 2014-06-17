package org.codelibs.elasticsearch.taste.river.handler;

import java.util.Map;

import org.codelibs.elasticsearch.util.lang.StringUtils;
import org.codelibs.elasticsearch.util.settings.SettingsUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

public abstract class ActionHandler {
    protected Client client;

    protected final ESLogger logger;

    protected Map<String, Object> rootSettings;

    protected Settings settings;

    public ActionHandler(final RiverSettings riverSettings, final Client client) {
        this.client = client;
        settings = riverSettings.globalSettings();
        logger = Loggers.getLogger(getClass(), settings);
        rootSettings = riverSettings.settings();
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

        final int count = 0;
        long[] targetIDs = null;
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(keepAlive.longValue()))
                .setQuery(QueryBuilders.queryString(userQuery))
                .addField(fieldName).setSize(size.intValue()).execute()
                .actionGet();
        while (true) {
            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(keepAlive.longValue())).execute()
                    .actionGet();
            final SearchHits hits = response.getHits();
            if (targetIDs == null) {
                targetIDs = new long[(int) hits.getTotalHits()];
            }

            if (hits.getHits().length == 0) {
                break;
            }

            for (final SearchHit hit : hits) {
                final SearchHitField searchHitField = hit.getFields().get(
                        fieldName);
                final Number value = searchHitField.getValue();
                targetIDs[count] = value.longValue();
            }
        }
        return targetIDs;
    }
}