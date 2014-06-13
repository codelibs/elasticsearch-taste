package org.codelibs.elasticsearch.taste.river.handler;

import java.util.Map;

import org.codelibs.elasticsearch.util.settings.SettingsUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.river.RiverSettings;

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
}