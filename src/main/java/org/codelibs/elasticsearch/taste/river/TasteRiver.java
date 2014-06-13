package org.codelibs.elasticsearch.taste.river;

import java.util.Map;

import org.codelibs.elasticsearch.taste.river.handler.ActionHandler;
import org.codelibs.elasticsearch.taste.river.handler.EvalItemsFromUserHandler;
import org.codelibs.elasticsearch.taste.river.handler.GenTermValuesHandler;
import org.codelibs.elasticsearch.taste.river.handler.ItemsFromItemHandler;
import org.codelibs.elasticsearch.taste.river.handler.ItemsFromUserHandler;
import org.codelibs.elasticsearch.taste.river.handler.SimilarUsersHandler;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.codelibs.elasticsearch.util.river.RiverUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class TasteRiver extends AbstractRiverComponent implements River {
    private static final String RIVER_THREAD_NAME_PREFIX = "TasteRiver-";

    private static final String GENERATE_TERM_VALUES = "generate_term_values";

    private static final String EVALUATE_ITEMS_FROM_USER = "evaluate_items_from_user";

    private static final String RECOMMENDED_ITEMS_FROM_ITEM = "recommended_items_from_item";

    private static final String RECOMMENDED_ITEMS_FROM_USER = "recommended_items_from_user";

    private static final String SIMILAR_USERS = "similar_users";

    private final Client client;

    private TasteService tasteService;

    private Thread riverThread;

    @Inject
    public TasteRiver(final RiverName riverName, final RiverSettings settings,
            final Client client, final TasteService tasteService) {
        super(riverName, settings);
        this.client = client;
        this.tasteService = tasteService;

        logger.info("CREATE TasteRiver");
    }

    @Override
    public void start() {
        logger.info("START TasteRiver");
        try {
            final Map<String, Object> rootSettings = settings.settings();
            final Object actionObj = rootSettings.get("action");
            if (RECOMMENDED_ITEMS_FROM_USER.equals(actionObj)) {
                final ItemsFromUserHandler handler = new ItemsFromUserHandler(
                        settings, client, tasteService);
                startRiverThread(handler);
            } else if (RECOMMENDED_ITEMS_FROM_ITEM.equals(actionObj)) {
                final ItemsFromItemHandler handler = new ItemsFromItemHandler(
                        settings, client, tasteService);
                startRiverThread(handler);
            } else if (SIMILAR_USERS.equals(actionObj)) {
                final SimilarUsersHandler handler = new SimilarUsersHandler(
                        settings, client, tasteService);
                startRiverThread(handler);
            } else if (EVALUATE_ITEMS_FROM_USER.equals(actionObj)) {
                final EvalItemsFromUserHandler handler = new EvalItemsFromUserHandler(
                        settings, client, tasteService);
                startRiverThread(handler);
            } else if (GENERATE_TERM_VALUES.equals(actionObj)) {
                final GenTermValuesHandler handler = new GenTermValuesHandler(
                        settings, client);
                startRiverThread(handler);
            } else {
                logger.info("River {} has no actions. Deleting...",
                        riverName.name());
            }
        } finally {
            if (riverThread == null) {
                try {
                    RiverUtils.delete(client, riverName);
                    logger.info("Deleted " + riverName.name() + "river.");
                } catch (final Exception e) {
                    logger.warn("Failed to delete " + riverName.name(), e);
                }
            }
        }
    }

    protected void startRiverThread(final ActionHandler handler) {
        final String name = RIVER_THREAD_NAME_PREFIX + riverName.name();
        riverThread = new Thread(() -> {
            try {
                handler.execute();
            } catch (final Exception e) {
                logger.error("River {} is failed.", e, riverName.name(), e);
            } finally {
                try {
                    RiverUtils.delete(client, riverName);
                    logger.info("Deleted " + riverName.name() + "river.");
                } catch (final Exception e) {
                    logger.warn("Failed to delete " + riverName.name(), e);
                } finally {
                    handler.close();
                }
            }
        }, name);
        try {
            riverThread.start();
        } catch (final Exception e) {
            logger.error("Failed to start {}.", e, name);
            riverThread = null;
        }
    }

    @Override
    public void close() {
        logger.info("CLOSE TasteRiver");
        if (riverThread != null) {
            riverThread.interrupt();
            riverThread = null;
        }
    }

}
