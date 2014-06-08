package org.codelibs.elasticsearch.taste.river;

import java.util.Map;

import org.codelibs.elasticsearch.taste.river.handler.ActionHandler;
import org.codelibs.elasticsearch.taste.river.handler.EvalItemsFromUserHandler;
import org.codelibs.elasticsearch.taste.river.handler.GenTermValuesHandler;
import org.codelibs.elasticsearch.taste.river.handler.RmdItemsFromItemHandler;
import org.codelibs.elasticsearch.taste.river.handler.RmdItemsFromUserHandler;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
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
                final RmdItemsFromUserHandler handler = new RmdItemsFromUserHandler(
                        settings, client, tasteService);
                startRiverThread(handler);
            } else if (RECOMMENDED_ITEMS_FROM_ITEM.equals(actionObj)) {
                final RmdItemsFromItemHandler handler = new RmdItemsFromItemHandler(
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
                deleteRiver();
            }
        }
    }

    protected void startRiverThread(final ActionHandler handler) {
        final String name = RIVER_THREAD_NAME_PREFIX + riverName.name();
        riverThread = new Thread((Runnable) () -> {
            try {
                handler.execute();
            } catch (final Exception e) {
                logger.error("River {} is failed.", e, riverName.name());
            } finally {
                deleteRiver();
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

    protected void deleteRiver() {
        final DeleteMappingResponse deleteMappingResponse = client.admin()
                .indices().prepareDeleteMapping("_river")
                .setType(riverName.name()).execute().actionGet();
        if (deleteMappingResponse.isAcknowledged()) {
            logger.info("Deleted " + riverName.name() + "river.");
        } else {
            logger.warn("Failed to delete " + riverName.name() + ". Response: "
                    + deleteMappingResponse.toString());
        }
    }

}
