package org.codelibs.elasticsearch.taste.river;

import org.codelibs.elasticsearch.taste.service.PrecomputeService;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class TasteRiver extends AbstractRiverComponent implements River {
    private final Client client;

    private TasteRiverLogic riverLogic;

    @Inject
    public TasteRiver(final RiverName riverName, final RiverSettings settings,
            final Client client, final PrecomputeService precomputeService) {
        super(riverName, settings);
        this.client = client;
        logger.info("CREATE TasteRiver");

        // TODO Your code..

    }

    @Override
    public void start() {
        logger.info("START TasteRiver");

        riverLogic = new TasteRiverLogic();
        new Thread(riverLogic).start();

    }

    @Override
    public void close() {
        logger.info("CLOSE TasteRiver");

        // TODO Your code..
    }

    private class TasteRiverLogic implements Runnable {

        @Override
        public void run() {
            // TODO

            final DeleteMappingResponse deleteMappingResponse = client.admin()
                    .indices().prepareDeleteMapping("_river")
                    .setType(riverName.name()).execute().actionGet();
            if (deleteMappingResponse.isAcknowledged()) {
                logger.info("Deleted " + riverName.name() + "river.");
            } else {
                logger.warn("Failed to delete " + riverName.name()
                        + ". Response: " + deleteMappingResponse.toString());
            }
        }

    }
}
