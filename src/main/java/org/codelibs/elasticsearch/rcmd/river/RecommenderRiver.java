package org.codelibs.elasticsearch.rcmd.river;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class RecommenderRiver extends AbstractRiverComponent implements River {
    private final Client client;

    private RecommenderRiverLogic riverLogic;

    @Inject
    public RecommenderRiver(final RiverName riverName,
            final RiverSettings settings, final Client client) {
        super(riverName, settings);
        this.client = client;

        logger.info("CREATE RecommenderRiver");

        // TODO Your code..

    }

    @Override
    public void start() {
        logger.info("START RecommenderRiver");

        riverLogic = new RecommenderRiverLogic();
        new Thread(riverLogic).start();
    }

    @Override
    public void close() {
        logger.info("CLOSE RecommenderRiver");

        // TODO Your code..
    }

    private class RecommenderRiverLogic implements Runnable {

        @Override
        public void run() {
            logger.info("START RecommenderRiverLogic: " + client.toString());

            // TODO Your code..
        }
    }
}
