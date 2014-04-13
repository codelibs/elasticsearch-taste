package org.codelibs.elasticsearch.rcmd.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class RecommenderService extends
        AbstractLifecycleComponent<RecommenderService> {

    @Inject
    public RecommenderService(final Settings settings) {
        super(settings);
        logger.info("CREATE RecommenderService");

        // TODO Your code..
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("START RecommenderService");

        // TODO Your code..
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("STOP RecommenderService");

        // TODO Your code..
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("CLOSE RecommenderService");

        // TODO Your code..
    }

}
