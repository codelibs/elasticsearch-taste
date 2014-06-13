package org.codelibs.elasticsearch.taste.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class TasteService extends AbstractLifecycleComponent<TasteService> {

    @Inject
    public TasteService(final Settings settings) {
        super(settings);
        logger.info("CREATE TasteService");
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("START TasteService");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("STOP TasteService");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("CLOSE TasteService");
    }

}
