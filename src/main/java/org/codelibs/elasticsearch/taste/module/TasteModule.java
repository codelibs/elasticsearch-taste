package org.codelibs.elasticsearch.taste.module;

import org.codelibs.elasticsearch.taste.service.PrecomputeService;
import org.elasticsearch.common.inject.AbstractModule;

public class TasteModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(PrecomputeService.class).asEagerSingleton();
    }
}