package org.codelibs.elasticsearch.rcmd.module;

import org.codelibs.elasticsearch.rcmd.service.RecommenderService;
import org.elasticsearch.common.inject.AbstractModule;

public class RecommenderModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RecommenderService.class).asEagerSingleton();
    }
}