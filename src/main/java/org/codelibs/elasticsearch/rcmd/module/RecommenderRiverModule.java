package org.codelibs.elasticsearch.rcmd.module;

import org.codelibs.elasticsearch.rcmd.river.RecommenderRiver;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class RecommenderRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(RecommenderRiver.class).asEagerSingleton();
    }
}
