package org.codelibs.elasticsearch.taste.module;

import org.codelibs.elasticsearch.taste.river.TastederRiver;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class TasteRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(TastederRiver.class).asEagerSingleton();
    }
}
