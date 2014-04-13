package org.codelibs.elasticsearch.rcmd;

import java.util.Collection;

import org.codelibs.elasticsearch.rcmd.module.RecommenderModule;
import org.codelibs.elasticsearch.rcmd.module.RecommenderRiverModule;
import org.codelibs.elasticsearch.rcmd.rest.RecommenderRestAction;
import org.codelibs.elasticsearch.rcmd.service.RecommenderService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversModule;

public class RecommenderPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "RecommenderPlugin";
    }

    @Override
    public String description() {
        return "This is a elasticsearch-recommender plugin.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(RecommenderRestAction.class);
    }

    // for River
    public void onModule(final RiversModule module) {
        module.registerRiver("recommender", RecommenderRiverModule.class);
    }

    // for Service
    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(RecommenderModule.class);
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists
                .newArrayList();
        services.add(RecommenderService.class);
        return services;
    }
}
