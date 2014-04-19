package org.codelibs.elasticsearch.taste;

import java.util.Collection;

import org.codelibs.elasticsearch.taste.module.TasteModule;
import org.codelibs.elasticsearch.taste.module.TasteRiverModule;
import org.codelibs.elasticsearch.taste.rest.TasteEventRestAction;
import org.codelibs.elasticsearch.taste.service.PrecomputeService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversModule;

public class TastePlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "TastePlugin";
    }

    @Override
    public String description() {
        return "This is a elasticsearch-taste plugin.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(TasteEventRestAction.class);
    }

    // for River
    public void onModule(final RiversModule module) {
        module.registerRiver("taste", TasteRiverModule.class);
    }

    // for Service
    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(TasteModule.class);
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists
                .newArrayList();
        services.add(PrecomputeService.class);
        return services;
    }
}
