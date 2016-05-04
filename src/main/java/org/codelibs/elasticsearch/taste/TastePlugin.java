package org.codelibs.elasticsearch.taste;

import java.util.Collection;

import org.codelibs.elasticsearch.taste.module.TasteModule;
import org.codelibs.elasticsearch.taste.rest.TasteActionRestAction;
import org.codelibs.elasticsearch.taste.rest.TasteEventRestAction;
import org.codelibs.elasticsearch.taste.rest.TasteSearchRestAction;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

import com.google.common.collect.Lists;

public class TastePlugin extends Plugin {
    @Override
    public String name() {
        return "taste";
    }

    @Override
    public String description() {
        return "Taste plugin recommends items from data in indices.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(TasteEventRestAction.class);
        module.addRestAction(TasteSearchRestAction.class);
        module.addRestAction(TasteActionRestAction.class);
    }

    // for Service
    @Override
    public Collection<Module> nodeModules() {
        final Collection<Module> modules = Lists.newArrayList();
        modules.add(new TasteModule());
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists
                .newArrayList();
        services.add(TasteService.class);
        return services;
    }
}
