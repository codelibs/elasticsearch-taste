package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Map;

import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.similarity.UserSimilarity;
import org.codelibs.elasticsearch.taste.util.SettingsUtils;


public abstract class AbstractUserNeighborhoodFactory implements
        UserNeighborhoodFactory {

    protected DataModel dataModel;

    protected UserSimilarity userSimilarity;

    @Override
    public void init(final Map<String, Object> settings) {
        dataModel = SettingsUtils.get(settings, "dataModel");
        userSimilarity = SettingsUtils.get(settings, "userSimilarity");
    }

}