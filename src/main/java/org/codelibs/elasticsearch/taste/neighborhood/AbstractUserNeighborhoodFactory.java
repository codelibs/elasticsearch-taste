package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Map;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.codelibs.elasticsearch.util.SettingsUtils;

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