package org.codelibs.elasticsearch.taste.similarity;

import java.util.Map;

import org.apache.mahout.cf.taste.model.DataModel;
import org.codelibs.elasticsearch.util.SettingsUtils;

public abstract class AbstractUserSimilarityFactory<T> implements
        SimilarityFactory<T> {

    protected DataModel dataModel;

    @Override
    public void init(final Map<String, Object> settings) {
        dataModel = SettingsUtils.get(settings, "dataModel");
    }

}