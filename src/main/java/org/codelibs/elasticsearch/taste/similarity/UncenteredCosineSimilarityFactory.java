package org.codelibs.elasticsearch.taste.similarity;

import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.util.SettingsUtils;

public class UncenteredCosineSimilarityFactory extends
        AbstractUserSimilarityFactory {

    protected Weighting weighting;

    @Override
    public void init(final Map<String, Object> settings) {
        super.init(settings);
        final String value = SettingsUtils.get(settings, "weighting");
        if ("WEIGHTED".equalsIgnoreCase(value)) {
            weighting = Weighting.WEIGHTED;
        } else {
            weighting = Weighting.UNWEIGHTED;
        }
    }

    @Override
    public UserSimilarity create() {
        try {
            return new UncenteredCosineSimilarity(dataModel, weighting);
        } catch (final TasteException e) {
            throw new TasteSystemException("Failed to create an instance.", e);
        }
    }

}
