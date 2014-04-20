package org.codelibs.elasticsearch.taste.similarity;

import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.util.SettingsUtils;

public class UncenteredCosineSimilarityFactory<T> extends
        AbstractUserSimilarityFactory<T> {

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
    public T create() {
        try {
            @SuppressWarnings("unchecked")
            final T t = (T) new UncenteredCosineSimilarity(dataModel, weighting);
            return t;
        } catch (final TasteException e) {
            throw new TasteSystemException("Failed to create an instance.", e);
        }
    }

}
