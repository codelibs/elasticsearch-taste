package org.codelibs.elasticsearch.taste.similarity;

import java.util.Map;

import org.codelibs.elasticsearch.taste.common.Weighting;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.util.settings.SettingsUtils;

public class EuclideanDistanceSimilarityFactory<T> extends
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
            final T t = (T) new EuclideanDistanceSimilarity(dataModel,
                    weighting);
            return t;
        } catch (final  Exception e) {
            throw new TasteException("Failed to create an instance.", e);
        }
    }
}
