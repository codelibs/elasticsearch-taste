package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Map;

import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.util.settings.SettingsUtils;

public class NearestNUserNeighborhoodFactory extends
AbstractUserNeighborhoodFactory {

    protected int neighborhoodSize;

    protected double minSimilarity;

    @Override
    public void init(final Map<String, Object> settings) {
        super.init(settings);
        neighborhoodSize = SettingsUtils.get(settings, "neighborhood_size", 10);
        minSimilarity = SettingsUtils.get(settings, "min_similarity",
                Double.NEGATIVE_INFINITY);
    }

    @Override
    public UserNeighborhood create() {
        try {
            return new NearestNUserNeighborhood(neighborhoodSize,
                    minSimilarity, userSimilarity, dataModel);
        } catch (final  Exception e) {
            throw new TasteException("Failed to create an instance.", e);
        }
    }
}
