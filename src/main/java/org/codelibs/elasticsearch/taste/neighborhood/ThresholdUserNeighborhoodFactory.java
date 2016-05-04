package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Map;

import org.codelibs.elasticsearch.taste.util.SettingsUtils;


public class ThresholdUserNeighborhoodFactory extends
        AbstractUserNeighborhoodFactory {
    protected double threshold;

    @Override
    public void init(final Map<String, Object> settings) {
        super.init(settings);
        threshold = SettingsUtils.get(settings, "threshold", Double.NaN);
    }

    @Override
    public UserNeighborhood create() {
        return new ThresholdUserNeighborhood(threshold, userSimilarity,
                dataModel);
    }

}
