package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Map;

import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.codelibs.elasticsearch.util.settings.SettingsUtils;

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
