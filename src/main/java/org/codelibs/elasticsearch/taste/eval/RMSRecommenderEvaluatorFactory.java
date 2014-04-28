package org.codelibs.elasticsearch.taste.eval;

import java.util.Map;

import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.codelibs.elasticsearch.taste.eval.impl.RMSRecommenderEvaluator;
import org.codelibs.elasticsearch.util.SettingsUtils;

public class RMSRecommenderEvaluatorFactory implements
        RecommenderEvaluatorFactory {
    protected Number maxPreference;

    protected Number minPreference;

    @Override
    public void init(final Map<String, Object> settings) {
        maxPreference = SettingsUtils.get(settings, "max_preference");
        minPreference = SettingsUtils.get(settings, "min_preference");
    }

    @Override
    public RecommenderEvaluator create() {
        final RMSRecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
        if (maxPreference != null) {
            evaluator.setMaxPreference(maxPreference.floatValue());
        }
        if (minPreference != null) {
            evaluator.setMinPreference(minPreference.floatValue());
        }
        return evaluator;
    }

}
