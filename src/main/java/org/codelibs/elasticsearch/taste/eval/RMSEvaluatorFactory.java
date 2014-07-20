package org.codelibs.elasticsearch.taste.eval;

import java.util.Map;

import org.codelibs.elasticsearch.util.settings.SettingsUtils;

public class RMSEvaluatorFactory implements EvaluatorFactory {
    protected Number maxPreference;

    protected Number minPreference;

    @Override
    public void init(final Map<String, Object> settings) {
        maxPreference = SettingsUtils.get(settings, "max_preference");
        minPreference = SettingsUtils.get(settings, "min_preference");
    }

    @Override
    public Evaluator create() {
        final RMSEvaluator evaluator = new RMSEvaluator();
        if (maxPreference != null) {
            evaluator.setMaxPreference(maxPreference.floatValue());
        }
        if (minPreference != null) {
            evaluator.setMinPreference(minPreference.floatValue());
        }
        return evaluator;
    }

}
