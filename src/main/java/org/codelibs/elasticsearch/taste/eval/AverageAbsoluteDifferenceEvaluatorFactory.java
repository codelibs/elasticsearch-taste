package org.codelibs.elasticsearch.taste.eval;

import java.util.Map;

import org.codelibs.elasticsearch.taste.eval.impl.AverageAbsoluteDifferenceEvaluator;

public class AverageAbsoluteDifferenceEvaluatorFactory implements
        EvaluatorFactory {

    @Override
    public void init(final Map<String, Object> settings) {
    }

    @Override
    public Evaluator create() {
        final AverageAbsoluteDifferenceEvaluator evaluator = new AverageAbsoluteDifferenceEvaluator();
        return evaluator;
    }

}
