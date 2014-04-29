package org.codelibs.elasticsearch.taste.eval;

import java.util.Map;

import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;

public class AverageAbsoluteDifferenceRecommenderEvaluatorFactory implements
        RecommenderEvaluatorFactory {

    @Override
    public void init(final Map<String, Object> settings) {
    }

    @Override
    public RecommenderEvaluator create() {
        final AverageAbsoluteDifferenceRecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
        return evaluator;
    }

}
