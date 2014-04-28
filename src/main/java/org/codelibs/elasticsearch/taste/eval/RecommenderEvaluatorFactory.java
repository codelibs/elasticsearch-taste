package org.codelibs.elasticsearch.taste.eval;

import java.util.Map;

import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;

public interface RecommenderEvaluatorFactory {
    void init(Map<String, Object> settings);

    RecommenderEvaluator create();
}
