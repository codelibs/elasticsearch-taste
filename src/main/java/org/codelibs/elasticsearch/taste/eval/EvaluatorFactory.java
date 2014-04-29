package org.codelibs.elasticsearch.taste.eval;

import java.util.Map;

public interface EvaluatorFactory {
    void init(Map<String, Object> settings);

    Evaluator create();
}
