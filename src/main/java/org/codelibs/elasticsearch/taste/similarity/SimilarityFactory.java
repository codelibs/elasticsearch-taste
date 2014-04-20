package org.codelibs.elasticsearch.taste.similarity;

import java.util.Map;

public interface SimilarityFactory<T> {

    void init(Map<String, Object> settings);

    T create();

}