package org.codelibs.elasticsearch.taste.similarity;

import java.util.Map;

import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public interface UserSimilarityFactory {

    void init(Map<String, Object> settings);

    UserSimilarity create();

}