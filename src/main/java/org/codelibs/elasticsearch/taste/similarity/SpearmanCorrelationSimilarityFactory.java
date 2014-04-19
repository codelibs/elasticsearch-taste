package org.codelibs.elasticsearch.taste.similarity;

import org.apache.mahout.cf.taste.impl.similarity.SpearmanCorrelationSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class SpearmanCorrelationSimilarityFactory extends
        AbstractUserSimilarityFactory {

    @Override
    public UserSimilarity create() {
        return new SpearmanCorrelationSimilarity(dataModel);
    }

}
