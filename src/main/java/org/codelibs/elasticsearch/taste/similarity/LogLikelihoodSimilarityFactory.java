package org.codelibs.elasticsearch.taste.similarity;

import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class LogLikelihoodSimilarityFactory extends
        AbstractUserSimilarityFactory {

    @Override
    public UserSimilarity create() {
        return new LogLikelihoodSimilarity(dataModel);
    }

}
