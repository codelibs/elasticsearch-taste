package org.codelibs.elasticsearch.taste.similarity;

import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class TanimotoCoefficientSimilarityFactory extends
        AbstractUserSimilarityFactory {

    @Override
    public UserSimilarity create() {
        return new TanimotoCoefficientSimilarity(dataModel);
    }

}
