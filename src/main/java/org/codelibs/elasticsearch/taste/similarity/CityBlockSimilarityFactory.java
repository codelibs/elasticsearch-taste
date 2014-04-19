package org.codelibs.elasticsearch.taste.similarity;

import org.apache.mahout.cf.taste.impl.similarity.CityBlockSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class CityBlockSimilarityFactory extends AbstractUserSimilarityFactory {
    @Override
    public UserSimilarity create() {
        return new CityBlockSimilarity(dataModel);
    }

}
