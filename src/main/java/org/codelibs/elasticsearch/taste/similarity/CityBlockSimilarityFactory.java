package org.codelibs.elasticsearch.taste.similarity;

import org.apache.mahout.cf.taste.impl.similarity.CityBlockSimilarity;

public class CityBlockSimilarityFactory<T> extends
        AbstractUserSimilarityFactory<T> {
    @Override
    public T create() {
        @SuppressWarnings("unchecked")
        final T t = (T) new CityBlockSimilarity(dataModel);
        return t;
    }

}
