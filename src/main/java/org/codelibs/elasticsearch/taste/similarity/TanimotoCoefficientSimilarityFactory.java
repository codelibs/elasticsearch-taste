package org.codelibs.elasticsearch.taste.similarity;

import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;

public class TanimotoCoefficientSimilarityFactory<T> extends
        AbstractUserSimilarityFactory<T> {

    @Override
    public T create() {
        @SuppressWarnings("unchecked")
        final T t = (T) new TanimotoCoefficientSimilarity(dataModel);
        return t;
    }

}
