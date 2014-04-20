package org.codelibs.elasticsearch.taste.similarity;

import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;

public class LogLikelihoodSimilarityFactory<T> extends
        AbstractUserSimilarityFactory<T> {

    @Override
    public T create() {
        @SuppressWarnings("unchecked")
        final T t = (T) new LogLikelihoodSimilarity(dataModel);
        return t;
    }

}
