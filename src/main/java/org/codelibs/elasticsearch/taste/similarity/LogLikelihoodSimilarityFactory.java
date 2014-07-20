package org.codelibs.elasticsearch.taste.similarity;


public class LogLikelihoodSimilarityFactory<T> extends
AbstractUserSimilarityFactory<T> {

    @Override
    public T create() {
        @SuppressWarnings("unchecked")
        final T t = (T) new LogLikelihoodSimilarity(dataModel);
        return t;
    }

}
