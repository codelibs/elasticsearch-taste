package org.codelibs.elasticsearch.taste.similarity;

public class SpearmanCorrelationSimilarityFactory<T> extends
        AbstractUserSimilarityFactory<T> {

    @Override
    public T create() {
        @SuppressWarnings("unchecked")
        final T t = (T) new SpearmanCorrelationSimilarity(dataModel);
        return t;
    }

}
