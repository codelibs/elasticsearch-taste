package org.codelibs.elasticsearch.taste.similarity;


public class CityBlockSimilarityFactory<T> extends
AbstractUserSimilarityFactory<T> {
    @Override
    public T create() {
        @SuppressWarnings("unchecked")
        final T t = (T) new CityBlockSimilarity(dataModel);
        return t;
    }

}
