package org.codelibs.elasticsearch.taste.similarity.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.recommender.RecommendedItem;

public interface ItemsWriter extends Closeable {

    void open();

    @Override
    void close() throws IOException;

    void write(long id, List<RecommendedItem> recommendedItems);

}