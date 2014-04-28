package org.codelibs.elasticsearch.taste.similarity.worker;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.MemoryUtil;
import org.codelibs.elasticsearch.taste.similarity.writer.ItemsWriter;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class SimilarItemsWorker implements Runnable {
    private static final ESLogger logger = Loggers
            .getLogger(SimilarItemsWorker.class);

    protected int number;

    protected ItemBasedRecommender recommender;

    protected LongPrimitiveIterator itemIDs;

    protected int numOfMostSimilarItems;

    protected ItemsWriter writer;

    public SimilarItemsWorker(final int number,
            final ItemBasedRecommender recommender,
            final LongPrimitiveIterator itemIDs,
            final int numOfMostSimilarItems, final ItemsWriter writer) {
        this.number = number;
        this.recommender = recommender;
        this.itemIDs = itemIDs;
        this.numOfMostSimilarItems = numOfMostSimilarItems;
        this.writer = writer;
    }

    @Override
    public void run() {
        int count = 0;
        final long startTime = System.currentTimeMillis();
        logger.info("Worker {} is started.", number);
        long itemID;
        while ((itemID = nextId(itemIDs)) != -1) {
            try {
                long time = System.nanoTime();
                final List<RecommendedItem> recommendedItems = recommender
                        .mostSimilarItems(itemID, numOfMostSimilarItems);
                writer.write(itemID, recommendedItems);
                time = (System.nanoTime() - time) / 1000000;
                if (logger.isDebugEnabled()) {
                    logger.debug("Item {} => Time: {} ms, Result: {}", itemID,
                            time, recommendedItems);
                    if (count % 100 == 0) {
                        MemoryUtil.logMemoryStatistics();
                    }
                } else {
                    logger.info("Item {} => Time: {} ms, Result: {} items",
                            itemID, time, recommendedItems.size());
                    if (count % 1000 == 0) {
                        MemoryUtil.logMemoryStatistics();
                    }
                }
            } catch (final Exception e) {
                logger.error("Item {} could not be processed.", e, itemID);
            }
            count++;
        }
        logger.info("Worker {} processed {} items at {} ms. ", number, count,
                System.currentTimeMillis() - startTime);
    }

    private long nextId(final LongPrimitiveIterator itemIDs) {
        synchronized (itemIDs) {
            try {
                return itemIDs.nextLong();
            } catch (final NoSuchElementException e) {
                return -1;
            }
        }
    }
}
