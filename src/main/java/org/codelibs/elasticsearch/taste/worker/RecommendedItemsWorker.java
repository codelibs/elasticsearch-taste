package org.codelibs.elasticsearch.taste.worker;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.common.MemoryUtil;
import org.codelibs.elasticsearch.taste.writer.ItemsWriter;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class RecommendedItemsWorker implements Runnable {
    private static final ESLogger logger = Loggers
            .getLogger(RecommendedItemsWorker.class);

    protected int number;

    protected Recommender recommender;

    protected LongPrimitiveIterator userIDs;

    protected int numOfRecommendedItems;

    protected ItemsWriter writer;

    public RecommendedItemsWorker(final int number,
            final Recommender recommender, final LongPrimitiveIterator userIDs,
            final int numOfRecommendedItems, final ItemsWriter writer) {
        this.number = number;
        this.recommender = recommender;
        this.userIDs = userIDs;
        this.numOfRecommendedItems = numOfRecommendedItems;
        this.writer = writer;
    }

    @Override
    public void run() {
        int count = 0;
        final long startTime = System.currentTimeMillis();
        logger.info("Worker {} is started.", number);
        long userID;
        while ((userID = nextId(userIDs)) != -1) {
            try {
                long time = System.nanoTime();
                final List<RecommendedItem> recommendedItems = recommender
                        .recommend(userID, numOfRecommendedItems);
                writer.write(userID, recommendedItems);
                time = (System.nanoTime() - time) / 1000000;
                if (logger.isDebugEnabled()) {
                    logger.debug("User {} => Time: {} ms, Result: {}", userID,
                            time, recommendedItems);
                    if (count % 100 == 0) {
                        MemoryUtil.logMemoryStatistics();
                    }
                } else {
                    logger.info("User {} => Time: {} ms, Result: {} items",
                            userID, time, recommendedItems.size());
                    if (count % 1000 == 0) {
                        MemoryUtil.logMemoryStatistics();
                    }
                }
            } catch (final Exception e) {
                logger.error("User {} could not be processed.", e, userID);
            }
            count++;
        }
        logger.info("Worker {} processed {} users at {} ms. ", number, count,
                System.currentTimeMillis() - startTime);
    }

    private long nextId(final LongPrimitiveIterator userIDs) {
        synchronized (userIDs) {
            try {
                return userIDs.nextLong();
            } catch (final NoSuchElementException e) {
                return -1;
            }
        }
    }
}
