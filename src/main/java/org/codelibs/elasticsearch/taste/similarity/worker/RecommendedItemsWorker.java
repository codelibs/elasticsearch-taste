package org.codelibs.elasticsearch.taste.similarity.worker;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.codelibs.elasticsearch.taste.similarity.precompute.RecommendedItemsWriter;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class RecommendedItemsWorker implements Runnable {
    private static final ESLogger logger = Loggers
            .getLogger(RecommendedItemsWorker.class);

    protected int number;

    protected Recommender recommender;

    protected LongPrimitiveIterator userIDs;

    protected int numOfRecommendedItems;

    protected RecommendedItemsWriter writer;

    public RecommendedItemsWorker(final int number,
            final Recommender recommender, final LongPrimitiveIterator userIDs,
            final int numOfRecommendedItems, final RecommendedItemsWriter writer) {
        this.number = number;
        this.recommender = recommender;
        this.userIDs = userIDs;
        this.numOfRecommendedItems = numOfRecommendedItems;
        this.writer = writer;
    }

    @Override
    public void run() {
        logger.info("Worker {} is started.", number);
        long userID;
        while ((userID = nextId(userIDs)) != -1) {
            try {
                final List<RecommendedItem> recommendedItems = recommender
                        .recommend(userID, numOfRecommendedItems);
                writer.write(userID, recommendedItems);
            } catch (final Exception e) {
                logger.error("Item {} could not be processed.", e, userID);
            }
        }
        logger.info("Worker {} is complated.", number);
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
