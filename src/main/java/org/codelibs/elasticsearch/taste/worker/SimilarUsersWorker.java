package org.codelibs.elasticsearch.taste.worker;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.common.MemoryUtil;
import org.codelibs.elasticsearch.taste.recommender.SimilarUser;
import org.codelibs.elasticsearch.taste.recommender.UserBasedRecommender;
import org.codelibs.elasticsearch.taste.writer.UserWriter;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class SimilarUsersWorker implements Runnable {
    private static final ESLogger logger = Loggers
            .getLogger(SimilarUsersWorker.class);

    protected int number;

    protected UserBasedRecommender recommender;

    protected LongPrimitiveIterator userIDs;

    protected int numOfSimilarUsers;

    protected UserWriter writer;

    private boolean running;

    public SimilarUsersWorker(final int number,
            final UserBasedRecommender recommender,
            final LongPrimitiveIterator userIDs, final int numOfSimilarUsers,
            final UserWriter writer) {
        this.number = number;
        this.recommender = recommender;
        this.userIDs = userIDs;
        this.numOfSimilarUsers = numOfSimilarUsers;
        this.writer = writer;
    }

    @Override
    public void run() {
        int count = 0;
        final long startTime = System.currentTimeMillis();
        logger.info("Worker {} is started.", number);
        long userID;
        running = true;
        while ((userID = nextId(userIDs)) != -1 && running) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            try {
                long time = System.nanoTime();
                final List<SimilarUser> mostSimilarUsers = recommender
                        .mostSimilarUserIDs(userID, numOfSimilarUsers);
                writer.write(userID, mostSimilarUsers);
                time = (System.nanoTime() - time) / 1000000;
                if (logger.isDebugEnabled()) {
                    logger.debug("User {} => Time: {} ms, Result: {}", userID,
                            time, mostSimilarUsers);
                    if (count % 100 == 0) {
                        MemoryUtil.logMemoryStatistics();
                    }
                } else {
                    logger.info("User {} => Time: {} ms, Result: {} items",
                            userID, time, mostSimilarUsers.size());
                    if (count % 1000 == 0) {
                        MemoryUtil.logMemoryStatistics();
                    }
                }
            } catch (final Exception e) {
                if (!Thread.currentThread().isInterrupted()) {
                    logger.error("User {} could not be processed.", e, userID);
                } else {
                    break;
                }
            }
            count++;
        }
        logger.info("Worker {} processed {} users at {} ms. ", number, count,
                System.currentTimeMillis() - startTime);
    }

    public void stop() {
        running = false;
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
