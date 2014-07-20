/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.codelibs.elasticsearch.taste.similarity.precompute;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.recommender.ItemBasedRecommender;
import org.codelibs.elasticsearch.taste.recommender.RecommendedItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

/**
 * Precompute item similarities in parallel on a single machine. The recommender given to this class must use a
 * DataModel that holds the interactions in memory (such as
 * {@link org.codelibs.elasticsearch.taste.model.GenericDataModel} or
 * {@link org.codelibs.elasticsearch.taste.impl.model.file.FileDataModel}) as fast random access to the data is required
 */
public class MultithreadedBatchItemSimilarities extends BatchItemSimilarities {

    private int batchSize;

    private static final int DEFAULT_BATCH_SIZE = 100;

    private static final Logger log = LoggerFactory
            .getLogger(MultithreadedBatchItemSimilarities.class);

    /**
     * @param recommender recommender to use
     * @param similarItemsPerItem number of similar items to compute per item
     */
    public MultithreadedBatchItemSimilarities(
            final ItemBasedRecommender recommender,
            final int similarItemsPerItem) {
        this(recommender, similarItemsPerItem, DEFAULT_BATCH_SIZE);
    }

    /**
     * @param recommender recommender to use
     * @param similarItemsPerItem number of similar items to compute per item
     * @param batchSize size of item batches sent to worker threads
     */
    public MultithreadedBatchItemSimilarities(
            final ItemBasedRecommender recommender,
            final int similarItemsPerItem, final int batchSize) {
        super(recommender, similarItemsPerItem);
        this.batchSize = batchSize;
    }

    @Override
    public int computeItemSimilarities(final int degreeOfParallelism,
            final int maxDurationInHours, final SimilarItemsWriter writer)
            throws IOException {

        final ExecutorService executorService = Executors
                .newFixedThreadPool(degreeOfParallelism + 1);

        Output output = null;
        try {
            writer.open();

            final DataModel dataModel = getRecommender().getDataModel();

            final BlockingQueue<long[]> itemsIDsInBatches = queueItemIDsInBatches(
                    dataModel, batchSize);
            final BlockingQueue<List<SimilarItems>> results = new LinkedBlockingQueue<List<SimilarItems>>();

            final AtomicInteger numActiveWorkers = new AtomicInteger(
                    degreeOfParallelism);
            for (int n = 0; n < degreeOfParallelism; n++) {
                executorService.execute(new SimilarItemsWorker(n,
                        itemsIDsInBatches, results, numActiveWorkers));
            }

            output = new Output(results, writer, numActiveWorkers);
            executorService.execute(output);

        } catch (final Exception e) {
            throw new IOException(e);
        } finally {
            executorService.shutdown();
            try {
                final boolean succeeded = executorService.awaitTermination(
                        maxDurationInHours, TimeUnit.HOURS);
                if (!succeeded) {
                    throw new RuntimeException(
                            "Unable to complete the computation in "
                                    + maxDurationInHours + " hours!");
                }
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            Closeables.close(writer, false);
        }

        return output.getNumSimilaritiesProcessed();
    }

    private static BlockingQueue<long[]> queueItemIDsInBatches(
            final DataModel dataModel, final int batchSize) {

        final LongPrimitiveIterator itemIDs = dataModel.getItemIDs();
        final int numItems = dataModel.getNumItems();

        final BlockingQueue<long[]> itemIDBatches = new LinkedBlockingQueue<long[]>(
                numItems / batchSize + 1);

        final long[] batch = new long[batchSize];
        int pos = 0;
        while (itemIDs.hasNext()) {
            if (pos == batchSize) {
                itemIDBatches.add(batch.clone());
                pos = 0;
            }
            batch[pos] = itemIDs.nextLong();
            pos++;
        }
        final int nonQueuedItemIDs = batchSize - pos;
        if (nonQueuedItemIDs > 0) {
            final long[] lastBatch = new long[nonQueuedItemIDs];
            System.arraycopy(batch, 0, lastBatch, 0, nonQueuedItemIDs);
            itemIDBatches.add(lastBatch);
        }

        log.info("Queued {} items in {} batches", numItems,
                itemIDBatches.size());

        return itemIDBatches;
            }

    private static class Output implements Runnable {

        private final BlockingQueue<List<SimilarItems>> results;

        private final SimilarItemsWriter writer;

        private final AtomicInteger numActiveWorkers;

        private int numSimilaritiesProcessed = 0;

        Output(final BlockingQueue<List<SimilarItems>> results,
                final SimilarItemsWriter writer,
                final AtomicInteger numActiveWorkers) {
            this.results = results;
            this.writer = writer;
            this.numActiveWorkers = numActiveWorkers;
        }

        private int getNumSimilaritiesProcessed() {
            return numSimilaritiesProcessed;
        }

        @Override
        public void run() {
            while (numActiveWorkers.get() != 0) {
                try {
                    final List<SimilarItems> similarItemsOfABatch = results
                            .poll(10, TimeUnit.MILLISECONDS);
                    if (similarItemsOfABatch != null) {
                        for (final SimilarItems similarItems : similarItemsOfABatch) {
                            writer.add(similarItems);
                            numSimilaritiesProcessed += similarItems
                                    .numSimilarItems();
                        }
                    }
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private class SimilarItemsWorker implements Runnable {

        private final int number;

        private final BlockingQueue<long[]> itemIDBatches;

        private final BlockingQueue<List<SimilarItems>> results;

        private final AtomicInteger numActiveWorkers;

        SimilarItemsWorker(final int number,
                final BlockingQueue<long[]> itemIDBatches,
                final BlockingQueue<List<SimilarItems>> results,
                final AtomicInteger numActiveWorkers) {
            this.number = number;
            this.itemIDBatches = itemIDBatches;
            this.results = results;
            this.numActiveWorkers = numActiveWorkers;
        }

        @Override
        public void run() {

            int numBatchesProcessed = 0;
            while (!itemIDBatches.isEmpty()) {
                try {
                    final long[] itemIDBatch = itemIDBatches.take();

                    final List<SimilarItems> similarItemsOfBatch = Lists
                            .newArrayListWithCapacity(itemIDBatch.length);
                    for (final long itemID : itemIDBatch) {
                        final List<RecommendedItem> similarItems = getRecommender()
                                .mostSimilarItems(itemID,
                                        getSimilarItemsPerItem());

                        similarItemsOfBatch.add(new SimilarItems(itemID,
                                similarItems));
                    }

                    results.offer(similarItemsOfBatch);

                    if (++numBatchesProcessed % 5 == 0) {
                        log.info("worker {} processed {} batches", number,
                                numBatchesProcessed);
                    }

                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("worker {} processed {} batches. done.", number,
                    numBatchesProcessed);
            numActiveWorkers.decrementAndGet();
        }
    }
}
