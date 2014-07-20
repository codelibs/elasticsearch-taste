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

package org.codelibs.elasticsearch.taste.similarity;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.codelibs.elasticsearch.taste.common.Cache;
import org.codelibs.elasticsearch.taste.common.LongPair;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.common.Retriever;
import org.codelibs.elasticsearch.taste.model.DataModel;

import com.google.common.base.Preconditions;

/**
 * Caches the results from an underlying {@link ItemSimilarity} implementation.
 */
public final class CachingItemSimilarity implements ItemSimilarity {

    private final ItemSimilarity similarity;

    private final Cache<LongPair, Double> similarityCache;

    private final RefreshHelper refreshHelper;

    /**
     * Creates this on top of the given {@link ItemSimilarity}.
     * The cache is sized according to properties of the given {@link DataModel}.
     */
    public CachingItemSimilarity(final ItemSimilarity similarity,
            final DataModel dataModel) {
        this(similarity, dataModel.getNumItems());
    }

    /**
     * Creates this on top of the given {@link ItemSimilarity}.
     * The cache size is capped by the given size.
     */
    public CachingItemSimilarity(final ItemSimilarity similarity,
            final int maxCacheSize) {
        Preconditions.checkArgument(similarity != null, "similarity is null");
        this.similarity = similarity;
        similarityCache = new Cache<LongPair, Double>(new SimilarityRetriever(
                similarity), maxCacheSize);
        refreshHelper = new RefreshHelper(new Callable<Void>() {
            @Override
            public Void call() {
                similarityCache.clear();
                return null;
            }
        });
        refreshHelper.addDependency(similarity);
    }

    @Override
    public double itemSimilarity(final long itemID1, final long itemID2) {
        final LongPair key = itemID1 < itemID2 ? new LongPair(itemID1, itemID2)
                : new LongPair(itemID2, itemID1);
        return similarityCache.get(key);
    }

    @Override
    public double[] itemSimilarities(final long itemID1, final long[] itemID2s) {
        final int length = itemID2s.length;
        final double[] result = new double[length];
        for (int i = 0; i < length; i++) {
            result[i] = itemSimilarity(itemID1, itemID2s[i]);
        }
        return result;
    }

    @Override
    public long[] allSimilarItemIDs(final long itemID) {
        return similarity.allSimilarItemIDs(itemID);
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

    public void clearCacheForItem(final long itemID) {
        similarityCache.removeKeysMatching(new LongPairMatchPredicate(itemID));
    }

    private static final class SimilarityRetriever implements
            Retriever<LongPair, Double> {
        private final ItemSimilarity similarity;

        private SimilarityRetriever(final ItemSimilarity similarity) {
            this.similarity = similarity;
        }

        @Override
        public Double get(final LongPair key) {
            return similarity.itemSimilarity(key.getFirst(), key.getSecond());
        }
    }

}
