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

import org.codelibs.elasticsearch.taste.common.Cache;
import org.codelibs.elasticsearch.taste.common.LongPair;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.common.Retriever;
import org.codelibs.elasticsearch.taste.model.DataModel;

import com.google.common.base.Preconditions;

/**
 * Caches the results from an underlying {@link UserSimilarity} implementation.
 */
public final class CachingUserSimilarity implements UserSimilarity {

    private final UserSimilarity similarity;

    private final Cache<LongPair, Double> similarityCache;

    private final RefreshHelper refreshHelper;

    /**
     * Creates this on top of the given {@link UserSimilarity}.
     * The cache is sized according to properties of the given {@link DataModel}.
     */
    public CachingUserSimilarity(final UserSimilarity similarity,
            final DataModel dataModel) {
        this(similarity, dataModel.getNumUsers());
    }

    /**
     * Creates this on top of the given {@link UserSimilarity}.
     * The cache size is capped by the given size.
     */
    public CachingUserSimilarity(final UserSimilarity similarity,
            final int maxCacheSize) {
        Preconditions.checkArgument(similarity != null, "similarity is null");
        this.similarity = similarity;
        similarityCache = new Cache<>(new SimilarityRetriever(
                similarity), maxCacheSize);
        refreshHelper = new RefreshHelper(() -> {
            similarityCache.clear();
            return null;
        });
        refreshHelper.addDependency(similarity);
    }

    @Override
    public double userSimilarity(final long userID1, final long userID2) {
        final LongPair key = userID1 < userID2 ? new LongPair(userID1, userID2)
                : new LongPair(userID2, userID1);
        return similarityCache.get(key);
    }

    @Override
    public void setPreferenceInferrer(final PreferenceInferrer inferrer) {
        similarityCache.clear();
        similarity.setPreferenceInferrer(inferrer);
    }

    public void clearCacheForUser(final long userID) {
        similarityCache.removeKeysMatching(new LongPairMatchPredicate(userID));
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

    private static final class SimilarityRetriever implements
            Retriever<LongPair, Double> {
        private final UserSimilarity similarity;

        private SimilarityRetriever(final UserSimilarity similarity) {
            this.similarity = similarity;
        }

        @Override
        public Double get(final LongPair key) {
            return similarity.userSimilarity(key.getFirst(), key.getSecond());
        }
    }

}
