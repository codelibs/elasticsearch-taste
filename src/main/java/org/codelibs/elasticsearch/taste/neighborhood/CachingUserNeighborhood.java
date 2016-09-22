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

package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.Collection;
import java.util.List;

import org.codelibs.elasticsearch.taste.common.Cache;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.common.Retriever;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.recommender.SimilarUser;

import com.google.common.base.Preconditions;

/** A caching wrapper around an underlying {@link UserNeighborhood} implementation. */
public final class CachingUserNeighborhood implements UserNeighborhood {

    private final UserNeighborhood neighborhood;

    private final Cache<Long, List<SimilarUser>> neighborhoodCache;

    public CachingUserNeighborhood(final UserNeighborhood neighborhood,
            final DataModel dataModel) {
        Preconditions.checkArgument(neighborhood != null,
                "neighborhood is null");
        this.neighborhood = neighborhood;
        final int maxCacheSize = dataModel.getNumUsers(); // just a dumb heuristic for sizing
        neighborhoodCache = new Cache<>(
                new NeighborhoodRetriever(neighborhood), maxCacheSize);
    }

    @Override
    public List<SimilarUser> getUserNeighborhood(final long userID) {
        return neighborhoodCache.get(userID);
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        neighborhoodCache.clear();
        final Collection<Refreshable> refreshed = RefreshHelper
                .buildRefreshed(alreadyRefreshed);
        RefreshHelper.maybeRefresh(refreshed, neighborhood);
    }

    private static final class NeighborhoodRetriever implements
            Retriever<Long, List<SimilarUser>> {
        private final UserNeighborhood neighborhood;

        private NeighborhoodRetriever(final UserNeighborhood neighborhood) {
            this.neighborhood = neighborhood;
        }

        @Override
        public List<SimilarUser> get(final Long key) {
            return neighborhood.getUserNeighborhood(key);
        }
    }
}
