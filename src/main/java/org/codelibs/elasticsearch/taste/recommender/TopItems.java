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

package org.codelibs.elasticsearch.taste.recommender;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.exception.NoSuchItemException;
import org.codelibs.elasticsearch.taste.exception.NoSuchUserException;
import org.codelibs.elasticsearch.taste.similarity.GenericItemSimilarity;
import org.codelibs.elasticsearch.taste.similarity.GenericUserSimilarity;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * A simple class that refactors the "find top N things" logic that is used in several places.
 * </p>
 */
public final class TopItems {

    private TopItems() {
    }

    public static List<RecommendedItem> getTopItems(final int howMany,
            final LongPrimitiveIterator possibleItemIDs,
            final IDRescorer rescorer, final Estimator<Long> estimator) {
        Preconditions.checkArgument(possibleItemIDs != null,
                "possibleItemIDs is null");
        Preconditions.checkArgument(estimator != null, "estimator is null");

        final Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(
                howMany + 1,
                Collections.reverseOrder(ByValueRecommendedItemComparator
                        .getInstance()));
        boolean full = false;
        double lowestTopValue = Double.NEGATIVE_INFINITY;
        while (possibleItemIDs.hasNext()) {
            final long itemID = possibleItemIDs.next();
            if (rescorer == null || !rescorer.isFiltered(itemID)) {
                double preference;
                try {
                    preference = estimator.estimate(itemID);
                } catch (final NoSuchItemException nsie) {
                    continue;
                }
                final double rescoredPref = rescorer == null ? preference
                        : rescorer.rescore(itemID, preference);
                if (!Double.isNaN(rescoredPref)
                        && (!full || rescoredPref > lowestTopValue)) {
                    topItems.add(new GenericRecommendedItem(itemID,
                            (float) rescoredPref));
                    if (full) {
                        topItems.poll();
                    } else if (topItems.size() > howMany) {
                        full = true;
                        topItems.poll();
                    }
                    lowestTopValue = topItems.peek().getValue();
                }
            }
        }
        final int size = topItems.size();
        if (size == 0) {
            return Collections.emptyList();
        }
        final List<RecommendedItem> result = Lists
                .newArrayListWithCapacity(size);
        result.addAll(topItems);
        Collections
                .sort(result, ByValueRecommendedItemComparator.getInstance());
        return result;
    }

    public static List<SimilarUser> getTopUsers(final int howMany,
            final LongPrimitiveIterator allUserIDs, final IDRescorer rescorer,
            final Estimator<Long> estimator) {
        final Queue<SimilarUser> topUsers = new PriorityQueue<SimilarUser>(
                howMany + 1, Collections.reverseOrder());
        boolean full = false;
        double lowestTopValue = Double.NEGATIVE_INFINITY;
        while (allUserIDs.hasNext()) {
            final long userID = allUserIDs.next();
            if (rescorer != null && rescorer.isFiltered(userID)) {
                continue;
            }
            double similarity;
            try {
                similarity = estimator.estimate(userID);
            } catch (final NoSuchUserException nsue) {
                continue;
            }
            final double rescoredSimilarity = rescorer == null ? similarity
                    : rescorer.rescore(userID, similarity);
            if (!Double.isNaN(rescoredSimilarity)
                    && (!full || rescoredSimilarity > lowestTopValue)) {
                topUsers.add(new SimilarUser(userID, rescoredSimilarity));
                if (full) {
                    topUsers.poll();
                } else if (topUsers.size() > howMany) {
                    full = true;
                    topUsers.poll();
                }
                lowestTopValue = topUsers.peek().getSimilarity();
            }
        }
        final int size = topUsers.size();
        if (size == 0) {
            return Collections.emptyList();
        }
        final List<SimilarUser> sorted = Lists.newArrayListWithCapacity(size);
        sorted.addAll(topUsers);
        Collections.sort(sorted);
        return sorted;
    }

    /**
     * <p>
     * Thanks to tsmorton for suggesting this functionality and writing part of the code.
     * </p>
     *
     * @see GenericItemSimilarity#GenericItemSimilarity(Iterable, int)
     * @see GenericItemSimilarity#GenericItemSimilarity(org.codelibs.elasticsearch.taste.similarity.ItemSimilarity,
     *      org.codelibs.elasticsearch.taste.model.DataModel, int)
     */
    public static List<GenericItemSimilarity.ItemItemSimilarity> getTopItemItemSimilarities(
            final int howMany,
            final Iterator<GenericItemSimilarity.ItemItemSimilarity> allSimilarities) {

        final Queue<GenericItemSimilarity.ItemItemSimilarity> topSimilarities = new PriorityQueue<GenericItemSimilarity.ItemItemSimilarity>(
                howMany + 1, Collections.reverseOrder());
        boolean full = false;
        double lowestTopValue = Double.NEGATIVE_INFINITY;
        while (allSimilarities.hasNext()) {
            final GenericItemSimilarity.ItemItemSimilarity similarity = allSimilarities
                    .next();
            final double value = similarity.getValue();
            if (!Double.isNaN(value) && (!full || value > lowestTopValue)) {
                topSimilarities.add(similarity);
                if (full) {
                    topSimilarities.poll();
                } else if (topSimilarities.size() > howMany) {
                    full = true;
                    topSimilarities.poll();
                }
                lowestTopValue = topSimilarities.peek().getValue();
            }
        }
        final int size = topSimilarities.size();
        if (size == 0) {
            return Collections.emptyList();
        }
        final List<GenericItemSimilarity.ItemItemSimilarity> result = Lists
                .newArrayListWithCapacity(size);
        result.addAll(topSimilarities);
        Collections.sort(result);
        return result;
    }

    public static List<GenericUserSimilarity.UserUserSimilarity> getTopUserUserSimilarities(
            final int howMany,
            final Iterator<GenericUserSimilarity.UserUserSimilarity> allSimilarities) {

        final Queue<GenericUserSimilarity.UserUserSimilarity> topSimilarities = new PriorityQueue<GenericUserSimilarity.UserUserSimilarity>(
                howMany + 1, Collections.reverseOrder());
        boolean full = false;
        double lowestTopValue = Double.NEGATIVE_INFINITY;
        while (allSimilarities.hasNext()) {
            final GenericUserSimilarity.UserUserSimilarity similarity = allSimilarities
                    .next();
            final double value = similarity.getValue();
            if (!Double.isNaN(value) && (!full || value > lowestTopValue)) {
                topSimilarities.add(similarity);
                if (full) {
                    topSimilarities.poll();
                } else if (topSimilarities.size() > howMany) {
                    full = true;
                    topSimilarities.poll();
                }
                lowestTopValue = topSimilarities.peek().getValue();
            }
        }
        final int size = topSimilarities.size();
        if (size == 0) {
            return Collections.emptyList();
        }
        final List<GenericUserSimilarity.UserUserSimilarity> result = Lists
                .newArrayListWithCapacity(size);
        result.addAll(topSimilarities);
        Collections.sort(result);
        return result;
    }

    public interface Estimator<T> {
        double estimate(T thing);
    }

}
