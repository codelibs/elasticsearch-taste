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
import java.util.Iterator;

import org.apache.mahout.common.RandomUtils;
import org.codelibs.elasticsearch.taste.common.FastByIDMap;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.recommender.TopItems;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

public final class GenericUserSimilarity implements UserSimilarity {

    private final FastByIDMap<FastByIDMap<Double>> similarityMaps = new FastByIDMap<>();

    public GenericUserSimilarity(final Iterable<UserUserSimilarity> similarities) {
        initSimilarityMaps(similarities.iterator());
    }

    public GenericUserSimilarity(
            final Iterable<UserUserSimilarity> similarities, final int maxToKeep) {
        final Iterable<UserUserSimilarity> keptSimilarities = TopItems
                .getTopUserUserSimilarities(maxToKeep, similarities.iterator());
        initSimilarityMaps(keptSimilarities.iterator());
    }

    public GenericUserSimilarity(final UserSimilarity otherSimilarity,
            final DataModel dataModel) {
        final long[] userIDs = longIteratorToList(dataModel.getUserIDs());
        initSimilarityMaps(new DataModelSimilaritiesIterator(otherSimilarity,
                userIDs));
    }

    public GenericUserSimilarity(final UserSimilarity otherSimilarity,
            final DataModel dataModel, final int maxToKeep) {
        final long[] userIDs = longIteratorToList(dataModel.getUserIDs());
        final Iterator<UserUserSimilarity> it = new DataModelSimilaritiesIterator(
                otherSimilarity, userIDs);
        final Iterable<UserUserSimilarity> keptSimilarities = TopItems
                .getTopUserUserSimilarities(maxToKeep, it);
        initSimilarityMaps(keptSimilarities.iterator());
    }

    static long[] longIteratorToList(final LongPrimitiveIterator iterator) {
        long[] result = new long[5];
        int size = 0;
        while (iterator.hasNext()) {
            if (size == result.length) {
                final long[] newResult = new long[result.length << 1];
                System.arraycopy(result, 0, newResult, 0, result.length);
                result = newResult;
            }
            result[size++] = iterator.next();
        }
        if (size != result.length) {
            final long[] newResult = new long[size];
            System.arraycopy(result, 0, newResult, 0, size);
            result = newResult;
        }
        return result;
    }

    private void initSimilarityMaps(
            final Iterator<UserUserSimilarity> similarities) {
        while (similarities.hasNext()) {
            final UserUserSimilarity uuc = similarities.next();
            final long similarityUser1 = uuc.getUserID1();
            final long similarityUser2 = uuc.getUserID2();
            if (similarityUser1 != similarityUser2) {
                // Order them -- first key should be the "smaller" one
                long user1;
                long user2;
                if (similarityUser1 < similarityUser2) {
                    user1 = similarityUser1;
                    user2 = similarityUser2;
                } else {
                    user1 = similarityUser2;
                    user2 = similarityUser1;
                }
                FastByIDMap<Double> map = similarityMaps.get(user1);
                if (map == null) {
                    map = new FastByIDMap<>();
                    similarityMaps.put(user1, map);
                }
                map.put(user2, uuc.getValue());
            }
            // else similarity between user and itself already assumed to be 1.0
        }
    }

    @Override
    public double userSimilarity(final long userID1, final long userID2) {
        if (userID1 == userID2) {
            return 1.0;
        }
        long first;
        long second;
        if (userID1 < userID2) {
            first = userID1;
            second = userID2;
        } else {
            first = userID2;
            second = userID1;
        }
        final FastByIDMap<Double> nextMap = similarityMaps.get(first);
        if (nextMap == null) {
            return Double.NaN;
        }
        final Double similarity = nextMap.get(second);
        return similarity == null ? Double.NaN : similarity;
    }

    @Override
    public void setPreferenceInferrer(final PreferenceInferrer inferrer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        // Do nothing
    }

    public static final class UserUserSimilarity implements
            Comparable<UserUserSimilarity> {

        private final long userID1;

        private final long userID2;

        private final double value;

        public UserUserSimilarity(final long userID1, final long userID2,
                final double value) {
            Preconditions.checkArgument(value >= -1.0 && value <= 1.0,
                    "Illegal value: " + value
                            + ". Must be: -1.0 <= value <= 1.0");
            this.userID1 = userID1;
            this.userID2 = userID2;
            this.value = value;
        }

        public long getUserID1() {
            return userID1;
        }

        public long getUserID2() {
            return userID2;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "UserUserSimilarity[" + userID1 + ',' + userID2 + ':'
                    + value + ']';
        }

        /** Defines an ordering from highest similarity to lowest. */
        @Override
        public int compareTo(final UserUserSimilarity other) {
            final double otherValue = other.getValue();
            return value > otherValue ? -1 : value < otherValue ? 1 : 0;
        }

        @Override
        public boolean equals(final Object other) {
            if (!(other instanceof UserUserSimilarity)) {
                return false;
            }
            final UserUserSimilarity otherSimilarity = (UserUserSimilarity) other;
            return otherSimilarity.getUserID1() == userID1
                    && otherSimilarity.getUserID2() == userID2
                    && !(otherSimilarity.getValue() < value
                            || otherSimilarity.getValue() > value);
        }

        @Override
        public int hashCode() {
            return (int) userID1 ^ (int) userID2
                    ^ RandomUtils.hashDouble(value);
        }

    }

    private static final class DataModelSimilaritiesIterator extends
            AbstractIterator<UserUserSimilarity> {

        private final UserSimilarity otherSimilarity;

        private final long[] itemIDs;

        private int i;

        private long itemID1;

        private int j;

        private DataModelSimilaritiesIterator(
                final UserSimilarity otherSimilarity, final long[] itemIDs) {
            this.otherSimilarity = otherSimilarity;
            this.itemIDs = itemIDs;
            i = 0;
            itemID1 = itemIDs[0];
            j = 1;
        }

        @Override
        protected UserUserSimilarity computeNext() {
            final int size = itemIDs.length;
            while (i < size - 1) {
                final long itemID2 = itemIDs[j];
                double similarity;
                try {
                    similarity = otherSimilarity.userSimilarity(itemID1,
                            itemID2);
                } catch (final Exception te) {
                    // ugly:
                    throw new TasteException("Invalid state: " + itemID1 + ", "
                            + itemID2, te);
                }
                if (!Double.isNaN(similarity)) {
                    return new UserUserSimilarity(itemID1, itemID2, similarity);
                }
                if (++j == size) {
                    itemID1 = itemIDs[++i];
                    j = i + 1;
                }
            }
            return endOfData();
        }

    }

}
