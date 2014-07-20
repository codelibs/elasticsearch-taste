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

package org.codelibs.elasticsearch.taste.eval;

import java.util.Arrays;
import java.util.List;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.RunningAverage;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;
import org.codelibs.elasticsearch.taste.recommender.RecommendedItem;
import org.codelibs.elasticsearch.taste.recommender.Recommender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluate recommender by comparing order of all raw prefs with order in
 * recommender's output for that user. Can also compare data models.
 */
public final class OrderBasedRecommenderEvaluator {

    private static final Logger log = LoggerFactory
            .getLogger(OrderBasedRecommenderEvaluator.class);

    private OrderBasedRecommenderEvaluator() {
    }

    public static void evaluate(final Recommender recommender1,
            final Recommender recommender2, final int samples,
            final RunningAverage tracker, final String tag) {
        printHeader();
        final LongPrimitiveIterator users = recommender1.getDataModel()
                .getUserIDs();

        while (users.hasNext()) {
            final long userID = users.nextLong();
            final List<RecommendedItem> recs1 = recommender1.recommend(userID,
                    samples);
            final List<RecommendedItem> recs2 = recommender2.recommend(userID,
                    samples);
            final FastIDSet commonSet = new FastIDSet();
            long maxItemID = setBits(commonSet, recs1, samples);
            final FastIDSet otherSet = new FastIDSet();
            maxItemID = Math.max(maxItemID, setBits(otherSet, recs2, samples));
            int max = mask(commonSet, otherSet, maxItemID);
            max = Math.min(max, samples);
            if (max < 2) {
                continue;
            }
            final Long[] items1 = getCommonItems(commonSet, recs1, max);
            final Long[] items2 = getCommonItems(commonSet, recs2, max);
            final double variance = scoreCommonSubset(tag, userID, samples,
                    max, items1, items2);
            tracker.addDatum(variance);
        }
    }

    public static void evaluate(final Recommender recommender,
            final DataModel model, final int samples,
            final RunningAverage tracker, final String tag) {
        printHeader();
        final LongPrimitiveIterator users = recommender.getDataModel()
                .getUserIDs();
        while (users.hasNext()) {
            final long userID = users.nextLong();
            final List<RecommendedItem> recs1 = recommender.recommend(userID,
                    model.getNumItems());
            final PreferenceArray prefs2 = model.getPreferencesFromUser(userID);
            prefs2.sortByValueReversed();
            final FastIDSet commonSet = new FastIDSet();
            long maxItemID = setBits(commonSet, recs1, samples);
            final FastIDSet otherSet = new FastIDSet();
            maxItemID = Math.max(maxItemID, setBits(otherSet, prefs2, samples));
            int max = mask(commonSet, otherSet, maxItemID);
            max = Math.min(max, samples);
            if (max < 2) {
                continue;
            }
            final Long[] items1 = getCommonItems(commonSet, recs1, max);
            final Long[] items2 = getCommonItems(commonSet, prefs2, max);
            final double variance = scoreCommonSubset(tag, userID, samples,
                    max, items1, items2);
            tracker.addDatum(variance);
        }
    }

    public static void evaluate(final DataModel model1, final DataModel model2,
            final int samples, final RunningAverage tracker, final String tag) {
        printHeader();
        final LongPrimitiveIterator users = model1.getUserIDs();
        while (users.hasNext()) {
            final long userID = users.nextLong();
            final PreferenceArray prefs1 = model1
                    .getPreferencesFromUser(userID);
            final PreferenceArray prefs2 = model2
                    .getPreferencesFromUser(userID);
            prefs1.sortByValueReversed();
            prefs2.sortByValueReversed();
            final FastIDSet commonSet = new FastIDSet();
            long maxItemID = setBits(commonSet, prefs1, samples);
            final FastIDSet otherSet = new FastIDSet();
            maxItemID = Math.max(maxItemID, setBits(otherSet, prefs2, samples));
            int max = mask(commonSet, otherSet, maxItemID);
            max = Math.min(max, samples);
            if (max < 2) {
                continue;
            }
            final Long[] items1 = getCommonItems(commonSet, prefs1, max);
            final Long[] items2 = getCommonItems(commonSet, prefs2, max);
            final double variance = scoreCommonSubset(tag, userID, samples,
                    max, items1, items2);
            tracker.addDatum(variance);
        }
    }

    /**
     * This exists because FastIDSet has 'retainAll' as MASK, but there is
     * no count of the number of items in the set. size() is supposed to do
     * this but does not work.
     */
    private static int mask(final FastIDSet commonSet,
            final FastIDSet otherSet, final long maxItemID) {
        int count = 0;
        for (int i = 0; i <= maxItemID; i++) {
            if (commonSet.contains(i)) {
                if (otherSet.contains(i)) {
                    count++;
                } else {
                    commonSet.remove(i);
                }
            }
        }
        return count;
    }

    private static Long[] getCommonItems(final FastIDSet commonSet,
            final Iterable<RecommendedItem> recs, final int max) {
        final Long[] commonItems = new Long[max];
        int index = 0;
        for (final RecommendedItem rec : recs) {
            final Long item = rec.getItemID();
            if (commonSet.contains(item)) {
                commonItems[index++] = item;
            }
            if (index == max) {
                break;
            }
        }
        return commonItems;
    }

    private static Long[] getCommonItems(final FastIDSet commonSet,
            final PreferenceArray prefs1, final int max) {
        final Long[] commonItems = new Long[max];
        int index = 0;
        for (int i = 0; i < prefs1.length(); i++) {
            final Long item = prefs1.getItemID(i);
            if (commonSet.contains(item)) {
                commonItems[index++] = item;
            }
            if (index == max) {
                break;
            }
        }
        return commonItems;
    }

    private static long setBits(final FastIDSet modelSet,
            final List<RecommendedItem> items, final int max) {
        long maxItem = -1;
        for (int i = 0; i < items.size() && i < max; i++) {
            final long itemID = items.get(i).getItemID();
            modelSet.add(itemID);
            if (itemID > maxItem) {
                maxItem = itemID;
            }
        }
        return maxItem;
    }

    private static long setBits(final FastIDSet modelSet,
            final PreferenceArray prefs, final int max) {
        long maxItem = -1;
        for (int i = 0; i < prefs.length() && i < max; i++) {
            final long itemID = prefs.getItemID(i);
            modelSet.add(itemID);
            if (itemID > maxItem) {
                maxItem = itemID;
            }
        }
        return maxItem;
    }

    private static void printHeader() {
        log.info("tag,user,samples,common,hamming,bubble,rank,normal,score");
    }

    /**
     * Common Subset Scoring
     *
     * These measurements are given the set of results that are common to both
     * recommendation lists. They only get ordered lists.
     *
     * These measures all return raw numbers do not correlate among the tests.
     * The numbers are not corrected against the total number of samples or the
     * number of common items.
     * The one contract is that all measures are 0 for an exact match and an
     * increasing positive number as differences increase.
     */
    private static double scoreCommonSubset(final String tag,
            final long userID, final int samples, final int subset,
            final Long[] itemsL, final Long[] itemsR) {
        final int[] vectorZ = new int[subset];
        final int[] vectorZabs = new int[subset];

        final long bubble = sort(itemsL, itemsR);
        final int hamming = slidingWindowHamming(itemsR, itemsL);
        if (hamming > samples) {
            throw new IllegalStateException();
        }
        getVectorZ(itemsR, itemsL, vectorZ, vectorZabs);
        final double normalW = normalWilcoxon(vectorZ, vectorZabs);
        final double meanRank = getMeanRank(vectorZabs);
        // case statement for requested value
        final double variance = Math.sqrt(meanRank);
        log.info("{},{},{},{},{},{},{},{},{}", tag, userID, samples, subset,
                hamming, bubble, meanRank, normalW, variance);
        return variance;
    }

    // simple sliding-window hamming distance: a[i or plus/minus 1] == b[i]
    private static int slidingWindowHamming(final Long[] itemsR,
            final Long[] itemsL) {
        int count = 0;
        final int samples = itemsR.length;

        if (itemsR[0].equals(itemsL[0]) || itemsR[0].equals(itemsL[1])) {
            count++;
        }
        for (int i = 1; i < samples - 1; i++) {
            final long itemID = itemsL[i];
            if (itemsR[i] == itemID || itemsR[i - 1] == itemID
                    || itemsR[i + 1] == itemID) {
                count++;
            }
        }
        if (itemsR[samples - 1].equals(itemsL[samples - 1])
                || itemsR[samples - 1].equals(itemsL[samples - 2])) {
            count++;
        }
        return count;
    }

    /**
     * Normal-distribution probability value for matched sets of values.
     * Based upon:
     * http://comp9.psych.cornell.edu/Darlington/normscor.htm
     *
     * The Standard Wilcoxon is not used because it requires a lookup table.
     */
    static double normalWilcoxon(final int[] vectorZ, final int[] vectorZabs) {
        final int nitems = vectorZ.length;

        final double[] ranks = new double[nitems];
        final double[] ranksAbs = new double[nitems];
        wilcoxonRanks(vectorZ, vectorZabs, ranks, ranksAbs);
        return Math.min(getMeanWplus(ranks), getMeanWminus(ranks));
    }

    /**
     * vector Z is a list of distances between the correct value and the recommended value
     * Z[i] = position i of correct itemID - position of correct itemID in recommendation list
     * can be positive or negative
     * the smaller the better - means recommendations are closer
     * both are the same length, and both sample from the same set
     *
     * destructive to items arrays - allows N log N instead of N^2 order
     */
    private static void getVectorZ(final Long[] itemsR, final Long[] itemsL,
            final int[] vectorZ, final int[] vectorZabs) {
        final int nitems = itemsR.length;
        int bottom = 0;
        int top = nitems - 1;
        for (int i = 0; i < nitems; i++) {
            final long itemID = itemsR[i];
            for (int j = bottom; j <= top; j++) {
                if (itemsL[j] == null) {
                    continue;
                }
                final long test = itemsL[j];
                if (itemID == test) {
                    vectorZ[i] = i - j;
                    vectorZabs[i] = Math.abs(i - j);
                    if (j == bottom) {
                        bottom++;
                    } else if (j == top) {
                        top--;
                    } else {
                        itemsL[j] = null;
                    }
                    break;
                }
            }
        }
    }

    /**
     * Ranks are the position of the value from low to high, divided by the # of values.
     * I had to walk through it a few times.
     */
    private static void wilcoxonRanks(final int[] vectorZ,
            final int[] vectorZabs, final double[] ranks,
            final double[] ranksAbs) {
        final int nitems = vectorZ.length;
        final int[] sorted = vectorZabs.clone();
        Arrays.sort(sorted);
        int zeros = 0;
        for (; zeros < nitems; zeros++) {
            if (sorted[zeros] > 0) {
                break;
            }
        }
        for (int i = 0; i < nitems; i++) {
            double rank = 0.0;
            int count = 0;
            final int score = vectorZabs[i];
            for (int j = 0; j < nitems; j++) {
                if (score == sorted[j]) {
                    rank += j + 1 - zeros;
                    count++;
                } else if (score < sorted[j]) {
                    break;
                }
            }
            if (vectorZ[i] != 0) {
                ranks[i] = rank / count * (vectorZ[i] < 0 ? -1 : 1); // better be at least 1
                ranksAbs[i] = Math.abs(ranks[i]);
            }
        }
    }

    private static double getMeanRank(final int[] ranks) {
        final int nitems = ranks.length;
        double sum = 0.0;
        for (final int rank : ranks) {
            sum += rank;
        }
        return sum / nitems;
    }

    private static double getMeanWplus(final double[] ranks) {
        final int nitems = ranks.length;
        double sum = 0.0;
        for (final double rank : ranks) {
            if (rank > 0) {
                sum += rank;
            }
        }
        return sum / nitems;
    }

    private static double getMeanWminus(final double[] ranks) {
        final int nitems = ranks.length;
        double sum = 0.0;
        for (final double rank : ranks) {
            if (rank < 0) {
                sum -= rank;
            }
        }
        return sum / nitems;
    }

    /**
     * Do bubble sort and return number of swaps needed to match preference lists.
     * Sort itemsR using itemsL as the reference order.
     */
    static long sort(final Long[] itemsL, final Long[] itemsR) {
        int length = itemsL.length;
        if (length < 2) {
            return 0;
        }
        if (length == 2) {
            return itemsL[0].longValue() == itemsR[0].longValue() ? 0 : 1;
        }
        // 1) avoid changing originals; 2) primitive type is more efficient
        final long[] reference = new long[length];
        final long[] sortable = new long[length];
        for (int i = 0; i < length; i++) {
            reference[i] = itemsL[i];
            sortable[i] = itemsR[i];
        }
        int sorted = 0;
        long swaps = 0;
        while (sorted < length - 1) {
            // opportunistically trim back the top
            while (length > 0 && reference[length - 1] == sortable[length - 1]) {
                length--;
            }
            if (length == 0) {
                break;
            }
            if (reference[sorted] == sortable[sorted]) {
                sorted++;
            } else {
                for (int j = sorted; j < length - 1; j++) {
                    // do not swap anything already in place
                    int jump = 1;
                    if (reference[j] == sortable[j]) {
                        while (j + jump < length
                                && reference[j + jump] == sortable[j + jump]) {
                            jump++;
                        }
                    }
                    if (j + jump < length
                            && !(reference[j] == sortable[j] && reference[j
                                    + jump] == sortable[j + jump])) {
                        final long tmp = sortable[j];
                        sortable[j] = sortable[j + 1];
                        sortable[j + 1] = tmp;
                        swaps++;
                    }
                }
            }
        }
        return swaps;
    }

}
