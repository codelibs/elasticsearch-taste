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

package org.codelibs.elasticsearch.taste.recommender.svd;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.RandomWrapper;
import org.codelibs.elasticsearch.taste.common.FullRunningAverage;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.RunningAverage;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.Preference;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;

/** Matrix factorization with user and item biases for rating prediction, trained with plain vanilla SGD  */
public class RatingSGDFactorizer extends AbstractFactorizer {

    protected static final int FEATURE_OFFSET = 3;

    /** Multiplicative decay factor for learning_rate */
    protected final double learningRateDecay;

    /** Learning rate (step size) */
    protected final double learningRate;

    /** Parameter used to prevent overfitting. */
    protected final double preventOverfitting;

    /** Number of features used to compute this factorization */
    protected final int numFeatures;

    /** Number of iterations */
    private final int numIterations;

    /** Standard deviation for random initialization of features */
    protected final double randomNoise;

    /** User features */
    protected double[][] userVectors;

    /** Item features */
    protected double[][] itemVectors;

    protected final DataModel dataModel;

    private long[] cachedUserIDs;

    private long[] cachedItemIDs;

    protected double biasLearningRate = 0.5;

    protected double biasReg = 0.1;

    /** place in user vector where the bias is stored */
    protected static final int USER_BIAS_INDEX = 1;

    /** place in item vector where the bias is stored */
    protected static final int ITEM_BIAS_INDEX = 2;

    public RatingSGDFactorizer(final DataModel dataModel,
            final int numFeatures, final int numIterations) {
        this(dataModel, numFeatures, 0.01, 0.1, 0.01, numIterations, 1.0);
    }

    public RatingSGDFactorizer(final DataModel dataModel,
            final int numFeatures, final double learningRate,
            final double preventOverfitting, final double randomNoise,
            final int numIterations, final double learningRateDecay) {
        super(dataModel);
        this.dataModel = dataModel;
        this.numFeatures = numFeatures + FEATURE_OFFSET;
        this.numIterations = numIterations;

        this.learningRate = learningRate;
        this.learningRateDecay = learningRateDecay;
        this.preventOverfitting = preventOverfitting;
        this.randomNoise = randomNoise;
    }

    protected void prepareTraining() {
        final RandomWrapper random = RandomUtils.getRandom();
        userVectors = new double[dataModel.getNumUsers()][numFeatures];
        itemVectors = new double[dataModel.getNumItems()][numFeatures];

        final double globalAverage = getAveragePreference();
        for (int userIndex = 0; userIndex < userVectors.length; userIndex++) {
            userVectors[userIndex][0] = globalAverage;
            userVectors[userIndex][USER_BIAS_INDEX] = 0; // will store user bias
            userVectors[userIndex][ITEM_BIAS_INDEX] = 1; // corresponding item feature contains item bias
            for (int feature = FEATURE_OFFSET; feature < numFeatures; feature++) {
                userVectors[userIndex][feature] = random.nextGaussian()
                        * randomNoise;
            }
        }
        for (int itemIndex = 0; itemIndex < itemVectors.length; itemIndex++) {
            itemVectors[itemIndex][0] = 1; // corresponding user feature contains global average
            itemVectors[itemIndex][USER_BIAS_INDEX] = 1; // corresponding user feature contains user bias
            itemVectors[itemIndex][ITEM_BIAS_INDEX] = 0; // will store item bias
            for (int feature = FEATURE_OFFSET; feature < numFeatures; feature++) {
                itemVectors[itemIndex][feature] = random.nextGaussian()
                        * randomNoise;
            }
        }

        cachePreferences();
        shufflePreferences();
    }

    private int countPreferences() {
        int numPreferences = 0;
        final LongPrimitiveIterator userIDs = dataModel.getUserIDs();
        while (userIDs.hasNext()) {
            final PreferenceArray preferencesFromUser = dataModel
                    .getPreferencesFromUser(userIDs.nextLong());
            numPreferences += preferencesFromUser.length();
        }
        return numPreferences;
    }

    private void cachePreferences() {
        final int numPreferences = countPreferences();
        cachedUserIDs = new long[numPreferences];
        cachedItemIDs = new long[numPreferences];

        final LongPrimitiveIterator userIDs = dataModel.getUserIDs();
        int index = 0;
        while (userIDs.hasNext()) {
            final long userID = userIDs.nextLong();
            final PreferenceArray preferencesFromUser = dataModel
                    .getPreferencesFromUser(userID);
            for (final Preference preference : preferencesFromUser) {
                cachedUserIDs[index] = userID;
                cachedItemIDs[index] = preference.getItemID();
                index++;
            }
        }
    }

    protected void shufflePreferences() {
        final RandomWrapper random = RandomUtils.getRandom();
        /* Durstenfeld shuffle */
        for (int currentPos = cachedUserIDs.length - 1; currentPos > 0; currentPos--) {
            final int swapPos = random.nextInt(currentPos + 1);
            swapCachedPreferences(currentPos, swapPos);
        }
    }

    private void swapCachedPreferences(final int posA, final int posB) {
        final long tmpUserIndex = cachedUserIDs[posA];
        final long tmpItemIndex = cachedItemIDs[posA];

        cachedUserIDs[posA] = cachedUserIDs[posB];
        cachedItemIDs[posA] = cachedItemIDs[posB];

        cachedUserIDs[posB] = tmpUserIndex;
        cachedItemIDs[posB] = tmpItemIndex;
    }

    @Override
    public Factorization factorize() {
        prepareTraining();
        double currentLearningRate = learningRate;

        for (int it = 0; it < numIterations; it++) {
            for (int index = 0; index < cachedUserIDs.length; index++) {
                final long userId = cachedUserIDs[index];
                final long itemId = cachedItemIDs[index];
                final float rating = dataModel.getPreferenceValue(userId,
                        itemId);
                updateParameters(userId, itemId, rating, currentLearningRate);
            }
            currentLearningRate *= learningRateDecay;
        }
        return createFactorization(userVectors, itemVectors);
    }

    double getAveragePreference() {
        final RunningAverage average = new FullRunningAverage();
        final LongPrimitiveIterator it = dataModel.getUserIDs();
        while (it.hasNext()) {
            for (final Preference pref : dataModel.getPreferencesFromUser(it
                    .nextLong())) {
                average.addDatum(pref.getValue());
            }
        }
        return average.getAverage();
    }

    protected void updateParameters(final long userID, final long itemID,
            final float rating, final double currentLearningRate) {
        final int userIndex = userIndex(userID);
        final int itemIndex = itemIndex(itemID);

        final double[] userVector = userVectors[userIndex];
        final double[] itemVector = itemVectors[itemIndex];
        final double prediction = predictRating(userIndex, itemIndex);
        final double err = rating - prediction;

        // adjust user bias
        userVector[USER_BIAS_INDEX] += biasLearningRate
                * currentLearningRate
                * (err - biasReg * preventOverfitting
                        * userVector[USER_BIAS_INDEX]);

        // adjust item bias
        itemVector[ITEM_BIAS_INDEX] += biasLearningRate
                * currentLearningRate
                * (err - biasReg * preventOverfitting
                        * itemVector[ITEM_BIAS_INDEX]);

        // adjust features
        for (int feature = FEATURE_OFFSET; feature < numFeatures; feature++) {
            final double userFeature = userVector[feature];
            final double itemFeature = itemVector[feature];

            final double deltaUserFeature = err * itemFeature
                    - preventOverfitting * userFeature;
            userVector[feature] += currentLearningRate * deltaUserFeature;

            final double deltaItemFeature = err * userFeature
                    - preventOverfitting * itemFeature;
            itemVector[feature] += currentLearningRate * deltaItemFeature;
        }
    }

    private double predictRating(final int userID, final int itemID) {
        double sum = 0;
        for (int feature = 0; feature < numFeatures; feature++) {
            sum += userVectors[userID][feature] * itemVectors[itemID][feature];
        }
        return sum;
    }
}
