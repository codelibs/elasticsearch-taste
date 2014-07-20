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

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;
import org.apache.mahout.math.als.ImplicitFeedbackAlternatingLeastSquaresSolver;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.codelibs.elasticsearch.taste.common.FullRunningAverage;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.RunningAverage;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.Preference;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * factorizes the rating matrix using "Alternating-Least-Squares with Weighted-λ-Regularization" as described in
 * <a href="http://www.hpl.hp.com/personal/Robert_Schreiber/papers/2008%20AAIM%20Netflix/netflix_aaim08(submitted).pdf">
 * "Large-scale Collaborative Filtering for the Netflix Prize"</a>
 *
 *  also supports the implicit feedback variant of this approach as described in "Collaborative Filtering for Implicit
 *  Feedback Datasets" available at http://research.yahoo.com/pub/2433
 */
public class ALSWRFactorizer extends AbstractFactorizer {

    private final DataModel dataModel;

    /** number of features used to compute this factorization */
    private final int numFeatures;

    /** parameter to control the regularization */
    private final double lambda;

    /** number of iterations */
    private final int numIterations;

    private final boolean usesImplicitFeedback;

    /** confidence weighting parameter, only necessary when working with implicit feedback */
    private final double alpha;

    private final int numTrainingThreads;

    private static final double DEFAULT_ALPHA = 40;

    private static final Logger log = LoggerFactory
            .getLogger(ALSWRFactorizer.class);

    public ALSWRFactorizer(final DataModel dataModel, final int numFeatures,
            final double lambda, final int numIterations,
            final boolean usesImplicitFeedback, final double alpha,
            final int numTrainingThreads) {
        super(dataModel);
        this.dataModel = dataModel;
        this.numFeatures = numFeatures;
        this.lambda = lambda;
        this.numIterations = numIterations;
        this.usesImplicitFeedback = usesImplicitFeedback;
        this.alpha = alpha;
        this.numTrainingThreads = numTrainingThreads;
    }

    public ALSWRFactorizer(final DataModel dataModel, final int numFeatures,
            final double lambda, final int numIterations,
            final boolean usesImplicitFeedback, final double alpha) {
        this(dataModel, numFeatures, lambda, numIterations,
                usesImplicitFeedback, alpha, Runtime.getRuntime()
                        .availableProcessors());
    }

    public ALSWRFactorizer(final DataModel dataModel, final int numFeatures,
            final double lambda, final int numIterations) {
        this(dataModel, numFeatures, lambda, numIterations, false,
                DEFAULT_ALPHA);
    }

    static class Features {

        private final DataModel dataModel;

        private final int numFeatures;

        private final double[][] M;

        private final double[][] U;

        Features(final ALSWRFactorizer factorizer) {
            dataModel = factorizer.dataModel;
            numFeatures = factorizer.numFeatures;
            final Random random = RandomUtils.getRandom();
            M = new double[dataModel.getNumItems()][numFeatures];
            final LongPrimitiveIterator itemIDsIterator = dataModel
                    .getItemIDs();
            while (itemIDsIterator.hasNext()) {
                final long itemID = itemIDsIterator.nextLong();
                final int itemIDIndex = factorizer.itemIndex(itemID);
                M[itemIDIndex][0] = averateRating(itemID);
                for (int feature = 1; feature < numFeatures; feature++) {
                    M[itemIDIndex][feature] = random.nextDouble() * 0.1;
                }
            }
            U = new double[dataModel.getNumUsers()][numFeatures];
        }

        double[][] getM() {
            return M;
        }

        double[][] getU() {
            return U;
        }

        Vector getUserFeatureColumn(final int index) {
            return new DenseVector(U[index]);
        }

        Vector getItemFeatureColumn(final int index) {
            return new DenseVector(M[index]);
        }

        void setFeatureColumnInU(final int idIndex, final Vector vector) {
            setFeatureColumn(U, idIndex, vector);
        }

        void setFeatureColumnInM(final int idIndex, final Vector vector) {
            setFeatureColumn(M, idIndex, vector);
        }

        protected void setFeatureColumn(final double[][] matrix,
                final int idIndex, final Vector vector) {
            for (int feature = 0; feature < numFeatures; feature++) {
                matrix[idIndex][feature] = vector.get(feature);
            }
        }

        protected double averateRating(final long itemID) {
            final PreferenceArray prefs = dataModel
                    .getPreferencesForItem(itemID);
            final RunningAverage avg = new FullRunningAverage();
            for (final Preference pref : prefs) {
                avg.addDatum(pref.getValue());
            }
            return avg.getAverage();
        }
    }

    @Override
    public Factorization factorize() {
        log.info("starting to compute the factorization...");
        final Features features = new Features(this);

        /* feature maps necessary for solving for implicit feedback */
        OpenIntObjectHashMap<Vector> userY = null;
        OpenIntObjectHashMap<Vector> itemY = null;

        if (usesImplicitFeedback) {
            userY = userFeaturesMapping(dataModel.getUserIDs(),
                    dataModel.getNumUsers(), features.getU());
            itemY = itemFeaturesMapping(dataModel.getItemIDs(),
                    dataModel.getNumItems(), features.getM());
        }

        for (int iteration = 0; iteration < numIterations; iteration++) {
            log.info("iteration {}", iteration);

            /* fix M - compute U */
            ExecutorService queue = createQueue();
            final LongPrimitiveIterator userIDsIterator = dataModel
                    .getUserIDs();
            try {

                final ImplicitFeedbackAlternatingLeastSquaresSolver implicitFeedbackSolver = usesImplicitFeedback ? new ImplicitFeedbackAlternatingLeastSquaresSolver(
                        numFeatures, lambda, alpha, itemY) : null;

                while (userIDsIterator.hasNext()) {
                    final long userID = userIDsIterator.nextLong();
                    final LongPrimitiveIterator itemIDsFromUser = dataModel
                            .getItemIDsFromUser(userID).iterator();
                    final PreferenceArray userPrefs = dataModel
                            .getPreferencesFromUser(userID);
                    queue.execute(new Runnable() {
                        @Override
                        public void run() {
                            final List<Vector> featureVectors = Lists
                                    .newArrayList();
                            while (itemIDsFromUser.hasNext()) {
                                final long itemID = itemIDsFromUser.nextLong();
                                featureVectors.add(features
                                        .getItemFeatureColumn(itemIndex(itemID)));
                            }

                            final Vector userFeatures = usesImplicitFeedback ? implicitFeedbackSolver
                                    .solve(sparseUserRatingVector(userPrefs))
                                    : AlternatingLeastSquaresSolver.solve(
                                            featureVectors,
                                            ratingVector(userPrefs), lambda,
                                            numFeatures);

                            features.setFeatureColumnInU(userIndex(userID),
                                    userFeatures);
                        }
                    });
                }
            } finally {
                queue.shutdown();
                try {
                    queue.awaitTermination(dataModel.getNumUsers(),
                            TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    log.warn("Error when computing user features", e);
                }
            }

            /* fix U - compute M */
            queue = createQueue();
            final LongPrimitiveIterator itemIDsIterator = dataModel
                    .getItemIDs();
            try {

                final ImplicitFeedbackAlternatingLeastSquaresSolver implicitFeedbackSolver = usesImplicitFeedback ? new ImplicitFeedbackAlternatingLeastSquaresSolver(
                        numFeatures, lambda, alpha, userY) : null;

                while (itemIDsIterator.hasNext()) {
                    final long itemID = itemIDsIterator.nextLong();
                    final PreferenceArray itemPrefs = dataModel
                            .getPreferencesForItem(itemID);
                    queue.execute(new Runnable() {
                        @Override
                        public void run() {
                            final List<Vector> featureVectors = Lists
                                    .newArrayList();
                            for (final Preference pref : itemPrefs) {
                                final long userID = pref.getUserID();
                                featureVectors.add(features
                                        .getUserFeatureColumn(userIndex(userID)));
                            }

                            final Vector itemFeatures = usesImplicitFeedback ? implicitFeedbackSolver
                                    .solve(sparseItemRatingVector(itemPrefs))
                                    : AlternatingLeastSquaresSolver.solve(
                                            featureVectors,
                                            ratingVector(itemPrefs), lambda,
                                            numFeatures);

                            features.setFeatureColumnInM(itemIndex(itemID),
                                    itemFeatures);
                        }
                    });
                }
            } finally {
                queue.shutdown();
                try {
                    queue.awaitTermination(dataModel.getNumItems(),
                            TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    log.warn("Error when computing item features", e);
                }
            }
        }

        log.info("finished computation of the factorization...");
        return createFactorization(features.getU(), features.getM());
    }

    protected ExecutorService createQueue() {
        return Executors.newFixedThreadPool(numTrainingThreads);
    }

    protected static Vector ratingVector(final PreferenceArray prefs) {
        final double[] ratings = new double[prefs.length()];
        for (int n = 0; n < prefs.length(); n++) {
            ratings[n] = prefs.get(n).getValue();
        }
        return new DenseVector(ratings, true);
    }

    //TODO find a way to get rid of the object overhead here
    protected OpenIntObjectHashMap<Vector> itemFeaturesMapping(
            final LongPrimitiveIterator itemIDs, final int numItems,
            final double[][] featureMatrix) {
        final OpenIntObjectHashMap<Vector> mapping = new OpenIntObjectHashMap<Vector>(
                numItems);
        while (itemIDs.hasNext()) {
            final long itemID = itemIDs.next();
            mapping.put((int) itemID, new DenseVector(
                    featureMatrix[itemIndex(itemID)], true));
        }

        return mapping;
    }

    protected OpenIntObjectHashMap<Vector> userFeaturesMapping(
            final LongPrimitiveIterator userIDs, final int numUsers,
            final double[][] featureMatrix) {
        final OpenIntObjectHashMap<Vector> mapping = new OpenIntObjectHashMap<Vector>(
                numUsers);

        while (userIDs.hasNext()) {
            final long userID = userIDs.next();
            mapping.put((int) userID, new DenseVector(
                    featureMatrix[userIndex(userID)], true));
        }

        return mapping;
    }

    protected Vector sparseItemRatingVector(final PreferenceArray prefs) {
        final SequentialAccessSparseVector ratings = new SequentialAccessSparseVector(
                Integer.MAX_VALUE, prefs.length());
        for (final Preference preference : prefs) {
            ratings.set((int) preference.getUserID(), preference.getValue());
        }
        return ratings;
    }

    protected Vector sparseUserRatingVector(final PreferenceArray prefs) {
        final SequentialAccessSparseVector ratings = new SequentialAccessSparseVector(
                Integer.MAX_VALUE, prefs.length());
        for (final Preference preference : prefs) {
            ratings.set((int) preference.getItemID(), preference.getValue());
        }
        return ratings;
    }
}
