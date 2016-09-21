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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.mahout.common.RandomUtils;
import org.codelibs.elasticsearch.taste.common.FastByIDMap;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.exception.NoSuchItemException;
import org.codelibs.elasticsearch.taste.exception.NoSuchUserException;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.GenericDataModel;
import org.codelibs.elasticsearch.taste.model.GenericPreference;
import org.codelibs.elasticsearch.taste.model.GenericUserPreferenceArray;
import org.codelibs.elasticsearch.taste.model.Preference;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;
import org.codelibs.elasticsearch.taste.recommender.Recommender;
import org.codelibs.elasticsearch.taste.writer.ResultWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Abstract superclass of a couple implementations, providing shared functionality.
 */
public abstract class AbstractDifferenceEvaluator implements Evaluator {

    private static final Logger log = LoggerFactory
            .getLogger(AbstractDifferenceEvaluator.class);

    protected final Random random;

    protected float maxPreference;

    protected float minPreference;

    protected ResultWriter resultWriter;

    protected String id;

    private boolean interrupted = false;

    protected AbstractDifferenceEvaluator() {
        random = RandomUtils.getRandom();
        maxPreference = Float.NaN;
        minPreference = Float.NaN;
    }

    @Override
    public void setId(final String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setResultWriter(final ResultWriter resultWriter) {
        this.resultWriter = resultWriter;
    }

    public final float getMaxPreference() {
        return maxPreference;
    }

    public final void setMaxPreference(final float maxPreference) {
        this.maxPreference = maxPreference;
    }

    public final float getMinPreference() {
        return minPreference;
    }

    public final void setMinPreference(final float minPreference) {
        this.minPreference = minPreference;
    }

    @Override
    public Evaluation evaluate(final RecommenderBuilder recommenderBuilder,
            final DataModel dataModel, final EvaluationConfig config) {
        Preconditions.checkNotNull(recommenderBuilder);
        Preconditions.checkNotNull(dataModel);
        final double trainingPercentage = config.getTrainingPercentage();
        final double evaluationPercentage = config.getEvaluationPercentage();
        Preconditions.checkArgument(trainingPercentage >= 0.0
                && trainingPercentage <= 1.0, "Invalid trainingPercentage: "
                + trainingPercentage
                + ". Must be: 0.0 <= trainingPercentage <= 1.0");
        Preconditions.checkArgument(evaluationPercentage >= 0.0
                && evaluationPercentage <= 1.0,
                "Invalid evaluationPercentage: " + evaluationPercentage
                        + ". Must be: 0.0 <= evaluationPercentage <= 1.0");

        log.info("Beginning evaluation using {} of {}", trainingPercentage,
                dataModel);

        final int numUsers = dataModel.getNumUsers();
        final FastByIDMap<PreferenceArray> trainingPrefs = new FastByIDMap<PreferenceArray>(
                1 + (int) (evaluationPercentage * numUsers));
        final FastByIDMap<PreferenceArray> testPrefs = new FastByIDMap<PreferenceArray>(
                1 + (int) (evaluationPercentage * numUsers));

        final LongPrimitiveIterator it = dataModel.getUserIDs();
        while (it.hasNext()) {
            final long userID = it.nextLong();
            if (random.nextDouble() < evaluationPercentage) {
                splitOneUsersPrefs(trainingPercentage, trainingPrefs,
                        testPrefs, userID, dataModel);
            }
        }

        final DataModel trainingModel = new GenericDataModel(trainingPrefs);

        final Recommender recommender = recommenderBuilder
                .buildRecommender(trainingModel);

        final Evaluation result = getEvaluation(testPrefs, recommender,
                config.getMarginForError());
        result.setTraining(trainingPrefs.size());
        result.setTest(testPrefs.size());
        log.info("Evaluation result: {}", result);

        if (resultWriter != null) {
            try {
                resultWriter.close();
            } catch (final IOException e) {
                // ignore
            }
        }

        return result;
    }

    private void splitOneUsersPrefs(final double trainingPercentage,
            final FastByIDMap<PreferenceArray> trainingPrefs,
            final FastByIDMap<PreferenceArray> testPrefs, final long userID,
            final DataModel dataModel) {
        List<Preference> oneUserTrainingPrefs = null;
        List<Preference> oneUserTestPrefs = null;
        final PreferenceArray prefs = dataModel.getPreferencesFromUser(userID);
        final int size = prefs.length();
        for (int i = 0; i < size; i++) {
            final Preference newPref = new GenericPreference(userID,
                    prefs.getItemID(i), prefs.getValue(i));
            if (random.nextDouble() < trainingPercentage) {
                if (oneUserTrainingPrefs == null) {
                    oneUserTrainingPrefs = Lists.newArrayListWithCapacity(3);
                }
                oneUserTrainingPrefs.add(newPref);
            } else {
                if (oneUserTestPrefs == null) {
                    oneUserTestPrefs = Lists.newArrayListWithCapacity(3);
                }
                oneUserTestPrefs.add(newPref);
            }
        }
        if (oneUserTrainingPrefs != null) {
            trainingPrefs.put(userID, new GenericUserPreferenceArray(
                    oneUserTrainingPrefs));
            if (oneUserTestPrefs != null) {
                testPrefs.put(userID, new GenericUserPreferenceArray(
                        oneUserTestPrefs));
            }
        }
    }

    protected Evaluation getEvaluation(
            final FastByIDMap<PreferenceArray> testPrefs,
            final Recommender recommender, final float marginForError) {
        reset();
        final Collection<Callable<EstimateStatsResult>> estimateCallables = Lists
                .newArrayList();
        for (final Map.Entry<Long, PreferenceArray> entry : testPrefs
                .entrySet()) {
            estimateCallables.add(new PreferenceEstimateCallable(recommender,
                    entry.getKey(), entry.getValue(), marginForError));
        }
        log.info("Beginning evaluation of {} users", estimateCallables.size());

        final int numProcessors = Runtime.getRuntime().availableProcessors();
        final ExecutorService executor = Executors
                .newFixedThreadPool(numProcessors);
        log.info("Starting timing of {} tasks in {} threads",
                estimateCallables.size(), numProcessors);
        EstimateStatsResult finalResult = null;
        try {
            final List<Future<EstimateStatsResult>> futures = executor
                    .invokeAll(estimateCallables);
            int count = 0;
            // Go look for exceptions here, really
            for (final Future<EstimateStatsResult> future : futures) {
                final EstimateStatsResult result = future.get();
                if (Thread.currentThread().isInterrupted()) {
                    throw new TasteException("Interrupted evaluator.");
                }
                if (finalResult == null) {
                    finalResult = result;
                } else {
                    finalResult.merge(result);
                }
                if (count % 1000 == 0) {
                    final Runtime runtime = Runtime.getRuntime();
                    final long totalMemory = runtime.totalMemory();
                    final long memory = totalMemory - runtime.freeMemory();
                    log.info("Approximate memory used: {}MB / {}MB",
                            memory / 1000000L, totalMemory / 1000000L);
                }
                count++;
            }

        } catch (final InterruptedException ie) {
            throw new TasteException(ie);
        } catch (final ExecutionException ee) {
            throw new TasteException(ee.getCause());
        } finally {
            executor.shutdown();
        }

        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new TasteException(e.getCause());
        }

        final Evaluation evaluation = new Evaluation();
        evaluation.setScore(computeFinalEvaluation());
        if (finalResult != null) {
            evaluation.setTotalProcessingTime(finalResult
                    .getTotalProcessingTime());
            evaluation.setAverageProcessingTime(finalResult
                    .getAverageProcessingTime());
            evaluation.setMaxProcessingTime(finalResult.getMaxProcessingTime());
            evaluation.setSuccessful(finalResult.getSuccessful());
            evaluation.setFailure(finalResult.getFailure());
            evaluation.setNoEstimate(finalResult.getNoEstimate());
            evaluation.setTotalPreference(finalResult.getTotalPreference());
        }
        return evaluation;
    }

    protected float capEstimatedPreference(final float estimate) {
        if (estimate > maxPreference) {
            return maxPreference;
        }
        if (estimate < minPreference) {
            return minPreference;
        }
        return estimate;
    }

    protected abstract void reset();

    protected abstract void processOneEstimate(float estimatedPreference,
            Preference realPref);

    protected abstract double computeFinalEvaluation();

    protected static class EstimateStatsResult {

        private int noEstimate = 0;

        private int successful = 0;

        private int failure = 0;

        private int numOfTime = 0;

        private long totalTime = 0;

        private long maxTime = 0;

        public void incrementNoEstimate() {
            noEstimate++;
        }

        public void incrementSuccess() {
            successful++;
        }

        public void incrementFailure() {
            failure++;
        }

        public int getNoEstimate() {
            return noEstimate;
        }

        public int getSuccessful() {
            return successful;
        }

        public int getFailure() {
            return failure;
        }

        public int getTotalPreference() {
            return noEstimate + successful + failure;
        }

        public long getTotalProcessingTime() {
            return totalTime;
        }

        public long getAverageProcessingTime() {
            return totalTime / numOfTime;
        }

        public long getMaxProcessingTime() {
            return maxTime;
        }

        public void addDuration(final long time) {
            numOfTime++;
            totalTime += time;
            if (maxTime < time) {
                maxTime = time;
            }
        }

        public void merge(final EstimateStatsResult result) {
            noEstimate += result.noEstimate;
            successful += result.successful;
            failure += result.failure;
            numOfTime += result.numOfTime;
            totalTime += result.totalTime;
            maxTime += result.maxTime;
        }
    }

    protected class PreferenceEstimateCallable implements
            Callable<EstimateStatsResult> {

        private final Recommender recommender;

        private final long testUserID;

        private final PreferenceArray prefs;

        private final float marginForError;

        public PreferenceEstimateCallable(final Recommender recommender,
                final long testUserID, final PreferenceArray prefs,
                final float marginForError) {
            this.recommender = recommender;
            this.testUserID = testUserID;
            this.prefs = prefs;
            this.marginForError = marginForError;
        }

        @Override
        public EstimateStatsResult call() {
            final EstimateStatsResult stats = new EstimateStatsResult();
            for (final Preference realPref : prefs) {
                if (interrupted) {
                    break;
                }
                float estimatedPreference = Float.NaN;
                final float actualPreference = realPref.getValue();
                final long start = System.currentTimeMillis();
                final long time;
                try {
                    estimatedPreference = recommender.estimatePreference(
                            testUserID, realPref.getItemID());
                } catch (final NoSuchUserException nsue) {
                    // It's possible that an item exists in the test data but not training data in which case
                    // NSEE will be thrown. Just ignore it and move on.
                    log.info(
                            "User exists in test data but not training data: {}",
                            testUserID);
                } catch (final NoSuchItemException nsie) {
                    log.info(
                            "Item exists in test data but not training data: {}",
                            realPref.getItemID());
                } finally {
                    time = System.currentTimeMillis() - start;
                    stats.addDuration(time);
                }

                String estimateResultType;
                if (Float.isNaN(estimatedPreference)) {
                    estimateResultType = "no_estimate";
                    stats.incrementNoEstimate();
                } else {
                    estimatedPreference = capEstimatedPreference(estimatedPreference);
                    processOneEstimate(estimatedPreference, realPref);
                    if (Math.abs(estimatedPreference - realPref.getValue()) < marginForError) {
                        estimateResultType = "success";
                        stats.incrementSuccess();
                    } else {
                        estimateResultType = "failure";
                        stats.incrementFailure();
                    }
                }
                if (resultWriter != null) {
                    resultWriter.write(id, testUserID, realPref.getItemID(),
                            estimateResultType, actualPreference,
                            estimatedPreference, time);
                }
            }

            return stats;
        }

    }

    @Override
    public void interrupt() {
        interrupted = true;
    }
}
