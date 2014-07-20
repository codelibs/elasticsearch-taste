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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.codelibs.elasticsearch.taste.common.FullRunningAverageAndStdDev;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.RunningAverageAndStdDev;
import org.codelibs.elasticsearch.taste.common.SamplingLongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.recommender.Recommender;

import com.google.common.collect.Lists;

/**
 * Simple helper class for running load on a Recommender.
 */
public final class LoadEvaluator {

    private LoadEvaluator() {
    }

    public static LoadStatistics runLoad(final Recommender recommender) {
        return runLoad(recommender, 10);
    }

    public static LoadStatistics runLoad(final Recommender recommender,
            final int howMany) {
        final DataModel dataModel = recommender.getDataModel();
        final int numUsers = dataModel.getNumUsers();
        final double sampleRate = 1000.0 / numUsers;
        final LongPrimitiveIterator userSampler = SamplingLongPrimitiveIterator
                .maybeWrapIterator(dataModel.getUserIDs(), sampleRate);
        recommender.recommend(userSampler.next(), howMany); // Warm up
        final Collection<Callable<Void>> callables = Lists.newArrayList();
        while (userSampler.hasNext()) {
            callables.add(new LoadCallable(recommender, userSampler.next()));
        }
        final AtomicInteger noEstimateCounter = new AtomicInteger();
        final RunningAverageAndStdDev timing = new FullRunningAverageAndStdDev();
        AbstractDifferenceRecommenderEvaluator.execute(callables,
                noEstimateCounter, timing);
        return new LoadStatistics(timing);
    }

}
