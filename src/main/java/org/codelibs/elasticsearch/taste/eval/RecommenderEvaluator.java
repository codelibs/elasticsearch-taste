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

import org.codelibs.elasticsearch.taste.model.DataModel;

/**
 * <p>
 * Implementations of this interface evaluate the quality of a
 * {@link org.codelibs.elasticsearch.taste.recommender.Recommender}'s recommendations.
 * </p>
 */
public interface RecommenderEvaluator {

    /**
     * <p>
     * Evaluates the quality of a {@link org.codelibs.elasticsearch.taste.recommender.Recommender}'s recommendations.
     * The range of values that may be returned depends on the implementation, but <em>lower</em> values must
     * mean better recommendations, with 0 being the lowest / best possible evaluation, meaning a perfect match.
     * This method does not accept a {@link org.codelibs.elasticsearch.taste.recommender.Recommender} directly, but
     * rather a {@link RecommenderBuilder} which can build the
     * {@link org.codelibs.elasticsearch.taste.recommender.Recommender} to test on top of a given {@link DataModel}.
     * </p>
     *
     * <p>
     * Implementations will take a certain percentage of the preferences supplied by the given {@link DataModel}
     * as "training data". This is typically most of the data, like 90%. This data is used to produce
     * recommendations, and the rest of the data is compared against estimated preference values to see how much
     * the {@link org.codelibs.elasticsearch.taste.recommender.Recommender}'s predicted preferences match the user's
     * real preferences. Specifically, for each user, this percentage of the user's ratings are used to produce
     * recommendations, and for each user, the remaining preferences are compared against the user's real
     * preferences.
     * </p>
     *
     * <p>
     * For large datasets, it may be desirable to only evaluate based on a small percentage of the data.
     * {@code evaluationPercentage} controls how many of the {@link DataModel}'s users are used in
     * evaluation.
     * </p>
     *
     * <p>
     * To be clear, {@code trainingPercentage} and {@code evaluationPercentage} are not related. They
     * do not need to add up to 1.0, for example.
     * </p>
     *
     * @param recommenderBuilder
     *          object that can build a {@link org.codelibs.elasticsearch.taste.recommender.Recommender} to test
     * @param dataModelBuilder
     *          {@link DataModelBuilder} to use, or if null, a default {@link DataModel}
     *          implementation will be used
     * @param dataModel
     *          dataset to test on
     * @param trainingPercentage
     *          percentage of each user's preferences to use to produce recommendations; the rest are compared
     *          to estimated preference values to evaluate
     *          {@link org.codelibs.elasticsearch.taste.recommender.Recommender} performance
     * @param evaluationPercentage
     *          percentage of users to use in evaluation
     * @return a "score" representing how well the {@link org.codelibs.elasticsearch.taste.recommender.Recommender}'s
     *         estimated preferences match real values; <em>lower</em> scores mean a better match and 0 is a
     *         perfect match
     */
    double evaluate(RecommenderBuilder recommenderBuilder,
            DataModelBuilder dataModelBuilder, DataModel dataModel,
            double trainingPercentage, double evaluationPercentage);

}
