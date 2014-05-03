package org.codelibs.elasticsearch.taste.eval;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.writer.ResultWriter;

public interface Evaluator {
    /**
     * <p>
     * Evaluates the quality of a {@link org.apache.mahout.cf.taste.recommender.Recommender}'s recommendations.
     * The range of values that may be returned depends on the implementation, but <em>lower</em> values must
     * mean better recommendations, with 0 being the lowest / best possible evaluation, meaning a perfect match.
     * This method does not accept a {@link org.apache.mahout.cf.taste.recommender.Recommender} directly, but
     * rather a {@link RecommenderBuilder} which can build the
     * {@link org.apache.mahout.cf.taste.recommender.Recommender} to test on top of a given {@link DataModel}.
     * </p>
     *
     * <p>
     * Implementations will take a certain percentage of the preferences supplied by the given {@link DataModel}
     * as "training data". This is typically most of the data, like 90%. This data is used to produce
     * recommendations, and the rest of the data is compared against estimated preference values to see how much
     * the {@link org.apache.mahout.cf.taste.recommender.Recommender}'s predicted preferences match the user's
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
     *          object that can build a {@link org.apache.mahout.cf.taste.recommender.Recommender} to test
     * @param dataModel
     *          dataset to test on
     * @param trainingPercentage
     *          percentage of each user's preferences to use to produce recommendations; the rest are compared
     *          to estimated preference values to evaluate
     *          {@link org.apache.mahout.cf.taste.recommender.Recommender} performance
     * @param evaluationPercentage
     *          percentage of users to use in evaluation
     * @return a "score" representing how well the {@link org.apache.mahout.cf.taste.recommender.Recommender}'s
     *         estimated preferences match real values; <em>lower</em> scores mean a better match and 0 is a
     *         perfect match
     * @throws TasteException
     *           if an error occurs while accessing the {@link DataModel}
     */
    Evaluation evaluate(RecommenderBuilder recommenderBuilder,
            DataModel dataModel, EvaluationConfig config) throws TasteException;

    void setResultWriter(ResultWriter resultWriter);

    void setId(String evaluatorId);
}
