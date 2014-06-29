package org.codelibs.elasticsearch.taste.recommender;

import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.Rescorer;
import org.apache.mahout.common.LongPair;

/**
 * <p>
 * Interface implemented by "user-based" recommenders.
 * </p>
 */
public interface UserBasedRecommender extends Recommender {

    /**
     * @param userID
     *          ID of user for which to find most similar other users
     * @param howMany
     *          desired number of most similar users to find
     * @return users most similar to the given user
     * @throws TasteException
     *           if an error occurs while accessing the {@link org.apache.mahout.cf.taste.model.DataModel}
     */
    List<SimilarUser> mostSimilarUserIDs(long userID, int howMany)
            throws TasteException;

    /**
     * @param userID
     *          ID of user for which to find most similar other users
     * @param howMany
     *          desired number of most similar users to find
     * @param rescorer
     *          {@link Rescorer} which can adjust user-user similarity estimates used to determine most similar
     *          users
     * @return IDs of users most similar to the given user
     * @throws TasteException
     *           if an error occurs while accessing the {@link org.apache.mahout.cf.taste.model.DataModel}
     */
    List<SimilarUser> mostSimilarUserIDs(long userID, int howMany,
            Rescorer<LongPair> rescorer) throws TasteException;

}
