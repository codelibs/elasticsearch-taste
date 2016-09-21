package org.codelibs.elasticsearch.taste.recommender;

import org.apache.mahout.common.RandomUtils;

/** Simply encapsulates a user and a similarity value. */
public final class SimilarUser implements Comparable<SimilarUser> {

    private final long userID;

    private final double similarity;

    public SimilarUser(final long userID, final double similarity) {
        this.userID = userID;
        this.similarity = similarity;
    }

    public long getUserID() {
        return userID;
    }

    public double getSimilarity() {
        return similarity;
    }

    @Override
    public int hashCode() {
        return (int) userID ^ RandomUtils.hashDouble(similarity);
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof SimilarUser)) {
            return false;
        }
        final SimilarUser other = (SimilarUser) o;
        return userID == other.getUserID()
                && !(similarity != other.getSimilarity());
    }

    @Override
    public String toString() {
        return "SimilarUser[user:" + userID + ", similarity:" + similarity
                + ']';
    }

    /** Defines an ordering from most similar to least similar. */
    @Override
    public int compareTo(final SimilarUser other) {
        final double otherSimilarity = other.getSimilarity();
        if (similarity > otherSimilarity) {
            return -1;
        }
        if (similarity < otherSimilarity) {
            return 1;
        }
        final long otherUserID = other.getUserID();
        if (userID < otherUserID) {
            return -1;
        }
        if (userID > otherUserID) {
            return 1;
        }
        return 0;
    }

}
